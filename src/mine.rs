use crate::network::{ControlState, SolutionResult, NETWORK_WINDOW};
use crate::{
    args::MineArgs,
    send_and_confirm::ComputeBudget,
    utils::{
        amount_u64_to_string, get_clock, get_config, get_updated_proof_with_authority, proof_pubkey,
    },
    Miner,
};
use colored::*;
use drillx::{
    equix::{self},
    Hash, Solution,
};
use ore_api::{
    consts::{BUS_ADDRESSES, BUS_COUNT, EPOCH_DURATION},
    state::{Bus, Config, Proof},
};
use ore_utils::AccountDeserialize;
use rand::Rng;
use solana_program::pubkey::Pubkey;
use solana_rpc_client::spinner;
use solana_sdk::signer::Signer;
use std::sync::atomic::{AtomicU32, Ordering};
use std::{sync::Arc, time::{Duration, Instant}};
use tokio::select;
use tokio::sync::broadcast::error::RecvError;

impl Miner {
    pub async fn mine(&self, args: MineArgs) {
        // Open account, if needed.
        let signer = self.signer();
        self.open().await;

        // Check num threads
        self.check_num_cores(args.cores);

        let (mut control_receiver, solution_sender) = if let Some(address) = args.forward_address.clone() {
            let (cr, ss) = self.connect(address).await;
            (Some(cr), Some(ss))
        } else {
            (None, None)
        };

        let (control_sender, mut solution_receiver) = if args.forward_address.is_none() {
            let (cs, sr) = self.serve().await;
            (Some(cs), Some(sr))
        } else {
            (None, None)
        };

        // Start mining loop
        let mut last_hash_at = 0;
        let mut last_balance = 0;
        loop {
            if args.forward_address.is_some() {
                loop {
                    let mut receiver = control_receiver.take().unwrap();
                    let result = receiver.recv().await;
                    _ = control_receiver.insert(receiver);
                    match result {
                        Ok(s) => {
                            match s {
                                ControlState::Stop => {
                                    continue
                                }
                                ControlState::Go => {
                                    break;
                                }
                            }
                        }
                        Err(err) => {
                            match err {
                                RecvError::Lagged(lag) => {
                                    println!("warn: control lag: {}", lag);
                                    continue;
                                }
                                RecvError::Closed => {
                                    panic!("control receiver unexpectedly closed");
                                }
                            }
                        }
                    };
                }
            } else {
                _ = control_sender.clone().unwrap().send(ControlState::Go)
            };

            // Fetch proof
            let config = get_config(&self.rpc_client).await;
            let proof =
                get_updated_proof_with_authority(&self.rpc_client, signer.pubkey(), last_hash_at)
                    .await;
            println!(
                "\n\nStake: {} ORE\n{}  Multiplier: {:12}x",
                amount_u64_to_string(proof.balance),
                if last_hash_at.gt(&0) {
                    format!(
                        "  Change: {} ORE\n",
                        amount_u64_to_string(proof.balance.saturating_sub(last_balance))
                    )
                } else {
                    "".to_string()
                },
                calculate_multiplier(proof.balance, config.top_balance)
            );
            last_hash_at = proof.last_hash_at;
            last_balance = proof.balance;

            // Calculate cutoff time
            let cutoff_time = self.get_cutoff(proof, args.buffer_time).await;

            let min_difficulty = if args.minimum_difficulty > config.min_difficulty {
                args.minimum_difficulty
            } else {
                config.min_difficulty
            };

            // Run drillx
            let solution_result =
                Self::find_hash_par(proof, cutoff_time, args.cores, min_difficulty as u32)
                    .await;

            let mut solution = solution_result.solution;
            let mut difficulty = solution_result.difficulty;
            let mut hash_rate = solution_result.hash_rate.unwrap();

            if let Some(mut receiver) = solution_receiver.take() {
                let progress_bar = Arc::new(spinner::new_progress_bar());
                let sleep = tokio::time::sleep(Duration::from_secs(NETWORK_WINDOW));
                tokio::pin!(sleep);
                progress_bar.set_message("Waiting for solutions...");
                loop {
                    let result = select! {
                        msg = receiver.recv() => {
                            match msg {
                                Ok(sr) => {
                                    sr
                                }
                                Err(err) => {
                                    match err {
                                        RecvError::Closed => {
                                            break;
                                        }
                                        RecvError::Lagged(lag) => {
                                            println!("solution receiver lagged: {}", lag);
                                            continue;
                                        }
                                    }
                                }
                            }
                        }
                        _ = &mut sleep => {
                            break;
                        }
                    };

                    if let Some(rate) = result.hash_rate {
                        hash_rate += rate;
                    }
                    if result.difficulty.gt(&difficulty) {
                        solution = result.solution;
                        difficulty = result.difficulty;
                    };
                    progress_bar.set_message(format!(
                        "Receiving solutions... (best difficulty: {})",
                        difficulty
                    ));
                }
                progress_bar.finish_with_message(format!(
                    "Best difficulty: {}\n Hash Rate: {}",
                    difficulty,
                    hash_rate,
                ));
                _ = solution_receiver.insert(receiver);
                _ = control_sender.clone().unwrap().send(ControlState::Stop);
            } else {
                match solution_sender.clone().unwrap().send(solution_result) {
                    Ok(_) => {}
                    Err(err) => {
                        panic!("failed to send solution result: {:?}", err);
                    }
                };
                continue;
            }

            // Build instruction set
            let mut ixs = vec![ore_api::instruction::auth(proof_pubkey(signer.pubkey()))];
            let mut compute_budget = 500_000;
            if self.should_reset(config).await && rand::thread_rng().gen_range(0..100).eq(&0) {
                compute_budget += 100_000;
                ixs.push(ore_api::instruction::reset(signer.pubkey()));
            }

            // Build mine ix
            ixs.push(ore_api::instruction::mine(
                signer.pubkey(),
                signer.pubkey(),
                self.find_bus().await,
                solution,
            ));

            // Submit transaction
            self.send_and_confirm(&ixs, ComputeBudget::Fixed(compute_budget), false)
                .await
                .ok();
        }
    }

    async fn find_hash_par(
        proof: Proof,
        cutoff_time: u64,
        cores: u64,
        min_difficulty: u32,
    ) -> SolutionResult {
        // Dispatch job to each thread
        let progress_bar = Arc::new(spinner::new_progress_bar());
        let global_best_difficulty = Arc::new(AtomicU32::new(0));
        let hashes = Arc::new(AtomicU32::new(0));
        let start_time = tokio::time::Instant::now();
        progress_bar.set_message("Mining...");
        let core_ids = core_affinity::get_core_ids().unwrap();
        let handles: Vec<_> = core_ids
            .into_iter()
            .map(|i| {
                std::thread::spawn({
                    let proof = proof.clone();
                    let progress_bar = progress_bar.clone();
                    let mut memory = equix::SolverMemory::new();
                    let global_best_difficulty = Arc::clone(&global_best_difficulty);
                    let global_hashes = hashes.clone();
                    move || {
                        // Return if core should not be used
                        if (i.id as u64).ge(&cores) {
                            return (0, 0, Hash::default());
                        }

                        // Pin to core
                        let _ = core_affinity::set_for_current(i);

                        // Start hashing
                        let timer = Instant::now();
                        let mut nonce = u64::MAX.saturating_div(cores).saturating_mul(i.id as u64);
                        let mut best_nonce = nonce;
                        let mut best_difficulty = 0;
                        let mut best_hash = Hash::default();
                        loop {
                            // Get hashes
                            let hxs = drillx::hashes_with_memory(
                                &mut memory,
                                &proof.challenge,
                                &nonce.to_le_bytes(),
                            );

                            // Look for best difficulty score in all hashes
                            for hx in hxs {
                                let difficulty = hx.difficulty();
                                if difficulty.gt(&best_difficulty) {
                                    best_nonce = nonce;
                                    best_difficulty = difficulty;
                                    best_hash = hx;
                                    let current_best = global_best_difficulty.load(Ordering::Acquire);
                                    if best_difficulty.gt(&current_best) {
                                        global_best_difficulty.store(best_difficulty, Ordering::Release);
                                    }
                                }
                            }
                            global_hashes.fetch_add(1, Ordering::Relaxed);

                            // Exit if time has elapsed
                            if nonce % 100 == 0 {
                                let global_best_difficulty = global_best_difficulty.load(Ordering::Acquire);
                                if timer.elapsed().as_secs().ge(&cutoff_time) {
                                    if i.id == 0 {
                                        progress_bar.set_message(format!(
                                            "Mining... (difficulty {})",
                                            global_best_difficulty,
                                        ));
                                    }
                                    if global_best_difficulty.ge(&min_difficulty) {
                                        // Mine until min difficulty has been met
                                        break;
                                    }
                                } else if i.id == 0 {
                                    progress_bar.set_message(format!(
                                        "Mining... (difficulty {}, time {})",
                                        global_best_difficulty,
                                        format_duration(
                                            cutoff_time.saturating_sub(timer.elapsed().as_secs())
                                                as u32
                                        ),
                                    ));
                                }
                            }

                            // Increment nonce
                            nonce += 1;
                        }

                        // Return the best nonce
                        (best_nonce, best_difficulty, best_hash)
                    }
                })
            })
            .collect();

        // Join handles and return best nonce
        let mut best_nonce = 0;
        let mut best_difficulty = 0;
        let mut best_hash = Hash::default();
        for h in handles {
            if let Ok((nonce, difficulty, hash)) = h.join() {
                if difficulty > best_difficulty {
                    best_difficulty = difficulty;
                    best_nonce = nonce;
                    best_hash = hash;
                }
            }
        }
        let hash_time = start_time.elapsed();
        let hashes = hashes.load(Ordering::Relaxed);
        let hash_rate = (hashes as f64 / hash_time.as_secs_f64()).round() as u32;
        println!("hashes: {}, hash_rate: {}", hashes, hash_rate);

        // Update log
        progress_bar.finish_with_message(format!(
            "Best hash: {} (difficulty {})",
            bs58::encode(best_hash.h).into_string(),
            best_difficulty
        ));

        SolutionResult::new(
            Solution::new(best_hash.d, best_nonce.to_le_bytes()), best_difficulty, hash_rate,
        )
    }

    pub fn check_num_cores(&self, cores: u64) {
        let num_cores = num_cpus::get() as u64;
        if cores.gt(&num_cores) {
            println!(
                "{} Cannot exceeds available cores ({})",
                "WARNING".bold().yellow(),
                num_cores
            );
        }
    }

    async fn should_reset(&self, config: Config) -> bool {
        let clock = get_clock(&self.rpc_client).await;
        config
            .last_reset_at
            .saturating_add(EPOCH_DURATION)
            .saturating_sub(5) // Buffer
            .le(&clock.unix_timestamp)
    }

    async fn get_cutoff(&self, proof: Proof, buffer_time: u64) -> u64 {
        let clock = get_clock(&self.rpc_client).await;
        proof
            .last_hash_at
            .saturating_add(60)
            .saturating_sub(buffer_time as i64)
            .saturating_sub(clock.unix_timestamp)
            .max(0) as u64
    }

    async fn find_bus(&self) -> Pubkey {
        // Fetch the bus with the largest balance
        if let Ok(accounts) = self.rpc_client.get_multiple_accounts(&BUS_ADDRESSES).await {
            let mut top_bus_balance: u64 = 0;
            let mut top_bus = BUS_ADDRESSES[0];
            for account in accounts {
                if let Some(account) = account {
                    if let Ok(bus) = Bus::try_from_bytes(&account.data) {
                        if bus.rewards.gt(&top_bus_balance) {
                            top_bus_balance = bus.rewards;
                            top_bus = BUS_ADDRESSES[bus.id as usize];
                        }
                    }
                }
            }
            return top_bus;
        }

        // Otherwise return a random bus
        let i = rand::thread_rng().gen_range(0..BUS_COUNT);
        BUS_ADDRESSES[i]
    }
}

fn calculate_multiplier(balance: u64, top_balance: u64) -> f64 {
    1.0 + (balance as f64 / top_balance as f64).min(1.0f64)
}

fn format_duration(seconds: u32) -> String {
    let minutes = seconds / 60;
    let remaining_seconds = seconds % 60;
    format!("{:02}:{:02}", minutes, remaining_seconds)
}
