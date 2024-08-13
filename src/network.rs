use crate::Miner;
use drillx::Solution;
use futures::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tokio::time::{sleep, Duration};
use tokio::{net, select};
use tokio_serde::formats::*;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

pub const NETWORK_WINDOW: u64 = 10;

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub enum ControlState {
    Stop = 0,
    Go = 1,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct SolutionResult {
    pub solution: Solution,
    pub difficulty: u32,
}

impl SolutionResult {
    pub fn new(solution: Solution, difficulty: u32) -> Self {
        SolutionResult {
            solution,
            difficulty,
        }
    }
}

impl Miner {
    pub async fn serve(&self) -> (broadcast::Sender<ControlState>, broadcast::Receiver<SolutionResult>) {
        let (c_sender, _) = broadcast::channel(100);
        let (s_sender, sr) = broadcast::channel(1000);

        let cs = c_sender.clone();

        tokio::spawn(async move {
            let listener = match net::TcpListener::bind("0.0.0.0:8000").await {
                Ok(listener) => {
                    listener
                }
                Err(err) => {
                    panic!("failed to listen on port 8000: {:?}", err);
                }
            };
            loop {
                let result = listener.accept().await;
                let (mut stream, addr) = {
                    match result {
                        Ok((stream, addr)) => {
                            println!("  {} connected", addr);
                            (stream, addr)
                        }
                        Err(err) => {
                            println!("client connection failed: {:?}", err);
                            continue;
                        }
                    }
                };

                let mut cr = c_sender.subscribe();
                let ss = s_sender.clone();

                tokio::spawn(async move {
                    let mut state: ControlState = ControlState::Stop;

                    let (stream_reader, stream_writer) = stream.split();

                    let framed_reader = FramedRead::new(stream_reader, LengthDelimitedCodec::new());
                    let mut deserialized = tokio_serde::SymmetricallyFramed::new(
                        framed_reader,
                        SymmetricalJson::<SolutionResult>::default(),
                    );

                    let framed_writer = FramedWrite::new(stream_writer, LengthDelimitedCodec::new());
                    let mut serialized = tokio_serde::SymmetricallyFramed::new(framed_writer, SymmetricalJson::default());

                    loop {
                        select! {
                            msg = cr.recv() => {
                                match msg {
                                    Ok(r_state) => {
                                        state = r_state;
                                        if let Err(err) = serialized.send(state).await {
                                            println!("{} lost: {:?}", addr.ip(), err);
                                            return;
                                        };
                                    }
                                    Err(err) => {
                                        match err {
                                            RecvError::Lagged(lag) => {
                                                println!("WARN: control receiver lagging: {}", lag);
                                                continue;
                                            }
                                            RecvError::Closed => {
                                                panic!("control receiver unexpectedly closed");
                                            }
                                        }
                                    }
                                }
                            }
                            _ = sleep(Duration::from_millis(500)) => {}
                        }

                        let message = select! {
                            msg = deserialized.try_next() => {
                                match msg {
                                    Ok(message) => message,
                                    Err(err) => {
                                        println!("failed to receive solution: {:?}", err);
                                        continue;
                                    }
                                }
                            }
                            _ = sleep(Duration::from_secs(1)) => {
                                continue;
                            }
                        };

                        let Some(solution) = message else {
                            println!("  {} disconnected", addr.ip());
                            return;
                        };

                        match state {
                            ControlState::Stop => {
                                // drop old solution
                            }
                            ControlState::Go => {
                                println!("    {}: {}", addr.ip(), solution.difficulty);
                                if let Err(err) = ss.send(solution) {
                                    println!("Failed to forward solution: {:?}", err);
                                };
                            }
                        }
                    };
                });
            }
        });

        (cs, sr)
    }

    pub async fn connect(&self, forward_address: String) -> (broadcast::Receiver<ControlState>, broadcast::Sender<SolutionResult>) {
        let mut stream = TcpStream::connect(forward_address).await.unwrap();
        println!("connection with host established");

        let (solution_sender, mut solution_receiver) = broadcast::channel::<SolutionResult>(1000);
        let (control_sender, _) = broadcast::channel::<ControlState>(100);

        let cs = control_sender.clone();
        let ss = solution_sender.clone();

        tokio::spawn(async move {
            let (stream_reader, stream_writer) = stream.split();

            let framed_reader = FramedRead::new(stream_reader, LengthDelimitedCodec::new());
            let deserialized = tokio_serde::SymmetricallyFramed::new(framed_reader, SymmetricalJson::<ControlState>::default());
            tokio::pin!(deserialized);

            let framed_writer = FramedWrite::new(stream_writer, LengthDelimitedCodec::new());
            let mut serialized = tokio_serde::SymmetricallyFramed::new(framed_writer, SymmetricalJson::default());

            let mut state = ControlState::Stop;

            loop {
                select! {
                    msg = deserialized.try_next() => {
                        match msg {
                            Ok(so) => {
                                match so {
                                    Some(s) => {
                                        state = s;
                                        _ = control_sender.send(s);
                                    }
                                    None => {}
                                }
                            }
                            Err(err) => {
                                panic!("problem reading state message: {:?}", err);
                            }
                        }
                    }
                    _ = sleep(Duration::from_millis(500)) => {}
                }

                match state {
                    ControlState::Stop => {
                        sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                    ControlState::Go => {}
                }

                let solution = match solution_receiver.recv().await {
                    Ok(solution) => {
                        solution
                    }
                    Err(err) => {
                        match err {
                            RecvError::Lagged(lag) => {
                                println!("WARN: solution receiver lagging: {}", lag);
                                continue;
                            }
                            RecvError::Closed => {
                                return;
                            }
                        }
                    }
                };

                if let Err(err) = serialized.send(solution).await {
                    println!("problem sending solution: {:?}", err)
                };

                println!("  sent solution: {}", solution.difficulty);
                // only send one solution per Go message
                state = ControlState::Stop;
            }
        });

        (cs.subscribe(), ss)
    }
}