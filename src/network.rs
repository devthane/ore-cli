use drillx::Solution;
use futures::prelude::*;
use tokio::net;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_serde::formats::*;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use crate::Miner;

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub struct SolutionResult {
    pub solution: Solution,
    pub difficulty: u32,
}

impl SolutionResult {
    pub fn new(solution: Solution, difficulty: u32) -> Self {
        SolutionResult {
            solution,
            difficulty
        }
    }
}

impl Miner {
    pub async fn solution_receiver(&self) -> mpsc::Receiver<SolutionResult> {
        let (sender, receiver) = mpsc::channel::<SolutionResult>(1000);
        tokio::spawn(async move {
            let Ok(listener) = net::TcpListener::bind("0.0.0.0:8000").await else {
                panic!("failed to listen on port 8080")
            };
            loop {
                if sender.is_closed() {
                    return;
                }
                let Ok((stream, _)) = listener.accept().await else {
                    println!("connection failed");
                    continue;
                };

                let length_delimited = FramedRead::new(stream, LengthDelimitedCodec::new());

                let mut deserialized = tokio_serde::SymmetricallyFramed::new(
                    length_delimited,
                    SymmetricalJson::<SolutionResult>::default(),
                );

                let Ok(message) = deserialized.try_next().await else {
                    println!("did not receive solution");
                    continue;
                };
                let Some(solution) = message else {
                    println!("failed to parse solution");
                    continue;
                };
                let Err(err) = sender.send(solution).await else {
                    continue;
                };
                println!("failed for pass solution: {:?}", err)
            }
        });
        receiver
    }

    pub async fn send_solution(&self, forward_address: String, solution: SolutionResult) {
        let stream = TcpStream::connect(forward_address).await.unwrap();
        let length_delimited = FramedWrite::new(stream, LengthDelimitedCodec::new());
        let mut serialized = tokio_serde::SymmetricallyFramed::new(length_delimited, SymmetricalJson::default());

        let Err(err) = serialized.send(solution).await else {
            return;
        };

        println!("problem sending solution: {:?}", err)
    }
}