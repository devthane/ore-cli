use drillx::Solution;
use futures::prelude::*;
use tokio::{net, select};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_serde::formats::*;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use crate::Miner;

pub const NETWORK_WINDOW: u64 = 10;

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
                println!("failed to listen on port 8000");
                return;
            };
            loop {
                let stream = select! {
                    _ = sender.closed() => {
                        break;
                    }
                    result = listener.accept() => {
                        if let Ok((stream, _)) = result {
                            stream
                        } else {
                            println!("connection failed");
                            break;
                        }
                    }
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

    pub async fn solution_sender(&self, forward_address: String) -> mpsc::Sender<SolutionResult> {
        let stream = TcpStream::connect(forward_address).await.unwrap();
        
        let (sender, mut receiver) = mpsc::channel::<SolutionResult>(1000);
        
        tokio::spawn(async move {
            let solution = receiver.recv().await.unwrap();
            receiver.close();
            
            let length_delimited = FramedWrite::new(stream, LengthDelimitedCodec::new());
            let mut serialized = tokio_serde::SymmetricallyFramed::new(length_delimited, SymmetricalJson::default());

            let Err(err) = serialized.send(solution).await else {
                return;
            };
            println!("problem sending solution: {:?}", err)
        });
        
        sender
    }
}