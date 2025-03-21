use serde::{Deserialize, Serialize};
use serde_json::{Result, Value};
use std::io;

struct NodeConfig {
    id: usize,
}

#[derive(Serialize, Deserialize, Debug)]
struct InitializationBody {
    #[serde(rename = "type")]
    msg_type: String,
    msg_id: u64,
    node_id: String,
    node_ids: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct InitializationMessage {
    src: String,
    dest: String,
    body: InitializationBody,
}

fn main() -> Result<()> {
    // Read the node config
    let mut buffer = String::new();
    let stdin = io::stdin();

    let _ = stdin.read_line(&mut buffer).expect("Failed to read config");

    let config: InitializationMessage = serde_json::from_str(buffer.as_str())?;

    println!("Ran until here.");
    loop {
        continue;
    }
    Ok(())
}
