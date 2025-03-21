use serde::{Deserialize, Serialize};
use serde_json::{json, Result, Value};
use std::io;

#[derive(Debug)]
struct Node {
    node_id: String,
    next_message_id: u64,
}

impl Node {
    fn get_next_msg_id(&mut self) -> u64 {
        self.next_message_id += 1;
        self.next_message_id.clone()
    }
}

enum MessageType {
    InitOk,
}

impl MessageType {
    fn as_str(&self) -> String {
        match self {
            MessageType::InitOk => String::from("init_ok"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct JSONBody {
    #[serde(rename = "type")]
    msg_type: String,
    msg_id: u64,
    node_id: String,
    node_ids: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct InitJSONResponseBody {
    #[serde(rename = "type")]
    msg_type: String,
    msg_id: u64,
    in_reply_to: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug)]
struct InitializationMessage {
    src: String,
    dest: String,
    body: JSONBody,
}

#[derive(Serialize, Deserialize, Debug)]
struct Response {
    src: String,
    dest: String,
    body: JSONBody,
}

fn init_reply(request: &JSONBody, node: &mut Node) -> InitJSONResponseBody {
    InitJSONResponseBody {
        msg_id: node.get_next_msg_id(),
        in_reply_to: Some(request.msg_id),
        msg_type: MessageType::InitOk.as_str(),
    }
}

fn init_envelope(node: &Node, dest: &str, body: InitJSONResponseBody) -> Value {
    json!({"src": node.node_id, dest: dest, "body": body})
}

fn main() -> Result<()> {
    // Read the node config
    let mut node = {
        let mut buffer = String::new();
        let stdin = io::stdin();

        let _ = stdin.read_line(&mut buffer).expect("Failed to read config");

        let config: InitializationMessage = serde_json::from_str(buffer.as_str())?;

        if config.body.msg_type != "init" {
            eprintln!("First message received must initialize");
        }
        let mut node = Node {
            node_id: config.body.node_id.clone(),
            next_message_id: 0,
        };
        let response_body = init_reply(&config.body, &mut node);
        let response = init_envelope(&node, &config.src, response_body);

        eprint!("Initialized Node: {:?}", &node);
        node
    };

    loop {
        continue;
    }
    Ok(())
}
