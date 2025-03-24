use serde::de::Error;
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
    EchoOk,
}

impl MessageType {
    fn as_str(&self) -> String {
        match self {
            MessageType::InitOk => String::from("init_ok"),
            MessageType::EchoOk => String::from("echo_ok"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
enum MessageBody {
    #[serde(rename = "init")]
    Init {
        msg_id: u64,
        node_id: String,
        node_ids: Vec<String>,
    },
    #[serde(rename = "init_ok")]
    InitOk { msg_id: u64, in_reply_to: u64 },
    #[serde(rename = "echo")]
    Echo { msg_id: u64, echo: String },
    #[serde(rename = "echo_ok")]
    EchoOk {
        msg_id: u64,
        echo: String,
        in_reply_to: u64,
    },
}

#[derive(Serialize, Deserialize, Debug)]
struct Message {
    src: String,
    dest: String,
    body: MessageBody,
}

fn main() -> Result<()> {
    // Read the node config
    let mut node = {
        let mut buffer = String::new();
        let stdin = io::stdin();

        let _ = stdin.read_line(&mut buffer).expect("Failed to read config");

        let config: Message = serde_json::from_str(buffer.as_str())?;
        eprintln!("Received: {:?}", config);

        let (init_request_id, node_id, node_ids) = match &config.body {
            MessageBody::Init {
                msg_id,
                node_id,
                node_ids,
            } => (msg_id, node_id, node_ids),

            _ => {
                return Err(serde_json::Error::custom(
                    "First message received wasn't init",
                ))
            }
        };

        let mut node = Node {
            node_id: node_id.clone(),
            next_message_id: 0,
        };
        let response_body = MessageBody::InitOk {
            msg_id: node.get_next_msg_id(),
            in_reply_to: init_request_id.clone(),
        };
        let response = serde_json::to_string(&json!(Message {
            src: node.node_id.clone(),
            dest: config.src.clone(),
            body: response_body,
        }))
        .expect("Failed to serialise InitOk response");
        println!("{}", response);

        eprint!("Initialized Node: {:?}", &node);
        node
    };

    loop {
        let mut buffer = String::new();
        let stdin = io::stdin();
        let _ = stdin
            .read_line(&mut buffer)
            .expect("Failed to read message.");
        let message: Message = serde_json::from_str(buffer.as_str())?;
        match message.body {
            MessageBody::Echo { msg_id, echo } => {
                // Create and stdout the echo response
                let response_body = MessageBody::EchoOk {
                    msg_id: node.get_next_msg_id(),
                    echo,
                    in_reply_to: msg_id,
                };
                let response = serde_json::to_string(&json!(Message {
                    src: node.node_id.clone(),
                    dest: message.src.clone(),
                    body: response_body
                }))
                .expect("Failed to serizalize Echo Response");
                println!("{}", response);
            }
            _ => continue,
        }
    }
    Ok(())
}
