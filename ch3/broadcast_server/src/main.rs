use serde::de::Error;
use serde::{Deserialize, Serialize};
use serde_json::{json, Result};
use std::io;
use std::io::Write;
use std::sync::{Arc, Mutex};

type NodeId = String;
type MsgId = u64;

#[derive(Debug)]
struct Node {
    node_id: NodeId,
    node_ids: Vec<NodeId>,
    next_message_id: MsgId,
    stdout: Arc<Mutex<std::io::Stdout>>,
    stderr: Arc<Mutex<std::io::Stderr>>,
}

impl Node {
    fn get_next_msg_id(&mut self) -> MsgId {
        self.next_message_id += 1;
        self.next_message_id
    }

    fn log(&mut self, message: &str) -> Result<()> {
        match self.stderr.lock() {
            Ok(mut err_out_guard) => {
                writeln!(err_out_guard, "{}", message);
                Ok(())
            }
            Err(e) => Err(serde_json::Error::custom(format!(
                "Failed to acquire lock on logging: {}",
                e,
            ))),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
enum MessageBody {
    #[serde(rename = "init")]
    Init {
        msg_id: MsgId,
        node_id: NodeId,
        node_ids: Vec<String>,
    },
    #[serde(rename = "init_ok")]
    InitOk { msg_id: MsgId, in_reply_to: MsgId },
    #[serde(rename = "echo")]
    Echo { msg_id: MsgId, echo: String },
    #[serde(rename = "echo_ok")]
    EchoOk {
        msg_id: MsgId,
        echo: String,
        in_reply_to: MsgId,
    },
}

#[derive(Serialize, Deserialize, Debug)]
struct Message {
    src: NodeId,
    dest: NodeId,
    body: MessageBody,
}

fn acknowledge_init_node(
    node: &mut Node,
    init_request_id: MsgId,
    init_ok_tgt: NodeId,
) -> Result<()> {
    let response_body = MessageBody::InitOk {
        msg_id: node.get_next_msg_id(),
        in_reply_to: init_request_id,
    };

    let response = serde_json::to_string(&json!(Message {
        src: node.node_id.clone(),
        dest: init_ok_tgt,
        body: response_body,
    }))
    .expect("Failed to serialise InitOk response");
    println!("{}", response);
    Ok(())
}

fn main() -> Result<()> {
    // Read the node config
    let mut node_wrapper: Option<Node> = None;

    loop {
        let mut buffer = String::new();
        let stdin = io::stdin();
        let _ = stdin
            .read_line(&mut buffer)
            .expect("Failed to read message.");
        let message: Message = serde_json::from_str(buffer.as_str())?;

        if let MessageBody::Init {
            msg_id,
            node_id,
            node_ids,
        } = &message.body
        {
            let mut node = Node {
                node_id: node_id.to_string(),
                node_ids: node_ids.clone(),
                next_message_id: 0,
                stdout: Arc::new(Mutex::new(io::stdout())),
                stderr: Arc::new(Mutex::new(io::stderr())),
            };
            let _ = acknowledge_init_node(&mut node, *msg_id, message.src);
            let _ = node.log(&format!("Initialized Node: {:?}", &node_wrapper));
            node_wrapper = Some(node);
            continue;
        };
        match &mut node_wrapper {
            Some(node) => {
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
                        let _ = node.log(&format!("Sent response: {}", response));
                    }
                    _ => continue,
                }
            }
            None => {
                return Err(serde_json::Error::custom(
                    "Received non-init message before node initialization",
                ))
            }
        }
    }
}
