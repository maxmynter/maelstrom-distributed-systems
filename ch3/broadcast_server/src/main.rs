use serde::de::Error;
use serde::{Deserialize, Serialize};
use serde_json::Result;
use std::collections::{HashMap, HashSet};
use std::io;
use std::io::Write;
use std::sync::{Arc, Mutex};

type NodeId = String;
type MsgId = u64;
type NodeMessage = i64;

#[derive(Debug)]
struct Node {
    node_id: NodeId,
    node_ids: Vec<NodeId>,
    topology: Option<HashMap<NodeId, Vec<NodeId>>>,
    messages: Arc<Mutex<HashSet<NodeMessage>>>,
    next_message_id: MsgId,
    stdout: Arc<Mutex<std::io::Stdout>>,
    stderr: Arc<Mutex<std::io::Stderr>>,
}

impl Node {
    fn get_next_msg_id(&mut self) -> MsgId {
        self.next_message_id += 1;
        self.next_message_id
    }

    fn add_message(
        &mut self,
        message: NodeMessage,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let was_inserted = {
            let mut messages = self
                .messages
                .lock()
                .map_err(|e| format!("Failed to acquire lock on messages: {}", e))?;
            messages.insert(message)
        };
        let _ = self.log(&format!(
            "Node({}): {} message '{}'",
            self.node_id,
            if was_inserted {
                "Inserted"
            } else {
                "Already had"
            },
            &message
        ));
        Ok(())
    }

    fn read_messages(
        &mut self,
    ) -> std::result::Result<Vec<NodeMessage>, Box<dyn std::error::Error>> {
        let messages = self
            .messages
            .lock()
            .map_err(|e| format!("Failed to aqcuire lock on messages: {}", e))?;
        let messages_vec = messages.iter().cloned().collect();
        Ok(messages_vec)
    }

    fn log(&mut self, text: &str) -> std::result::Result<(), Box<dyn std::error::Error>> {
        match self.stderr.lock() {
            Ok(mut err_out_guard) => {
                let _ = writeln!(err_out_guard, "{}", text)?;
                Ok(())
            }
            Err(e) => Err(format!("Failed to acquire lock on logging: {}", e).into()),
        }
    }

    fn send(&mut self, dest: &NodeId, body: MessageBody) -> Result<()> {
        let message = Message {
            src: self.node_id.clone(),
            dest: dest.to_string(),
            body,
        };
        let jsonified = serde_json::to_string(&message).expect("Failed to serialise message");
        let _ = match self.stdout.lock() {
            Ok(mut stdout_guard) => {
                let _ = writeln!(stdout_guard, "{}", jsonified)
                    .map_err(|e| serde_json::Error::custom(e.to_string()));
                Ok(())
            }
            Err(e) => Err(serde_json::Error::custom(format!(
                "Failed to acquire lock on stdout for sending: {}",
                e
            ))),
        };
        let _ = self.log(&format!("Sent: {}", jsonified));
        Ok(())
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
    #[serde(rename = "topology")]
    Topology {
        msg_id: MsgId,
        topology: std::collections::HashMap<NodeId, Vec<NodeId>>,
    },
    #[serde(rename = "topology_ok")]
    TopologyOk { msg_id: MsgId, in_reply_to: MsgId },
    #[serde(rename = "broadcast")]
    Broadcast { msg_id: MsgId, message: NodeMessage },
    #[serde(rename = "broadcast_ok")]
    BroadcastOk { msg_id: MsgId, in_reply_to: MsgId },
    #[serde(rename = "read")]
    Read { msg_id: MsgId },
    #[serde(rename = "read_ok")]
    ReadOk {
        msg_id: MsgId,
        in_reply_to: MsgId,
        messages: Vec<NodeMessage>,
    },
}

#[derive(Serialize, Deserialize, Debug)]
struct Message {
    src: NodeId,
    dest: NodeId,
    body: MessageBody,
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
                messages: Arc::new(Mutex::new(HashSet::new())),
                topology: None,
                next_message_id: 0,
                stdout: Arc::new(Mutex::new(io::stdout())),
                stderr: Arc::new(Mutex::new(io::stderr())),
            };
            let _ = node.log(&format!("Initialized Node: {:?}", &node));

            let response_body = MessageBody::InitOk {
                msg_id: node.get_next_msg_id(),
                in_reply_to: *msg_id,
            };
            let _ = node.send(&message.src, response_body);
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
                        let _ = node.send(&message.src, response_body);
                    }
                    MessageBody::Topology { msg_id, topology } => {
                        node.topology = Some(topology);
                        let response_body = MessageBody::TopologyOk {
                            msg_id: node.get_next_msg_id(),
                            in_reply_to: msg_id,
                        };
                        let _ = node.send(&message.src, response_body);
                    }
                    MessageBody::Broadcast {
                        msg_id,
                        message: broadcast_message,
                    } => {
                        let _ = node.add_message(broadcast_message);
                        let response_body = MessageBody::BroadcastOk {
                            msg_id: node.get_next_msg_id(),
                            in_reply_to: msg_id,
                        };
                        let _ = node.send(&message.src, response_body);
                    }
                    MessageBody::Read { msg_id } => {
                        let Ok(messages) = node.read_messages() else {
                            return Err(serde_json::Error::custom(&format!(
                                "Failed to read messages on node {}",
                                node.node_id
                            ))
                            .into());
                        };
                        let response_body = MessageBody::ReadOk {
                            msg_id: node.get_next_msg_id(),
                            in_reply_to: msg_id,
                            messages,
                        };
                        let _ = node.send(&message.src, response_body);
                    }
                    _ => {
                        let _ = node.log("Received message with no known handler");
                    }
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
