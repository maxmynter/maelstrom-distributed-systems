use crossbeam::channel::unbounded;
use serde::de::Error as SerdeError;
use serde::{Deserialize, Serialize};
use serde_json::Result;
use std::collections::{HashMap, HashSet};
use std::error::Error as StdError;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::{io, thread};

type NodeId = String;
type MsgId = u64;
type NodeMessage = i64;

#[derive(Debug)]
struct Node {
    node_id: NodeId,
    topology: Arc<Mutex<Option<HashMap<NodeId, Vec<NodeId>>>>>,
    messages: Arc<Mutex<HashSet<NodeMessage>>>,
    next_message_id: AtomicU64,
    stdout: Arc<Mutex<std::io::Stdout>>,
    stderr: Arc<Mutex<std::io::Stderr>>,
    stdin: Arc<Mutex<std::io::Stdin>>,
}

impl Node {
    fn new(node_id: &NodeId) -> Arc<Self> {
        Arc::new(Node {
            node_id: node_id.to_string(),
            messages: Arc::new(Mutex::new(HashSet::new())),
            topology: Arc::new(Mutex::new(None)),
            next_message_id: AtomicU64::new(0),
            stdout: Arc::new(Mutex::new(io::stdout())),
            stderr: Arc::new(Mutex::new(io::stderr())),
            stdin: Arc::new(Mutex::new(io::stdin())),
        })
    }
    fn get_next_msg_id(&self) -> MsgId {
        self.next_message_id.fetch_add(1 as u64, Ordering::SeqCst)
    }

    fn add_message(&self, message: NodeMessage) -> std::result::Result<(), Box<dyn StdError>> {
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

    fn read_messages(&self) -> std::result::Result<Vec<NodeMessage>, Box<dyn StdError>> {
        let messages = self
            .messages
            .lock()
            .map_err(|e| format!("Failed to acquire lock on messages: {}", e))?;
        let messages_vec = messages.iter().cloned().collect();
        Ok(messages_vec)
    }

    fn messages_contain(
        &self,
        message: &NodeMessage,
    ) -> std::result::Result<bool, Box<dyn StdError>> {
        let messages = self
            .messages
            .lock()
            .map_err(|e| format!("Failed to lock messages for read: {}", e))?;
        Ok(messages.contains(&message))
    }

    fn log(&self, text: &str) -> std::result::Result<(), Box<dyn StdError>> {
        match self.stderr.lock() {
            Ok(mut err_out_guard) => {
                let _ = writeln!(err_out_guard, "{}", text)?;
                Ok(())
            }
            Err(e) => Err(format!("Failed to acquire lock on logging: {}", e).into()),
        }
    }

    fn send(&self, dest: &NodeId, body: MessageBody) -> Result<()> {
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

    fn handle_echo(&self, message: &Message) -> std::result::Result<(), Box<dyn StdError>> {
        match &message.body {
            MessageBody::Echo { msg_id, echo } => {
                let response_body = MessageBody::EchoOk {
                    echo: echo.to_string(),
                    in_reply_to: *msg_id,
                };
                let _ = self.send(&message.src, response_body);
                Ok(())
            }
            _ => Err("handle_echo called on different message".into()),
        }
    }

    fn handle_topology(&self, message: &Message) -> std::result::Result<(), Box<dyn StdError>> {
        match &message.body {
            MessageBody::Topology { msg_id, topology } => {
                let mut topo_guard = self
                    .topology
                    .lock()
                    .map_err(|e| format!("Failed to lock topology: {}", e))?;
                *topo_guard = Some(topology.clone());
                let response_body = MessageBody::TopologyOk {
                    in_reply_to: *msg_id,
                };
                let _ = self.send(&message.src, response_body);
                Ok(())
            }
            _ => Err("handle_topology called on different message".into()),
        }
    }

    fn handle_broadcast(&self, message: &Message) -> std::result::Result<(), Box<dyn StdError>> {
        match message.body {
            MessageBody::Broadcast {
                msg_id,
                message: broadcast_message,
            } => {
                match self.messages_contain(&broadcast_message) {
                    Ok(true) => {}
                    Ok(false) => {
                        let _ = self.add_message(broadcast_message);

                        // Gossip message to neighbors
                        if let Some(topology) = &*self
                            .topology
                            .lock()
                            .map_err(|e| format!("Failed to lock topology in broadcast: {}", e))?
                        {
                            let neighbors = match topology.get(&self.node_id) {
                                Some(neighbors) => neighbors.clone(),
                                None => Vec::new(),
                            };
                            for tgt_node_id in neighbors {
                                if tgt_node_id == message.src {
                                    // Skip the origin of the broadcast
                                    continue;
                                }
                                let response_body = MessageBody::Broadcast {
                                    msg_id: self.get_next_msg_id(),
                                    message: broadcast_message.clone(),
                                };
                                let _ = self.send(&tgt_node_id, response_body);
                            }
                        }
                    }
                    Err(e) => {
                        return Err(format!(
                            "Error checking if node message contains broadcast message: {}",
                            e
                        )
                        .into());
                    }
                }
                // Acknowledge Broadcast
                let response_body = MessageBody::BroadcastOk {
                    in_reply_to: msg_id,
                };
                let _ = self.send(&message.src, response_body);
                Ok(())
            }
            _ => Err("handle_broadcast called on different message".into()),
        }
    }
    fn handle_read(&self, message: &Message) -> std::result::Result<(), Box<dyn StdError>> {
        match &message.body {
            MessageBody::Read { msg_id } => {
                let Ok(messages) = self.read_messages() else {
                    return Err(serde_json::Error::custom(&format!(
                        "Failed to read messages on node {}",
                        self.node_id
                    ))
                    .into());
                };
                let response_body = MessageBody::ReadOk {
                    in_reply_to: *msg_id,
                    messages,
                };
                let _ = self.send(&message.src, response_body);
                Ok(())
            }
            _ => Err("handle_read called on different message".into()),
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
    InitOk { in_reply_to: MsgId },
    #[serde(rename = "echo")]
    Echo { msg_id: MsgId, echo: String },
    #[serde(rename = "echo_ok")]
    EchoOk { echo: String, in_reply_to: MsgId },
    #[serde(rename = "topology")]
    Topology {
        msg_id: MsgId,
        topology: std::collections::HashMap<NodeId, Vec<NodeId>>,
    },
    #[serde(rename = "topology_ok")]
    TopologyOk { in_reply_to: MsgId },
    #[serde(rename = "broadcast")]
    Broadcast { msg_id: MsgId, message: NodeMessage },
    #[serde(rename = "broadcast_ok")]
    BroadcastOk { in_reply_to: MsgId },
    #[serde(rename = "read")]
    Read { msg_id: MsgId },
    #[serde(rename = "read_ok")]
    ReadOk {
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

fn message_from_stdin(stdin: &io::Stdin) -> Result<Message> {
    let mut buffer = String::new();
    let _ = stdin
        .read_line(&mut buffer)
        .expect("Failed to read message.");
    let message: Message = serde_json::from_str(buffer.as_str())?;
    Ok(message)
}

fn main() -> std::result::Result<(), Box<dyn StdError>> {
    let node = {
        let stdin = io::stdin();
        let message = message_from_stdin(&stdin)?;
        if let MessageBody::Init {
            msg_id,
            node_id,
            node_ids: _,
        } = &message.body
        {
            let node = Node::new(node_id);
            let _ = node.log(&format!("Initialized Node: {:?}", &node));
            let response_body = MessageBody::InitOk {
                in_reply_to: *msg_id,
            };
            let _ = node.send(&message.src, response_body);
            node
        } else {
            return Err(format!("First message received must be init",).into());
        }
    };
    let (tx, rx) = unbounded::<Message>();
    let node_reader = Arc::clone(&node);

    let reader_handle = thread::spawn(move || loop {
        let message = {
            let stdin = node_reader.stdin.lock().expect("Failed to lock stdin");
            match message_from_stdin(&stdin) {
                Ok(msg) => msg,
                Err(e) => {
                    let _ = node_reader.log(&format!("Error reading message: {}", e));
                    continue;
                }
            }
        };
        if tx.send(message).is_err() {
            break;
        }
    });

    let num_workers = 4;
    let mut worker_handles = Vec::with_capacity(num_workers);

    for worker_id in 0..num_workers {
        let worker_rx = rx.clone();
        let worker_node = Arc::clone(&node);

        let handle = thread::spawn(move || {
            let _ = worker_node.log(&format!("Started worker: {}", worker_id));
            for message in worker_rx {
                match message.body {
                    MessageBody::Echo { msg_id: _, echo: _ } => {
                        let _ = worker_node.handle_echo(&message);
                    }
                    MessageBody::Topology {
                        msg_id: _,
                        topology: _,
                    } => {
                        let _ = worker_node.handle_topology(&message);
                    }
                    MessageBody::Broadcast {
                        msg_id: _,
                        message: _,
                    } => {
                        let _ = worker_node.handle_broadcast(&message);
                    }
                    MessageBody::Read { msg_id: _ } => {
                        let _ = worker_node.handle_read(&message);
                    }
                    _ => {
                        let _ = worker_node.log("Received message with no known handler");
                    }
                }
            }
        });
        worker_handles.push(handle);
    }
    for handle in worker_handles {
        let _ = handle.join();
    }
    let _ = reader_handle.join();
    Ok(())
}
