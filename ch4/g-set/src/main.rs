use crossbeam::channel::unbounded;
use serde::de::Error as SerdeError;
use serde::{Deserialize, Serialize};
use serde_json::Result;
use std::collections::{HashMap, HashSet};
use std::error::Error as StdError;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{io, thread};

type NodeId = String;
type MsgId = u64;
type NodeMessage = i64;
type HandlerFn = Box<
    dyn Fn(&Arc<Node>, &Message) -> std::result::Result<(), Box<dyn StdError>> + Send + 'static,
>;
type PeriodicTask = Box<dyn Fn() -> std::result::Result<(), Box<dyn StdError>> + Send + 'static>;

#[derive(Debug)]
struct Handler {}
impl Handler {
    fn handle_echo(
        node: &Arc<Node>,
        message: &Message,
    ) -> std::result::Result<(), Box<dyn StdError>> {
        match &message.body {
            MessageBody::Echo { msg_id, echo } => {
                let response_body = MessageBody::EchoOk {
                    echo: echo.to_string(),
                    in_reply_to: *msg_id,
                };
                let _ = node.send(&message.src, response_body);
                Ok(())
            }
            _ => Err("handle_echo called on different message".into()),
        }
    }

    fn handle_topology(
        node: &Arc<Node>,
        message: &Message,
    ) -> std::result::Result<(), Box<dyn StdError>> {
        match &message.body {
            MessageBody::Topology { msg_id, topology } => {
                let mut topo_guard = node
                    .topology
                    .lock()
                    .map_err(|e| format!("Failed to lock topology: {}", e))?;
                *topo_guard = Some(topology.clone());
                let response_body = MessageBody::TopologyOk {
                    in_reply_to: *msg_id,
                };
                let _ = node.send(&message.src, response_body);
                Ok(())
            }
            _ => Err("handle_topology called on different message".into()),
        }
    }

    fn handle_broadcast(
        node: &Arc<Node>,
        message: &Message,
    ) -> std::result::Result<(), Box<dyn StdError>> {
        match message.body {
            MessageBody::Broadcast {
                msg_id,
                message: broadcast_message,
            } => {
                // Acknowledge Broadcast
                let response_body = MessageBody::BroadcastOk {
                    in_reply_to: msg_id,
                };
                let _ = node.send(&message.src, response_body);

                match node.messages_contain(&broadcast_message) {
                    Ok(true) => return Ok(()),
                    Ok(false) => {
                        let _ = node.add_message(broadcast_message);
                        let neighbors = {
                            if let Some(topology) = &*node.topology.lock().map_err(|e| {
                                format!("Failed to lock topology in broadcast: {}", e)
                            })? {
                                match topology.get(&node.node_id) {
                                    Some(neighbors) => neighbors.clone(),
                                    None => Vec::new(),
                                }
                            } else {
                                // No topology yet
                                return Ok(());
                            }
                        };

                        let neighbors: Vec<NodeId> = neighbors
                            .into_iter()
                            .filter(|n| n != &message.src)
                            .collect();
                        if neighbors.is_empty() {
                            return Ok(());
                        }

                        let unacked = Arc::new(Mutex::new(
                            neighbors.iter().cloned().collect::<HashSet<_>>(),
                        ));

                        let node_clone = Arc::clone(node);
                        let message_clone = broadcast_message.clone();
                        let unacked_clone = Arc::clone(&unacked);
                        thread::spawn(move || {
                            while !unacked_clone.lock().unwrap().is_empty() {
                                let currently_unacked = {
                                    let guard = unacked_clone.lock().unwrap();
                                    guard.iter().cloned().collect::<Vec<_>>()
                                };
                                for dest in currently_unacked {
                                    let dest_clone = dest.clone();
                                    let unacked_ref = Arc::clone(&unacked_clone);
                                    let broadcast_body = MessageBody::Broadcast {
                                        msg_id: node_clone.get_next_msg_id(),
                                        message: message_clone,
                                    };
                                    if let Err(e) = node_clone.rpc(
                                        &dest,
                                        broadcast_body,
                                        Box::new(move |_node, response| match &response.body {
                                            MessageBody::BroadcastOk { .. } => {
                                                let mut guard = unacked_ref.lock().unwrap();
                                                guard.remove(&dest_clone);
                                                Ok(())
                                            }
                                            _ => Ok(()),
                                        }),
                                    ) {
                                        let _ = node_clone.log(&format!(
                                            "Failed to send broadcast to {}: {}",
                                            dest, e
                                        ));
                                    }
                                }
                                thread::sleep(std::time::Duration::from_secs(1));
                            }
                            let _ =
                                node_clone.log(&format!("Acknowledged message: {}", message_clone));
                        });
                    }
                    Err(e) => {
                        return Err(format!(
                            "Error checking if node message contains broadcast message: {}",
                            e
                        )
                        .into());
                    }
                }
                Ok(())
            }
            _ => Err("handle_broadcast called on different message".into()),
        }
    }
    fn handle_read(
        node: &Arc<Node>,
        message: &Message,
    ) -> std::result::Result<(), Box<dyn StdError>> {
        match &message.body {
            MessageBody::Read { msg_id } => {
                let Ok(messages) = node.read_messages() else {
                    return Err(serde_json::Error::custom(&format!(
                        "Failed to read messages on node {}",
                        node.node_id
                    ))
                    .into());
                };
                let response_body = MessageBody::ReadOk {
                    in_reply_to: *msg_id,
                    messages,
                };
                let _ = node.send(&message.src, response_body);
                Ok(())
            }
            _ => Err("handle_read called on different message".into()),
        }
    }

    fn handle_add(
        node: &Arc<Node>,
        message: &Message,
    ) -> std::result::Result<(), Box<dyn StdError>> {
        match &message.body {
            MessageBody::Add { msg_id, element } => {
                if let Ok(()) = node.add_message(*element) {
                    let response_body = MessageBody::AddOk {
                        in_reply_to: *msg_id,
                    };
                    let _ = node.send(&message.src, response_body);
                    Ok(())
                } else {
                    return Err(format!("Failed to insert message to set").into());
                }
            }

            _ => Err("handle_add called on different message type".into()),
        }
    }
}

struct Node {
    node_id: NodeId,
    topology: Arc<Mutex<Option<HashMap<NodeId, Vec<NodeId>>>>>,
    messages: Arc<Mutex<HashSet<NodeMessage>>>,
    next_message_id: AtomicU64,
    stdout: Arc<Mutex<std::io::Stdout>>,
    stderr: Arc<Mutex<std::io::Stderr>>,
    stdin: Arc<Mutex<std::io::Stdin>>,
    callbacks: Arc<Mutex<HashMap<MsgId, HandlerFn>>>,
    periodic_tasks: Arc<Mutex<Vec<(Duration, PeriodicTask)>>>,
}

impl Node {
    fn new(node_id: &NodeId) -> Arc<Self> {
        Arc::new(Node {
            node_id: node_id.to_string(),
            messages: Arc::new(Mutex::new(HashSet::new())),
            callbacks: Arc::new(Mutex::new(HashMap::new())),
            topology: Arc::new(Mutex::new(None)),
            next_message_id: AtomicU64::new(0),
            stdout: Arc::new(Mutex::new(io::stdout())),
            stderr: Arc::new(Mutex::new(io::stderr())),
            stdin: Arc::new(Mutex::new(io::stdin())),
            periodic_tasks: Arc::new(Mutex::new(Vec::new())),
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

    fn every<F>(self: &Arc<Self>, duration: Duration, f: F)
    where
        F: Fn(&Self) -> std::result::Result<(), Box<dyn StdError>> + Send + 'static,
    {
        let self_clone = Arc::clone(self);
        let wrapped_fn = move || f(&self_clone);
        let mut tasks = self.periodic_tasks.lock().unwrap();
        tasks.push((duration, Box::new(wrapped_fn)))
    }

    fn run_event_loop(self: &Arc<Self>) {
        let node = Arc::clone(self);
        std::thread::spawn(move || {
            let mut last_runs: Vec<std::time::Instant> = Vec::new();
            loop {
                {
                    // Update tasks run timestamps
                    let tasks = node.periodic_tasks.lock().unwrap();
                    while last_runs.len() < tasks.len() {
                        last_runs.push(std::time::Instant::now());
                    }
                }

                {
                    // Check time passed and run each task
                    let tasks = node.periodic_tasks.lock().unwrap();
                    for (i, (duration, task)) in tasks.iter().enumerate() {
                        let now = std::time::Instant::now();
                        if now.duration_since(last_runs[i]) >= *duration {
                            if let Err(e) = task() {
                                let _ = &node.log(&format!("Error running periodic task: {}", e));
                            }
                            last_runs[i] = now;
                        }
                    }
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        });
    }

    fn rpc(
        &self,
        dest: &NodeId,
        body: MessageBody,
        response_handler: HandlerFn,
    ) -> std::result::Result<(), Box<dyn StdError>> {
        let rpc_id = body.msg_id().expect("Body contains no message id");
        let mut callbacks = self
            .callbacks
            .lock()
            .map_err(|e| format!("Could not acquire lock on callbacks: {}", e))?;
        let _ = callbacks.insert(rpc_id, response_handler);
        Ok(self.send(dest, body)?)
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
    #[serde(rename = "add")]
    Add { msg_id: MsgId, element: NodeMessage },

    #[serde(rename = "add_ok")]
    AddOk { in_reply_to: MsgId },
}

impl MessageBody {
    fn is_reply(&self) -> Option<MsgId> {
        match self {
            Self::InitOk { in_reply_to, .. } => Some(*in_reply_to),
            Self::EchoOk { in_reply_to, .. } => Some(*in_reply_to),
            Self::TopologyOk { in_reply_to, .. } => Some(*in_reply_to),
            Self::BroadcastOk { in_reply_to, .. } => Some(*in_reply_to),
            Self::ReadOk { in_reply_to, .. } => Some(*in_reply_to),
            Self::AddOk { in_reply_to, .. } => Some(*in_reply_to),
            _ => None,
        }
    }
    fn msg_id(&self) -> Option<MsgId> {
        match self {
            Self::Read { msg_id } => Some(*msg_id),
            Self::Echo { msg_id, .. } => Some(*msg_id),
            Self::Topology { msg_id, .. } => Some(*msg_id),
            Self::Broadcast { msg_id, .. } => Some(*msg_id),
            Self::Init { msg_id, .. } => Some(*msg_id),
            Self::Add { msg_id, .. } => Some(*msg_id),
            _ => None,
        }
    }
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
            let _ = node.log(&format!("Initialized Node: {}", &node.node_id));
            let response_body = MessageBody::InitOk {
                in_reply_to: *msg_id,
            };
            let _ = node.send(&message.src, response_body);
            node
        } else {
            return Err(format!("First message received must be init",).into());
        }
    };

    // Start executing periodic tasks
    node.run_event_loop();

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
                // If something is a reply, check the callbacks dict...
                if let Some(reply_to) = message.body.is_reply() {
                    let callback_opt = {
                        let mut callbacks = worker_node.callbacks.lock().unwrap();
                        callbacks.remove(&reply_to)
                    };
                    if let Some(callback) = callback_opt {
                        if let Err(e) = callback(&worker_node, &message) {
                            let _ = worker_node.log(&format!("Error in callback: {}", e));
                        }
                        continue;
                    }
                }
                // ...otherwise handle the message via handlers
                match message.body {
                    MessageBody::Echo { .. } => {
                        let _ = Handler::handle_echo(&worker_node, &message);
                    }
                    MessageBody::Topology { .. } => {
                        let _ = Handler::handle_topology(&worker_node, &message);
                    }
                    MessageBody::Broadcast { .. } => {
                        let _ = Handler::handle_broadcast(&worker_node, &message);
                    }
                    MessageBody::Read { .. } => {
                        let _ = Handler::handle_read(&worker_node, &message);
                    }
                    MessageBody::Add { .. } => {
                        let _ = Handler::handle_add(&worker_node, &message);
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
