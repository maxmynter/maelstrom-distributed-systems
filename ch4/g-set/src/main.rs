use anyhow::{Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::error::Error as StdError;
use std::io::Write;
use std::sync::{Arc, Mutex};

type NodeId = u64;
type MsgId = u64;
type MessageContent = u64;

type HandlerFn = Box<
    dyn Fn(&Arc<Node>, &Message) -> std::result::Result<(), Box<dyn StdError>> + Send + 'static,
>;

#[derive(Serialize, Deserialize, Debug)]
struct Message {
    src: NodeId,
    dest: NodeId,
    body: MessageBody,
}

#[derive(Serialize, Deserialize, Debug)]
enum MessageBody {
    #[serde(rename = "init")]
    Init { msg_id: MsgId, node_id: NodeId },
    #[serde(rename = "init_ok")]
    InitOk { in_reply_to: MsgId },
    #[serde(rename = "add")]
    Add { element: u64, msg_id: MsgId },
    #[serde(rename = "add_ok")]
    AddOk,
    #[serde(rename = "read")]
    Read,
    #[serde(rename = "read_ok")]
    ReadOk,
}

struct Node {
    node_id: NodeId,
    messages: Arc<Mutex<HashSet<MessageContent>>>,
    stdin: Arc<Mutex<std::io::Stdin>>,
    stdout: Arc<Mutex<std::io::Stdout>>,
    stderr: Arc<Mutex<std::io::Stderr>>,
    callbacks: Arc<Mutex<HashMap<MsgId, HandlerFn>>>,
}

impl Node {
    fn new(node_id: NodeId) -> Node {
        Node {
            node_id,
            messages: Arc::new(Mutex::new(HashSet::new())),
            stdin: Arc::new(Mutex::new(std::io::stdin())),
            stdout: Arc::new(Mutex::new(std::io::stdout())),
            stderr: Arc::new(Mutex::new(std::io::stderr())),
            callbacks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn receive(&self) -> Result<Message> {
        let mut buffer = String::new();
        let stdin = self.stdin.lock().unwrap();
        let _ = stdin
            .read_line(&mut buffer)
            .expect("Node failed to read stdin");
        let message: Message = serde_json::from_str(buffer.as_str())?;
        Ok(message)
    }

    fn send(&self, dest: &NodeId, body: MessageBody) -> Result<()> {
        let message = Message {
            src: self.node_id,
            dest: *dest,
            body,
        };
        let jsonified = serde_json::to_string(&message).expect("Failed to serialize message");
        let _ = match self.stdout.lock() {
            Ok(mut stdout_guard) => {
                writeln!(stdout_guard, "{}", jsonified)
            }
            Err(e) => bail!("Failed to capture lock on stdout for sending: {}", e),
        };
        Ok(())
    }

    fn add_message(&self, message: MessageContent) -> Result<()> {
        let mut messages = self
            .messages
            .lock()
            .map_err(|e| anyhow!("Failed to lock messages: {}", e))?;
        messages.insert(message);
        Ok(())
    }

    fn log(&self, text: String) {
        let mut stderr = self
            .stderr
            .lock()
            .expect("Node failed to acquire lock on stderr");
        writeln!(stderr, "Node {}: {}", self.node_id, text).expect("Failed to log");
    }
}

fn init_node_from_stdin() -> Result<Node> {
    // This does not work in threaded execution.
    // Launch threads only after node initalization
    let mut buffer = String::new();
    let _ = std::io::stdin()
        .read_line(&mut buffer)
        .expect("Failed to read stdin");
    let message: Message = serde_json::from_str(buffer.as_str())?;
    if let MessageBody::Init { msg_id: _, node_id } = message.body {
        Ok(Node::new(node_id))
    } else {
        return Err(anyhow!("Message received was not Init"));
    }
}

fn main() -> Result<()> {
    let node = init_node_from_stdin()?;
    loop {
        match node.receive() {
            Ok(message) => match message.body {
                MessageBody::Add { msg_id: _, element } => {
                    let _ = node.add_message(element);
                }
                MessageBody::Read => todo!(),
                _ => {
                    node.log(format!("Unkown message body: {:?}", message));
                }
            },
            Err(e) => {
                node.log(format!("Failed to receive message: {}", e));
            }
        }
    }
}
