use serde::de::Error;
use serde::{Deserialize, Serialize};
use serde_json::{json, Result};
use std::io;

type NodeId = String;
type MsgId = u64;

#[derive(Debug)]
struct Node {
    node_id: NodeId,
    next_message_id: MsgId,
}

impl Node {
    fn get_next_msg_id(&mut self) -> MsgId {
        self.next_message_id += 1;
        self.next_message_id
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

fn parse_init_node() -> Result<(MsgId, NodeId, NodeId, Vec<NodeId>)> {
    let mut buffer = String::new();
    let stdin = io::stdin();

    let _ = stdin.read_line(&mut buffer).expect("Failed to read config");

    let config: Message = serde_json::from_str(buffer.as_str())?;
    eprintln!("Received: {:?}", config);

    let init_request_src = config.src;

    let (init_request_id, node_id, node_ids) = match config.body {
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
    Ok((init_request_id, init_request_src, node_id, node_ids))
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

fn initialize_node() -> Result<Node> {
    let (init_request_id, init_request_src, node_id, _node_ids) = parse_init_node()?;

    let mut node = Node {
        node_id: node_id.clone(),
        next_message_id: 0,
    };
    let _ = acknowledge_init_node(&mut node, init_request_id, init_request_src);
    eprint!("Initialized Node: {:?}", &node);
    Ok(node)
}

fn main() -> Result<()> {
    // Read the node config
    let mut node = initialize_node()?;

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
}
