pub use crossbeam_channel as channel;
use crossbeam_channel::{Receiver, Sender};
use lazy_static::lazy_static;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::thread;

use cita_logger::error;

// pub const ZEROMQ_IP: &'static str = "ZEROMQ_IP";
// pub const ZEROMQ_BASE_PORT: &'static str = "ZEROMQ_BASE_PORT";

const AMQP_URL: &'static str = "AMQP_URL";

lazy_static! {
    static ref SERVICE_PORT_INDEX: HashMap<&'static str, usize> = {
        let mut m = HashMap::new();
        m.insert("network", 0);
        m.insert("chain", 1);
        m.insert("jsonrpc", 2);
        m.insert("consensus", 3);
        m.insert("executor", 4);
        m.insert("auth", 5);
        m.insert("snapshot", 6);
        m.insert("network_auth", 7);
        m.insert("network_consensus", 8);
        m
    };
    static ref CONTEXT: zmq::Context = zmq::Context::new();
}

fn new_sub_init(ctx: &zmq::Context, url: String) -> zmq::Socket {
    let skt = ctx.socket(zmq::SUB).unwrap();
    skt.connect(&*url)
        .expect(&*format!("zmq subscribe socket create error {}", url));
    skt
}

fn thread_work(sub: &zmq::Socket, msg_sender: &Sender<(String, Vec<u8>)>) {
    let topic = sub.recv_string(0).unwrap().unwrap();
    let msg = sub.recv_bytes(0).unwrap();
    let _ = msg_sender.send((topic, msg));
}

pub fn start_zeromq(
    name: &str,
    keys: Vec<String>,
    tx: Sender<(String, Vec<u8>)>,
    rx: Receiver<(String, Vec<u8>)>,
    ip: String,
) {
    let url = std::env::var(AMQP_URL).expect(&*format!("{} must be set", AMQP_URL));
    let mut s = DefaultHasher::new();
    url.hash(&mut s);
    let base_port = (s.finish() % 1000000) as usize;

    let mut ip = ip;
    let publisher = CONTEXT.socket(zmq::PUB).unwrap();
    let mut publish_flag = true;
    let mut delimiter = ":".to_string();
    let mut con_type = "tcp".to_string();
    let mut wild_cast = "*".to_string();
    if ip == "localhost" || ip == "127.0.0.1" {
        delimiter = "".to_string();
        con_type = "ipc".to_string();
        wild_cast = "ipc".to_string();
        ip = "ipc".to_string();
    }

    /* URL like tcp://8.8.8.8:6000  or like ipc://ipc6000 */
    let _ = SERVICE_PORT_INDEX.get(&name).map_or_else(
        || {
            publish_flag = false;
        },
        |idx| {
            let url = format!(
                "{}://{}{}{}",
                con_type,
                wild_cast,
                delimiter,
                base_port + idx
            );
            publisher
                .bind(&*url)
                .expect(&*format!("publisher bind error {}", url));
        },
    );

    if publish_flag {
        let _ = thread::Builder::new()
            .name("publisher".to_string())
            .spawn(move || loop {
                let ret = rx.recv();

                if ret.is_err() {
                    break;
                }
                let (topic, msg) = ret.unwrap();
                let _ = publisher.send_multipart(&[&(topic.into_bytes())], zmq::SNDMORE);
                let _ = publisher.send(&msg, 0);
            });
    }

    //sub

    // macro_rules!  {
    //     (ctx:&CONTEXT,$name:ident,$base:expr,$inc:expr,) => {
    //         let url = format!("ipc://{}", $base+$inc);
    //         let $name = ctx.socket(zmq::SUB).unwrap();
    //         $name.connect(&*url)
    //             .expect(&*format!("zmq subscribe socket create error {}", url));
    //         $name
    //     };
    // }

    let network_subscriber = new_sub_init(
        &CONTEXT,
        format!("{}://{}{}{}", con_type, ip, delimiter, base_port),
    );
    let chain_subscriber = new_sub_init(
        &CONTEXT,
        format!("{}://{}{}{}", con_type, ip, delimiter, base_port + 1),
    );
    let jsonrpc_subscriber = new_sub_init(
        &CONTEXT,
        format!("{}://{}{}{}", con_type, ip, delimiter, base_port + 2),
    );
    let consensus_subscriber = new_sub_init(
        &CONTEXT,
        format!("{}://{}{}{}", con_type, ip, delimiter, base_port + 3),
    );
    let executor_subscriber = new_sub_init(
        &CONTEXT,
        format!("{}://{}{}{}", con_type, ip, delimiter, base_port + 4),
    );
    let auth_subscriber = new_sub_init(
        &CONTEXT,
        format!("{}://{}{}{}", con_type, ip, delimiter, base_port + 5),
    );
    let snapshot_subscriber = new_sub_init(
        &CONTEXT,
        format!("{}://{}{}{}", con_type, ip, delimiter, base_port + 6),
    );
    let net_tx_subscriber = new_sub_init(
        &CONTEXT,
        format!("{}://{}{}{}", con_type, ip, delimiter, base_port + 7),
    );
    let net_consensus_subscriber = new_sub_init(
        &CONTEXT,
        format!("{}://{}{}{}", con_type, ip, delimiter, base_port + 8),
    );

    let mut flag = 0;
    for topic in keys {
        let tmp = topic.clone();
        let v: Vec<&str> = tmp.split('.').collect();

        flag = match v[0] {
            "net" => match v[1] {
                "raw_bytes" | "compact_signed_proposal" => {
                    net_consensus_subscriber
                        .set_subscribe(&topic.into_bytes())
                        .unwrap();
                    flag | 0x80
                }
                "request" | "get_block_txn" | "block_txn" => {
                    net_tx_subscriber
                        .set_subscribe(&topic.into_bytes())
                        .unwrap();
                    flag | 0x100
                }
                _ => {
                    network_subscriber
                        .set_subscribe(&topic.into_bytes())
                        .unwrap();
                    flag | 0x01
                }
            },
            "chain" => {
                chain_subscriber.set_subscribe(&topic.into_bytes()).unwrap();
                flag | 0x02
            }
            "jsonrpc" => {
                jsonrpc_subscriber
                    .set_subscribe(&topic.into_bytes())
                    .unwrap();
                flag | 0x04
            }
            "consensus" => {
                consensus_subscriber
                    .set_subscribe(&topic.into_bytes())
                    .unwrap();
                flag | 0x08
            }
            "executor" => {
                executor_subscriber
                    .set_subscribe(&topic.into_bytes())
                    .unwrap();
                flag | 0x10
            }
            "auth" => {
                auth_subscriber.set_subscribe(&topic.into_bytes()).unwrap();
                flag | 0x20
            }
            "snapshot" => {
                snapshot_subscriber
                    .set_subscribe(&topic.into_bytes())
                    .unwrap();
                flag | 0x40
            }
            "synchronizer" => flag,
            _ => {
                error!("invalid  flag! topic {}", tmp);
                flag
            }
        }
    }

    if flag & 0x01 != 0 {
        let other_tx = tx.clone();
        let _ = thread::Builder::new()
            .name("network_subscriber".to_string())
            .spawn(move || loop {
                thread_work(&network_subscriber, &other_tx);
            });
    }

    if flag & 0x02 != 0 {
        let other_tx = tx.clone();
        let _ = thread::Builder::new()
            .name("chain_subscriber".to_string())
            .spawn(move || loop {
                thread_work(&chain_subscriber, &other_tx);
            });
    }

    if flag & 0x04 != 0 {
        let other_tx = tx.clone();
        let _ = thread::Builder::new()
            .name("jsonrpc_subscriber".to_string())
            .spawn(move || loop {
                thread_work(&jsonrpc_subscriber, &other_tx);
            });
    }

    if flag & 0x08 != 0 {
        let other_tx = tx.clone();
        let _ = thread::Builder::new()
            .name("consensus_subscriber".to_string())
            .spawn(move || loop {
                thread_work(&consensus_subscriber, &other_tx);
            });
    }

    if flag & 0x10 != 0 {
        let other_tx = tx.clone();
        let _ = thread::Builder::new()
            .name("executor_subscriber".to_string())
            .spawn(move || loop {
                thread_work(&executor_subscriber, &other_tx);
            });
    }

    if flag & 0x20 != 0 {
        let other_tx = tx.clone();
        let _ = thread::Builder::new()
            .name("auth_subscriber".to_string())
            .spawn(move || loop {
                thread_work(&auth_subscriber, &other_tx);
            });
    }

    if flag & 0x40 != 0 {
        let other_tx = tx.clone();
        let _ = thread::Builder::new()
            .name("snapshot_subscriber".to_string())
            .spawn(move || loop {
                thread_work(&snapshot_subscriber, &other_tx);
            });
    }

    if flag & 0x80 != 0 {
        let other_tx = tx.clone();
        let _ = thread::Builder::new()
            .name("net_consensus_subscriber".to_string())
            .spawn(move || loop {
                thread_work(&net_consensus_subscriber, &other_tx);
            });
    }

    if flag & 0x100 != 0 {
        let other_tx = tx.clone();
        let _ = thread::Builder::new()
            .name("net_tx_subscriber".to_string())
            .spawn(move || loop {
                thread_work(&net_tx_subscriber, &other_tx);
            });
    }
}

pub fn start_pubsub<K>(
    name: &str,
    keys: Vec<K>,
    tx: Sender<(String, Vec<u8>)>,
    rx: Receiver<(String, Vec<u8>)>,
) where
    K: Into<String>,
{
    let keys: Vec<String> = keys.into_iter().map(Into::into).collect();
    start_zeromq(name, keys, tx, rx, "localhost".to_string())
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
