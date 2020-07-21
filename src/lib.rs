use cita_logger::{error, warn};
pub use crossbeam_channel as channel;
use crossbeam_channel::{Receiver, Sender};
use lazy_static::lazy_static;
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::thread;

// pub const ZEROMQ_IP: &str = "ZEROMQ_IP";
// pub const ZEROMQ_BASE_PORT: &str = "ZEROMQ_BASE_PORT";

const AMQP_URL: &str = "AMQP_URL";

const NET_SERVICE: &str = "network";
const CHAIN_SERVICE: &str = "chain";
const JSONRPC_SERVICE: &str = "jsonrpc";
const CONSENSUS_SERVICE: &str = "consensus";
const EXECUTOR_SERVICE: &str = "executor";
const AUTH_SERVICE: &str = "auth";
const SNAPSHOT_SERVICE: &str = "snapshot";
const NETAUTH_SERVICE: &str = "network_auth";
const NETCONSENSUS_SERVICE: &str = "network_consensus";

lazy_static! {
    static ref SERVICE_PORT_INDEX: BTreeMap<&'static str, usize> = {
        let mut m = BTreeMap::new();
        m.insert(NET_SERVICE, 0);
        m.insert(CHAIN_SERVICE, 1);
        m.insert(JSONRPC_SERVICE, 2);
        m.insert(CONSENSUS_SERVICE, 3);
        m.insert(EXECUTOR_SERVICE, 4);
        m.insert(AUTH_SERVICE, 5);
        m.insert(SNAPSHOT_SERVICE, 6);
        m.insert(NETAUTH_SERVICE, 7);
        m.insert(NETCONSENSUS_SERVICE, 8);
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

fn spawn_publisher(
    name: Option<&str>,
    bind_url: String,
    rx_then_publish: Receiver<(String, Vec<u8>)>,
) {
    let publisher = CONTEXT.socket(zmq::PUB).unwrap();
    publisher
        .bind(&*bind_url)
        .expect(&*format!("publisher bind error {}", bind_url));

    let _ = thread::Builder::new()
        .name(name.unwrap_or("publisher").to_string())
        .spawn(move || loop {
            let ret = rx_then_publish.recv();

            if ret.is_err() {
                break;
            }
            let (topic, msg) = ret.unwrap();
            let _ = publisher.send_multipart(&[&(topic.into_bytes())], zmq::SNDMORE);
            let _ = publisher.send(&msg, 0);
        });
}

fn get_zmq_url(service_name: &str, base_port: usize) -> Option<String> {
    SERVICE_PORT_INDEX
        .get(service_name)
        .map(|idx| format!("ipc://ipc{}", base_port + idx))
}

fn subscribe_topic(keys: Vec<String>) -> BTreeMap<&'static str, Vec<String>> {
    let mut service_topics: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for topic in keys {
        let tmp = topic.clone();
        let v: Vec<&str> = tmp.split('.').collect();
        match v[0] {
            "net" => match v[1] {
                "raw_bytes" | "compact_signed_proposal" => {
                    service_topics
                        .entry(NETCONSENSUS_SERVICE)
                        .or_default()
                        .push(topic);
                }
                "request" | "get_block_txn" | "block_txn" => {
                    service_topics
                        .entry(NETAUTH_SERVICE)
                        .or_default()
                        .push(topic);
                }
                _ => {
                    service_topics.entry(NET_SERVICE).or_default().push(topic);
                }
            },
            "chain" => {
                service_topics.entry(CHAIN_SERVICE).or_default().push(topic);
            }
            "jsonrpc" => {
                service_topics
                    .entry(JSONRPC_SERVICE)
                    .or_default()
                    .push(topic);
            }
            "consensus" => {
                service_topics
                    .entry(CONSENSUS_SERVICE)
                    .or_default()
                    .push(topic);
            }
            "executor" => {
                service_topics
                    .entry(EXECUTOR_SERVICE)
                    .or_default()
                    .push(topic);
            }
            "auth" => {
                service_topics.entry(AUTH_SERVICE).or_default().push(topic);
            }
            "snapshot" => {
                service_topics
                    .entry(SNAPSHOT_SERVICE)
                    .or_default()
                    .push(topic);
            }
            "synchronizer" => {}
            _ => {
                error!("invalid  flag! topic {}", topic);
            }
        }
    }
    service_topics
}

fn get_base_number() -> usize {
    let mq_url = std::env::var(AMQP_URL).expect(&*format!("{} must be set", AMQP_URL));
    let mut s = DefaultHasher::new();
    mq_url.hash(&mut s);
    (s.finish() % 1_000_000) as usize
}

pub fn start_zeromq(
    name: &str,
    keys: Vec<String>,
    tx: Sender<(String, Vec<u8>)>,
    rx: Receiver<(String, Vec<u8>)>,
) {
    let base_port = get_base_number();
    if let Some(url) = get_zmq_url(name, base_port) {
        spawn_publisher(None, url, rx);
    } else {
        return;
    }

    let serv_topics = subscribe_topic(keys);
    for (ser_name, topics) in serv_topics {
        if let Some(url) = get_zmq_url(ser_name, base_port) {
            let subscriber = new_sub_init(&CONTEXT, url);
            for topic in topics {
                subscriber.set_subscribe(&topic.into_bytes()).unwrap();
            }

            let other_tx = tx.clone();
            let _ = thread::Builder::new()
                .name(ser_name.to_string())
                .spawn(move || loop {
                    thread_work(&subscriber, &other_tx);
                });
        } else {
            warn!("Not recongnize sername {}", ser_name)
        }
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
    start_zeromq(name, keys, tx, rx)
}
