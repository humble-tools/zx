// start and manage redis processes for segments
use crate::Column;
use anyhow::Result;
use maplit::hashmap;
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};

use lazy_static::lazy_static;
use std::sync::Mutex;

lazy_static! {
    pub static ref REGISTRY: Mutex<Registry> = Mutex::new(Registry::default());
}

pub struct Registry {
    pub sockets: HashMap<Column, PathBuf>,
    pub childs: Vec<Child>,
    counter: i64,
}

impl Default for Registry {
    fn default() -> Self {
        Registry {
            sockets: hashmap!(),
            childs: vec![],
            counter: 0,
        }
    }
}

impl Registry {
    pub fn start_col_server(&mut self, col: &Column) -> Result<PathBuf> {
        dbg!(col);
        if let Some(pb) = self.sockets.get(col) {
            return Ok(pb.clone());
        }

        let socket = format!("/tmp/zx-{}.sock", self.counter);
        let db = format!("{}.rdb", col);
        self.counter += 1;
        let child = Command::new("redis-server")
            .arg(format!("--unixsocket {}", socket))
            .arg(format!("--unixsocketperm 777"))
            .arg(format!("--port {}", self.counter + 6379))
            .arg(format!("--dbfilename {}", db))
            .stdout(Stdio::piped())
            .spawn()?;

        self.sockets
            .insert(col.clone(), PathBuf::from(socket.clone()));

        self.childs.push(child);

        wait_server_ready(socket.clone());

        Ok(PathBuf::from(socket))
    }
}

use std::thread;

fn wait_server_ready(socket: String) {
    let mut c = 0;
    loop {
        let ret = Command::new("redis-cli")
            .arg("-s")
            .arg(socket.clone())
            .arg("PING")
            .output()
            .expect("failed to run redis-cli");

        if ret.status.success() && String::from_utf8_lossy(&ret.stdout) == "PONG\n" {
            return ();
        } else {
            if c > 50 {
                panic!("unable to start redis-cli");
            }
            c += 1;
            thread::sleep(std::time::Duration::from_millis(10))
        }
    }
}
