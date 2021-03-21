// use crate::scalable_filter::ScalableBloomFilter;
use crate::filter::{ScalableBloomFilter, ScaleFactor};
use crate::AsyncResult;
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::SinkExt;
use std::collections::HashMap;
use std::fmt;
use std::result::Result;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, Duration};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

// Fixed size exponential backoff value
const BACKOFF: u64 = 128;
const DEFAULT_CAPACITY: &str = "50000";
const DEFAULT_FPP: &str = "0.05";

#[derive(Debug, Clone)]
struct ParserError {
    message: String,
}

impl fmt::Display for ParserError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "parser error: {}", self.message)
    }
}

#[derive(Debug, PartialEq)]
enum Request {
    Create(String, usize, f64),
    Set(String, String),
    Check(String, String),
    Info(String),
    Drop(String),
}

enum Response {
    Done,
    True,
    False,
    Info(String, usize, usize, String, String),
    Error(String),
}

impl Request {
    fn parse(line: &str) -> Result<Request, ParserError> {
        let mut token = line.split(" ");
        let cmd = match token.next() {
            Some(c) if c == "create" => {
                let name = token
                    .next()
                    .ok_or(ParserError {
                        message: "missing name".into(),
                    })
                    .map(|s| s.to_string())?;
                let capacity = token
                    .next()
                    .or(Some(DEFAULT_CAPACITY))
                    .map(|s| {
                        s.parse::<usize>().map_err(|_| ParserError {
                            message: "capacity must be an i64 value".into(),
                        })
                    })
                    .unwrap()?;
                let fpp = token
                    .next()
                    .or(Some(DEFAULT_FPP))
                    .map(|s| {
                        s.parse::<f64>().map_err(|_| ParserError {
                            message: "false-positive probability must be a f64 value".into(),
                        })
                    })
                    .unwrap()?;
                Ok(Request::Create(name, capacity, fpp))
            }
            Some(c) if c == "set" => {
                let name = token
                    .next()
                    .ok_or(ParserError {
                        message: "missing name".into(),
                    })
                    .map(|s| s.to_string())?;
                let key = token
                    .next()
                    .ok_or(ParserError {
                        message: "missing key".into(),
                    })
                    .map(|s| s.to_string())?;
                Ok(Request::Set(name, key))
            }
            Some(c) if c == "check" => {
                let name = token
                    .next()
                    .ok_or(ParserError {
                        message: "missing name".into(),
                    })
                    .map(|s| s.to_string())?;
                let key = token
                    .next()
                    .ok_or(ParserError {
                        message: "missing key".into(),
                    })
                    .map(|s| s.to_string())?;
                Ok(Request::Check(name, key))
            }
            Some(c) if c == "info" => {
                let name = token
                    .next()
                    .ok_or(ParserError {
                        message: "missing filter name".into(),
                    })
                    .map(|s| s.to_string())?;
                Ok(Request::Info(name))
            }
            Some(c) if c == "drop" => {
                let name = token
                    .next()
                    .ok_or(ParserError {
                        message: "missing filter name".into(),
                    })
                    .map(|s| s.to_string())?;
                Ok(Request::Drop(name))
            }
            Some(_) => Err(ParserError {
                message: "unknown command".into(),
            }),
            None => Err(ParserError {
                message: "missing command".into(),
            }),
        };
        cmd
    }
}

impl Response {
    fn serialize(&self) -> String {
        match &*self {
            Response::Done => "Done".into(),
            Response::True => "True".into(),
            Response::False => "False".into(),
            Response::Info(name, capacity, size, space, dt) => format!(
                "{} capacity: {} size: {} space: {} creation: {}",
                name, capacity, size, space, dt
            ),
            Response::Error(message) => format!("Error: {}", message),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ParserError, Request};

    #[test]
    fn test_parse() -> Result<(), ParserError> {
        assert_eq!(
            Request::parse("create foo 5 0.01")?,
            Request::Create("foo".into(), 5, 0.01)
        );
        assert_eq!(
            Request::parse("check foo bar")?,
            Request::Check("foo".into(), "bar".into())
        );
        assert_eq!(
            Request::parse("set foo bar")?,
            Request::Set("foo".into(), "bar".into())
        );
        let r = Request::parse("create foo bar 0.01").map_err(|e| e);
        assert!(r.is_err());
        Ok(())
    }
}

/// Shared state between multiple connections, the filter manager to track and
/// update multiple scalable filters.
///
/// Being shared it's wrapped as an atomic counter reference (Arc) guarded by a mutex.
type FilterDb = Arc<Mutex<HashMap<String, ScalableBloomFilter>>>;

/// Server listener state. Created in the `run` call. It includes a `run` method
/// which performs the TCP listening and initialization of per-connection state.
struct Server {
    listener: TcpListener,
    /// Tcp exponential backoff threshold
    backoff: u64,
    /// Filter manager map
    db: FilterDb,
}

impl Server {
    /// Create a new Server and run.
    ///
    /// Listen for inbound connections. For each inbound connection, spawn a
    /// task to process that connection.
    ///
    /// # Errors
    ///
    /// Returns `Err` if accepting returns an error. This can happen for a
    /// number reasons that resolve over time. For example, if the underlying
    /// operating system has reached an internal limit for max number of
    /// sockets, accept will fail.
    pub async fn run(&mut self) -> AsyncResult<()> {
        // Loop forever on new connections, accept them and pass the handling
        // to a worker.
        loop {
            // Accepts a new connection, obtaining a valid socket.
            let stream = self.accept().await?;
            // Create a clone reference of the filters database to be used by this connection.
            let db = self.db.clone();
            // Spawn a new task to process the connection, moving the ownership of the cloned
            // db into the async closure.
            tokio::spawn(async move {
                // The protocol is line-based, `LinesCodec` is useful to automatically handle
                // this by converting the stream of bytes into a stream of lines.
                let mut lines = Framed::new(stream, LinesCodec::new());
                // Parse each line returned by the codec and by leveraging `LinesCodec` once again
                // send a response back to the client.
                while let Some(result) = lines.next().await {
                    match result {
                        Ok(line) => {
                            let response = handle_request(&line, &db);
                            let response = response.serialize();
                            if let Err(e) = lines.send(response.as_str()).await {
                                println!("error sending response: {:?}", e);
                            }
                        }
                        Err(e) => {
                            println!("error on deconding from stream: {:?}", e);
                        }
                    }
                }
            });
        }
    }

    /// Accept an inbound connection.
    ///
    /// Errors are handled by backing off and retrying. An exponential backoff
    /// strategy is used. After the first failure, the task waits for 1 second.
    /// After the second failure, the task waits for 2 seconds. Each subsequent
    /// failure doubles the wait time. If accepting fails on the 6th try after
    /// waiting for 64 seconds, then this function returns with an error.
    async fn accept(&mut self) -> AsyncResult<TcpStream> {
        let mut backoff = 1;

        // Try to accept a few times
        loop {
            // Perform the accept operation. If a socket is successfully
            // accepted, return it. Otherwise, save the error.
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > self.backoff {
                        // Accept has failed too many times. Return the error.
                        return Err(err.into());
                    }
                }
            }

            // Pause execution until the back off period elapses.
            sleep(Duration::from_secs(backoff)).await;

            // Double the back off
            backoff *= 2;
        }
    }
}

/// Parse a line into a `Request` and return a `Response` based on the outcome of the
/// operation requested.
fn handle_request(line: &str, db: &FilterDb) -> Response {
    let request = match Request::parse(&line) {
        Ok(req) => req,
        Err(e) => return Response::Error(e.message),
    };
    let mut db = db.lock().unwrap();
    match request {
        Request::Create(name, capacity, fpp) => {
            db.entry(name.clone()).or_insert(ScalableBloomFilter::new(
                name,
                capacity,
                fpp,
                ScaleFactor::SmallScaleSize,
            ));
            Response::Done
        }
        Request::Set(name, key) => match db.get_mut(&name) {
            Some(sbf) => {
                if let Err(e) = sbf.set(key.as_bytes()) {
                    Response::Error(format!(
                        "set \"{}\" into \"{}\" filter failed: {:?}",
                        key, name, e
                    ))
                } else {
                    Response::Done
                }
            }
            None => Response::Error(format!("no scalable filter named {}", name)),
        },
        Request::Check(name, key) => match db.get_mut(&name) {
            Some(sbf) => {
                if sbf.check(key.as_bytes()) {
                    Response::True
                } else {
                    Response::False
                }
            }
            None => Response::Error(format!("no scalable filter named {}", name)),
        },
        Request::Info(name) => match db.get(&name) {
            Some(sbf) => {
                let sec = sbf.creation_time().timestamp();
                Response::Info(
                    name,
                    sbf.capacity(),
                    sbf.size(),
                    format!("{}", sbf.byte_space()),
                    DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(sec, 0), Utc)
                        .to_rfc3339(),
                )
            }
            None => Response::Error(format!("no scalable filter named {}", name)),
        },
        Request::Drop(name) => match db.remove(&name) {
            Some(_) => Response::Done,
            None => Response::Error(format!("no scalable filter named {}", name)),
        },
    }
}

/// Run a tokio async server, init the shared filters database and accepts and handle new
/// connections asynchronously.
///
/// Requires single, already bound `TcpListener` argument
pub async fn run(listener: TcpListener) -> AsyncResult<()> {
    let mut server = Server {
        listener,
        backoff: BACKOFF,
        db: Arc::new(Mutex::new(HashMap::new())),
    };
    server.run().await?;
    Ok(())
}
