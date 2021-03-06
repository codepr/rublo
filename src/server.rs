use crate::filter::{ScalableBloomFilter, ScaleFactor, DEFAULT_DATA_DIR};
use crate::AsyncResult;
use chrono::{DateTime, NaiveDateTime, Utc};
use futures::SinkExt;
use log::{error, info};
use std::fmt;
use std::ops::DerefMut;
use std::path::Path;
use std::result::Result;
use std::sync::Arc;
use std::{collections::HashMap, collections::HashSet, net::SocketAddr};
use tokio::fs;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

// Fixed size exponential backoff value
const BACKOFF: u64 = 128;
// Dump to disk seconds interval
const DUMP_INTERVAL: u64 = 60;
// Interval to check for cold filters
const DUMP_COLD_INTERVAL: u64 = 5;
// Default timeout to declare a filter cold in seconds
const COLD_FILTER_TIMEOUT: i64 = 3600;
// Base capacity for each new filter, if not specified
const DEFAULT_CAPACITY: &str = "50000";
// Base false positive probability for each new filter, if not specified otherwise
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

/// Text protocol declaration, currently supports basic commands such as:
/// - Create filter-name [capacity] [fpp]
/// - Set filter-name key
/// - Check filter-name key
/// - Info filter-name
/// - Drop filter-name
/// - Clear filter-name
/// - Persist filter-name
/// - List
#[derive(Debug, PartialEq)]
enum Request {
    Create {
        name: String,
        capacity: usize,
        fpp: f64,
    },
    Set {
        name: String,
        key: String,
    },
    Check {
        name: String,
        key: String,
    },
    Info {
        name: String,
    },
    Drop {
        name: String,
    },
    Clear {
        name: String,
    },
    Persist {
        name: String,
    },
    List,
}

struct FilterProps {
    pub name: String,
    pub fpp: f64,
    pub capacity: usize,
}

enum Response {
    Done,
    True,
    False,
    Info {
        name: String,
        capacity: usize,
        size: usize,
        space: String,
        filters: u32,
        hash_count: u32,
        hits: u64,
        miss: u64,
        creation_time: String,
        last_access_time: String,
    },
    Error(String),
    List {
        filters: Vec<FilterProps>,
    },
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
                Ok(Request::Create {
                    name,
                    capacity,
                    fpp,
                })
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
                Ok(Request::Set { name, key })
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
                Ok(Request::Check { name, key })
            }
            Some(c) if c == "info" => {
                let name = token
                    .next()
                    .ok_or(ParserError {
                        message: "missing filter name".into(),
                    })
                    .map(|s| s.to_string())?;
                Ok(Request::Info { name })
            }
            Some(c) if c == "drop" => {
                let name = token
                    .next()
                    .ok_or(ParserError {
                        message: "missing filter name".into(),
                    })
                    .map(|s| s.to_string())?;
                Ok(Request::Drop { name })
            }
            Some(c) if c == "clear" => {
                let name = token
                    .next()
                    .ok_or(ParserError {
                        message: "missing filter name".into(),
                    })
                    .map(|s| s.to_string())?;
                Ok(Request::Clear { name })
            }
            Some(c) if c == "persist" => {
                let name = token
                    .next()
                    .ok_or(ParserError {
                        message: "missing filter name".into(),
                    })
                    .map(|s| s.to_string())?;
                Ok(Request::Persist { name })
            }
            Some(c) if c == "list" => Ok(Request::List),
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
            Response::Info {
                name,
                capacity,
                size,
                space,
                filters,
                hash_count,
                hits,
                miss,
                creation_time,
                last_access_time,
            } => format!(
                "name: {}\ncapacity: {}\nsize: {}\nspace: {}\nfilters: {}\nhash functions: {}\nhits: {}\nmiss: {}\ncreation: {}\nlast access: {}",
                name, capacity, size, space, filters, hash_count, hits, miss, creation_time, last_access_time
            ),
            Response::List { filters } => {
                let tostr: Vec<String> = filters.iter().map(|x| format!("{} {} {}", x.name, x.capacity, x.fpp)).collect();
                tostr.join("\n")
            }
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
            Request::Create {
                name: "foo".into(),
                capacity: 5,
                fpp: 0.01
            }
        );
        assert_eq!(
            Request::parse("create foo")?,
            Request::Create {
                name: "foo".into(),
                capacity: 50000,
                fpp: 0.05
            }
        );
        assert_eq!(
            Request::parse("check foo bar")?,
            Request::Check {
                name: "foo".into(),
                key: "bar".into()
            }
        );
        assert_eq!(
            Request::parse("set foo bar")?,
            Request::Set {
                name: "foo".into(),
                key: "bar".into()
            }
        );
        assert_eq!(
            Request::parse("drop foo")?,
            Request::Drop { name: "foo".into() }
        );
        let r = Request::parse("create foo bar 0.01").map_err(|e| e);
        assert!(r.is_err());
        Ok(())
    }
}

struct FilterDatabase {
    pub filters: HashMap<String, ScalableBloomFilter>,
    pub cold_filters: HashSet<String>,
}

/// Shared state between multiple connections, the filter manager to track and
/// update multiple scalable filters.
///
/// Being shared it's wrapped as an atomic counter reference (Arc) guarded by a mutex.
type FilterDb = Arc<Mutex<FilterDatabase>>;

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
    /// Init the shared database object by reading the disk at the default path for filters stored
    /// and put them into memory.
    ///
    /// # Errors
    ///
    /// Returns `Err` if anything wrong happens while reading from disk and deserializing memory
    /// maps into memory.
    pub async fn init(&mut self) -> AsyncResult<()> {
        let mut db = self.db.lock().await;
        let mut entries = fs::read_dir(DEFAULT_DATA_DIR).await?;
        info!("scanning {}/ for persistent filters", DEFAULT_DATA_DIR);
        while let Some(entry) = entries.next_entry().await? {
            if let Ok(path) = entry.path().into_os_string().into_string() {
                let filter = ScalableBloomFilter::from_file(&path).await?;
                info!("found persistent filter {}", filter);
                db.filters.insert(filter.name().clone(), filter);
            }
        }
        Ok(())
    }

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
        // Create a clone reference of the filters database to be used by the dump worker
        let db = self.db.clone();
        // Spawn a new task to dump every filter to disk
        tokio::spawn(async move {
            if let Err(e) = dump_to_disk(&db, DUMP_INTERVAL).await {
                error!("Can't spawn `dump_to_disk` worker: {:?}", e);
            }
        });
        let db = self.db.clone();
        // And another one to keep only warm filters in memory
        tokio::spawn(async move {
            if let Err(e) = dump_cold_filters(&db, DUMP_COLD_INTERVAL).await {
                error!("Can't spawn `dump_cold_filters` worker: {:?}", e);
            }
        });
        // Loop forever on new connections, accept them and pass the handling
        // to a worker.
        loop {
            // Accepts a new connection, obtaining a valid socket.
            let (stream, peer) = self.accept().await?;
            info!("connection from {}", peer.to_string());
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
                            let response = handle_request(&line, &db).await;
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
                info!("connection closed by client");
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
    async fn accept(&mut self) -> AsyncResult<(TcpStream, SocketAddr)> {
        let mut backoff = 1;

        // Try to accept a few times
        loop {
            // Perform the accept operation. If a socket is successfully
            // accepted, return it. Otherwise, save the error.
            match self.listener.accept().await {
                Ok((socket, peer)) => return Ok((socket, peer)),
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

/// Write to disk every scalable filter in the database every `interval` seconds, meant to run
/// as a tokio task
async fn dump_to_disk(db: &FilterDb, interval: u64) -> AsyncResult<()> {
    loop {
        // Sleep for a defined timeout
        sleep(Duration::from_secs(interval)).await;
        let db = db.lock().await;
        for (_, v) in db.filters.iter() {
            match v.to_file().await {
                Ok(()) => info!("{} filter dumped to disk", v),
                Err(e) => error!("{} filter dump error: {:?}", v, e),
            }
        }
        drop(db);
    }
}

/// Write to disk every scalable filter in the database that is considered cold. Cold filters are
/// those that are not accessed since a given time.
async fn dump_cold_filters(db: &FilterDb, interval: u64) -> AsyncResult<()> {
    loop {
        let mut db_ref = db.lock().await;
        let dbr = db_ref.deref_mut();
        let now = Utc::now().timestamp();
        for (k, v) in dbr.filters.iter() {
            if now - &v.last_access_time().timestamp() > COLD_FILTER_TIMEOUT {
                match v.to_file().await {
                    Ok(()) => {
                        dbr.cold_filters.insert(k.clone());
                        info!(
                            "{} filter dumped to disk as deemed cold - last access time {}",
                            v,
                            v.last_access_time()
                        )
                    }
                    Err(e) => error!("{} filter dump error: {:?}", v, e),
                }
            }
        }
        db_ref
            .filters
            .retain(|_, v| now - v.last_access_time().timestamp() < COLD_FILTER_TIMEOUT);
        drop(db_ref);
        // Sleep for a defined timeout
        sleep(Duration::from_secs(interval)).await;
    }
}

/// Parse a line into a `Request` and return a `Response` based on the outcome of the
/// operation requested.
async fn handle_request(line: &str, db: &FilterDb) -> Response {
    let request = match Request::parse(&line) {
        Ok(req) => req,
        Err(e) => return Response::Error(e.message),
    };
    let mut db = db.lock().await;
    let db_ref = db.deref_mut();
    match request {
        Request::Create {
            name,
            capacity,
            fpp,
        } => {
            db.filters
                .entry(name.clone())
                .or_insert(ScalableBloomFilter::new(
                    name,
                    capacity,
                    fpp,
                    ScaleFactor::SmallScaleSize,
                ));
            Response::Done
        }
        Request::Set { name, key } => match db_ref.filters.get_mut(&name) {
            // First we check that a warm filter matching the name exists and in case, try to set
            // the value
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
            // No warm filter found matching the name given, let's check for any cold fitler stored
            // on disk, if present, pull it back to memory for faster access, marking it as warm
            // again, then try to set the value
            None => match db_ref.cold_filters.get(&name) {
                Some(fname) => {
                    let path = Path::new(DEFAULT_DATA_DIR).join(format!("{}.rbl", fname));
                    let filter = ScalableBloomFilter::from_file(&path.to_str().unwrap()).await;
                    info!("pulling cold filter {} back to memory", fname);
                    match filter {
                        Ok(mut f) => {
                            let outcome = if let Err(e) = f.set(key.as_bytes()) {
                                Response::Error(format!(
                                    "set \"{}\" into \"{}\" filter failed: {:?}",
                                    key, name, e
                                ))
                            } else {
                                Response::Done
                            };
                            // We want to re-insert the filter into the shared database and remove
                            // it from the cold filters atomically
                            db_ref.filters.insert(f.name().clone(), f);
                            db_ref.cold_filters.remove(&name);
                            outcome
                        }
                        Err(e) => Response::Error(format!(
                            "error recovering cold filter named {}: {:?}",
                            name, e
                        )),
                    }
                }
                None => Response::Error(format!("no scalable filter named {}", name)),
            },
        },
        Request::Check { name, key } => match db_ref.filters.get_mut(&name) {
            // For check operation, the process is analogous to the Set command, we check that a
            // warm filter matching the name exists and in case, try to check the value
            Some(sbf) => {
                if sbf.check(key.as_bytes()) {
                    Response::True
                } else {
                    Response::False
                }
            }
            // No warm filter found matching the name given, let's check for any cold fitler stored
            // on disk, if present, pull it back to memory for faster access, marking it as warm
            // again, then try to check the value
            None => match db_ref.cold_filters.get(&name) {
                Some(fname) => {
                    let path = Path::new(DEFAULT_DATA_DIR).join(format!("{}.rbl", fname));
                    let filter = ScalableBloomFilter::from_file(&path.to_str().unwrap()).await;
                    info!("pulling cold filter {} back to memory", fname);
                    match filter {
                        Ok(mut f) => {
                            let outcome = if f.check(key.as_bytes()) {
                                Response::True
                            } else {
                                Response::False
                            };
                            // We want to re-insert the filter into the shared database and remove
                            // it from the cold filters atomically
                            db_ref.filters.insert(f.name().clone(), f);
                            db_ref.cold_filters.remove(&name);
                            outcome
                        }
                        Err(e) => Response::Error(format!(
                            "error recovering cold filter named {}: {:?}",
                            name, e
                        )),
                    }
                }
                None => Response::Error(format!("no scalable filter named {}", name)),
            },
        },
        Request::Info { name } => match db.filters.get(&name) {
            // Same for info operation, the process is analogous to the Set command, we check that
            // a warm filter matching the name exists and in case, try to retrieve info from the
            // filter
            Some(sbf) => get_filter_info(&sbf),
            // No warm filter found matching the name given, let's check for any cold fitler stored
            // on disk, if present, pull it back to memory for faster access, but without making it
            // warm again, we don't count info call as actually active operation for a filter
            None => match db.cold_filters.get(&name) {
                Some(fname) => {
                    let path = Path::new(DEFAULT_DATA_DIR).join(format!("{}.rbl", fname));
                    let filter = ScalableBloomFilter::from_file(&path.to_str().unwrap()).await;
                    match filter {
                        Ok(f) => get_filter_info(&f),
                        Err(e) => Response::Error(format!(
                            "error recovering cold scalable filter named {}: {:?}",
                            name, e
                        )),
                    }
                }
                None => Response::Error(format!("no scalable filter named {}", name)),
            },
        },
        Request::Drop { name } => match db.filters.remove(&name) {
            Some(_) => Response::Done,
            None => Response::Error(format!("no scalable filter named {}", name)),
        },
        Request::Clear { name } => match db.filters.get_mut(&name) {
            Some(sbf) => {
                sbf.clear();
                Response::Done
            }
            None => Response::Error(format!("no scalable filter named {}", name)),
        },
        Request::Persist { name } => match db.filters.get(&name) {
            Some(sbf) => match sbf.to_file().await {
                Ok(()) => Response::Done,
                Err(e) => Response::Error(format!("persist failed {}", e)),
            },
            None => Response::Error(format!("no scalable filter named {}", name)),
        },
        Request::List => {
            let filters = db
                .filters
                .iter()
                .map(|(_, v)| FilterProps {
                    name: v.name().clone(),
                    fpp: v.fpp(),
                    capacity: v.capacity(),
                })
                .collect();
            Response::List { filters }
        }
    }
}

// Read filter info and format them into a `Response::Info`
fn get_filter_info(f: &ScalableBloomFilter) -> Response {
    let sec = f.creation_time().timestamp();
    let lat = f.last_access_time().timestamp();
    Response::Info {
        name: f.name().clone(),
        capacity: f.capacity(),
        size: f.size(),
        space: format!("{}", f.byte_space()),
        filters: f.filter_count() as u32,
        hash_count: f.hash_count(),
        hits: f.hits(),
        miss: f.miss(),
        creation_time: DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(sec, 0), Utc)
            .to_rfc3339(),
        last_access_time: DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(lat, 0), Utc)
            .to_rfc3339(),
    }
}

/// Run a tokio async server, init the shared filters database and accepts and handle new
/// connections asynchronously.
///
/// Requires single, already bound `TcpListener` argument
pub async fn run(listener: TcpListener) -> AsyncResult<()> {
    fs::create_dir_all(DEFAULT_DATA_DIR).await?;
    let filter_db = Arc::new(Mutex::new(FilterDatabase {
        filters: HashMap::new(),
        cold_filters: HashSet::new(),
    }));
    let mut server = Server {
        listener,
        backoff: BACKOFF,
        db: filter_db,
    };
    server.init().await?;
    server.run().await
}
