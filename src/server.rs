use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, Duration};

pub type AsyncResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

// Fixed size exponential backoff value
const BACKOFF: u64 = 128;

/// Server listener state. Created in the `run` call. It includes a `run` method
/// which performs the TCP listening and initialization of per-connection state.
struct Server {
    listener: TcpListener,
    /// Tcp exponential backoff threshold
    backoff: u64,
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
        // to a worker
        loop {
            let stream = self.accept().await?;
            // Spawn a new task to process the connections.
            tokio::spawn(async move {
                if let Err(e) = handle_connection(stream).await {
                    panic!("Can't spawn `handle_connection` worker: {}", e);
                };
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

/// Process a single connection.
///
/// First retrieve a valid backend to forward the request to then call `handle_request` method
/// to forward the content to it and read the response back.
///
/// # Errors
///
/// If no backend are available return an `Err`, this can happen if all backends result
/// offline. Also return an `Err` in case of error reading from the selected backend,
/// connection can be broken in the mean-time.
async fn handle_connection(mut stream: TcpStream) -> AsyncResult<()> {
    // TODO
    Ok(())
}

/// Run a tokio async server, accepts and handle new connections asynchronously.
///
/// Arguments are listener, a bound `TcpListener` and pool a `BackendPool` with type
/// `LoadBalancing`
pub async fn run(listener: TcpListener) -> AsyncResult<()> {
    let mut server = Server {
        listener,
        backoff: BACKOFF,
    };
    server.run().await?;
    Ok(())
}
