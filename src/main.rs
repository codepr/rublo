use rublo::server;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> rublo::AsyncResult<()> {
    let listener = TcpListener::bind("127.0.0.1:4989".to_string()).await?;
    println!("Listening on ::4989");
    server::run(listener).await?;
    Ok(())
}
