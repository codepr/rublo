use log::info;
use rublo::server;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> rublo::AsyncResult<()> {
    rublo::init_logging().expect("Can't enable logging");
    let listener = TcpListener::bind("127.0.0.1:4989".to_string()).await?;
    info!("listening on ::4989");
    server::run(listener).await
}
