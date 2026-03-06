use axum::{routing::get, Router};

#[tokio::main]
async fn main() {
    let app = Router::new().route("/", get(|| async { "agentd placeholder" }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:4002").await.unwrap();
    println!("agentd listening on http://127.0.0.1:4002");
    axum::serve(listener, app).await.unwrap();
}
