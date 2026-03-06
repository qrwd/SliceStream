use axum::{routing::get, Router};

#[tokio::main]
async fn main() {
    let app = Router::new().route("/", get(|| async { "providerd placeholder" }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:4001").await.unwrap();
    println!("providerd listening on http://127.0.0.1:4001");
    axum::serve(listener, app).await.unwrap();
}
