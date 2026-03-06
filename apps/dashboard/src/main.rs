use axum::{response::Html, routing::get, Router};

#[tokio::main]
async fn main() {
    let app = Router::new().route(
        "/",
        get(|| async {
            Html("<html><body><h1>dashboard placeholder</h1><p>SSR template placeholder</p></body></html>")
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:4003").await.unwrap();
    println!("dashboard listening on http://127.0.0.1:4003");
    axum::serve(listener, app).await.unwrap();
}
