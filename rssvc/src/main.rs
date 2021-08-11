use axum::prelude::*;
use eyre::WrapErr;
use std::net::SocketAddr;
use tower::ServiceBuilder;
use tower_http::{compression::CompressionLayer, trace::TraceLayer};

mod trace_layer;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt::init();
    let addr = SocketAddr::from(([0; 4], 5000));
    let middleware = ServiceBuilder::new()
        .timeout(std::time::Duration::from_secs(10))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(trace_layer::MakeSpan)
                .on_request(trace_layer::OnRequest)
                .on_response(trace_layer::OnResponse),
        )
        .layer(CompressionLayer::new())
        .into_inner();
    let app = route("/", get(|| async { "Hello, world!\n" }))
        .route("/healthcheck", get(|| async {}))
        .layer(middleware);
    let server = hyper::Server::bind(&addr)
        .serve(app.into_make_service())
        .with_graceful_shutdown(shutdown_handler());
    tracing::info!("server started listening on {}", addr);
    server.await.wrap_err("server failed")
}

async fn shutdown_handler() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install signal handler")
}
