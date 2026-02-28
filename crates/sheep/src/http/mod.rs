//! HTTP/S3 server — replaces the C version's FCGI-based implementation.
//!
//! Uses axum for a native HTTP server providing:
//! - S3-compatible API (put/get/delete objects in buckets)
//! - Swift-compatible API (basic container/object ops)
//! - Health check endpoint
//!
//! Enabled by default via the `http` Cargo feature.

#[cfg(feature = "http")]
pub mod kv;
#[cfg(feature = "http")]
pub mod oalloc;
#[cfg(feature = "http")]
pub mod s3;
#[cfg(feature = "http")]
pub mod swift;

#[cfg(feature = "http")]
use std::net::SocketAddr;
#[cfg(feature = "http")]
use std::sync::Arc;

#[cfg(feature = "http")]
use axum::{
    Router,
    routing::{delete, get, head, put},
    extract::State,
    response::IntoResponse,
    http::StatusCode,
};
#[cfg(feature = "http")]
use tracing::info;

#[cfg(feature = "http")]
use crate::daemon::SharedSys;

/// Application state shared across HTTP handlers.
#[cfg(feature = "http")]
#[derive(Clone)]
pub struct HttpState {
    pub sys: SharedSys,
    pub kv: Arc<kv::KvStore>,
}

/// Start the HTTP/S3 server.
#[cfg(feature = "http")]
pub async fn start_http_server(sys: SharedSys, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let state = HttpState {
        sys,
        kv: Arc::new(kv::KvStore::new()),
    };

    let app = Router::new()
        // Health check
        .route("/", get(health_check))
        // S3-compatible routes
        .route("/{bucket}", get(s3::list_objects).put(s3::create_bucket).delete(s3::delete_bucket).head(s3::head_bucket))
        .route("/{bucket}/{key}", get(s3::get_object).put(s3::put_object).delete(s3::delete_object).head(s3::head_object))
        // Swift-compatible routes (on /v1/ prefix)
        .route("/v1/{account}", get(swift::list_containers))
        .route("/v1/{account}/{container}", get(swift::list_objects).put(swift::create_container).delete(swift::delete_container))
        .route("/v1/{account}/{container}/{object}", get(swift::get_object).put(swift::put_object).delete(swift::delete_object))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("HTTP/S3 server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

/// Health check endpoint.
#[cfg(feature = "http")]
async fn health_check(State(state): State<HttpState>) -> impl IntoResponse {
    let s = state.sys.read().await;
    if s.is_cluster_ok() {
        (StatusCode::OK, "OK")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "Cluster not ready")
    }
}
