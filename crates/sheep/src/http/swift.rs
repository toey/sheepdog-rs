//! Swift-compatible API handlers.
//!
//! Provides basic OpenStack Swift operations. Uses the same KvStore
//! as the S3 handlers — Swift containers map to sheepdog VDIs.

#[cfg(feature = "http")]
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
#[cfg(feature = "http")]
use tracing::{debug, info, warn};

#[cfg(feature = "http")]
use sheepdog_proto::request::{SdRequest, ResponseResult};

#[cfg(feature = "http")]
use crate::http::HttpState;
#[cfg(feature = "http")]
use crate::http::kv::KvStore;

/// GET /v1/{account} — List containers.
#[cfg(feature = "http")]
pub async fn list_containers(
    State(state): State<HttpState>,
    Path(account): Path<String>,
) -> impl IntoResponse {
    debug!("Swift list containers: {}", account);
    let buckets = state.kv.list_buckets().await;
    let names: Vec<String> = buckets.iter().map(|b| b.name.clone()).collect();
    (StatusCode::OK, names.join("\n"))
}

/// PUT /v1/{account}/{container} — Create container.
#[cfg(feature = "http")]
pub async fn create_container(
    State(state): State<HttpState>,
    Path((account, container)): Path<(String, String)>,
) -> impl IntoResponse {
    info!("Swift create container: {}/{}", account, container);

    let result = crate::request::exec_local_request(
        state.sys.clone(),
        SdRequest::NewVdi {
            name: container.clone(),
            vdi_size: 4 * 1024 * 1024 * 1024,
            base_vid: 0,
            copies: 0,
            copy_policy: 0,
            store_policy: 0,
            snap_id: 0,
            vdi_type: 0,
        },
    )
    .await;

    match result {
        Ok(ResponseResult::Vdi { vdi_id, .. }) => {
            if let Err(e) = state.kv.create_bucket(&container, vdi_id).await {
                warn!("Swift create container kv error: {}", e);
                return StatusCode::INTERNAL_SERVER_ERROR;
            }
            StatusCode::CREATED
        }
        Ok(_) => StatusCode::CREATED,
        Err(e) => {
            warn!("Swift create container error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// DELETE /v1/{account}/{container} — Delete container.
#[cfg(feature = "http")]
pub async fn delete_container(
    State(state): State<HttpState>,
    Path((account, container)): Path<(String, String)>,
) -> impl IntoResponse {
    info!("Swift delete container: {}/{}", account, container);

    if let Err(_) = state.kv.delete_bucket(&container).await {
        return StatusCode::NOT_FOUND;
    }

    let _ = crate::request::exec_local_request(
        state.sys.clone(),
        SdRequest::DelVdi {
            name: container,
            snap_id: 0,
        },
    )
    .await;

    StatusCode::NO_CONTENT
}

/// GET /v1/{account}/{container} — List objects in container.
#[cfg(feature = "http")]
pub async fn list_objects(
    State(state): State<HttpState>,
    Path((account, container)): Path<(String, String)>,
) -> impl IntoResponse {
    debug!("Swift list objects: {}/{}", account, container);

    if !state.kv.bucket_exists(&container).await {
        return (StatusCode::NOT_FOUND, String::new());
    }

    (StatusCode::OK, String::new())
}

/// PUT /v1/{account}/{container}/{object} — Upload object.
#[cfg(feature = "http")]
pub async fn put_object(
    State(state): State<HttpState>,
    Path((account, container, object)): Path<(String, String, String)>,
    body: Bytes,
) -> impl IntoResponse {
    debug!(
        "Swift put object: {}/{}/{} ({} bytes)",
        account, container, object, body.len()
    );

    let bucket_info = match state.kv.get_bucket(&container).await {
        Ok(b) => b,
        Err(_) => return StatusCode::NOT_FOUND,
    };

    let oid = KvStore::key_to_oid(bucket_info.vid, &object);
    let result = crate::request::exec_local_request(
        state.sys.clone(),
        SdRequest::CreateAndWriteObj {
            oid,
            cow_oid: sheepdog_proto::oid::ObjectId::new(0),
            copies: 0,
            copy_policy: 0,
            offset: 0,
            data: body.to_vec(),
        },
    )
    .await;

    match result {
        Ok(_) => StatusCode::CREATED,
        Err(e) => {
            warn!("Swift put object error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// GET /v1/{account}/{container}/{object} — Download object.
#[cfg(feature = "http")]
pub async fn get_object(
    State(state): State<HttpState>,
    Path((account, container, object)): Path<(String, String, String)>,
) -> impl IntoResponse {
    debug!("Swift get object: {}/{}/{}", account, container, object);

    let bucket_info = match state.kv.get_bucket(&container).await {
        Ok(b) => b,
        Err(_) => return (StatusCode::NOT_FOUND, Vec::<u8>::new()),
    };

    let oid = KvStore::key_to_oid(bucket_info.vid, &object);
    let result = crate::request::exec_local_request(
        state.sys.clone(),
        SdRequest::ReadObj {
            oid,
            offset: 0,
            length: 0,
        },
    )
    .await;

    match result {
        Ok(ResponseResult::Data(data)) => (StatusCode::OK, data),
        _ => (StatusCode::NOT_FOUND, Vec::new()),
    }
}

/// DELETE /v1/{account}/{container}/{object} — Delete object.
#[cfg(feature = "http")]
pub async fn delete_object(
    State(state): State<HttpState>,
    Path((account, container, object)): Path<(String, String, String)>,
) -> impl IntoResponse {
    debug!("Swift delete object: {}/{}/{}", account, container, object);

    let bucket_info = match state.kv.get_bucket(&container).await {
        Ok(b) => b,
        Err(_) => return StatusCode::NOT_FOUND,
    };

    let oid = KvStore::key_to_oid(bucket_info.vid, &object);
    let _ = crate::request::exec_local_request(
        state.sys.clone(),
        SdRequest::RemoveObj { oid },
    )
    .await;

    StatusCode::NO_CONTENT
}
