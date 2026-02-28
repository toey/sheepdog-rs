//! S3-compatible API handlers.
//!
//! Provides basic S3 operations: create/delete bucket, put/get/delete object.

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

/// PUT /{bucket} — Create a bucket.
#[cfg(feature = "http")]
pub async fn create_bucket(
    State(state): State<HttpState>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    info!("S3 create bucket: {}", bucket);

    // Create a VDI for this bucket
    let result = crate::request::exec_local_request(
        state.sys.clone(),
        SdRequest::NewVdi {
            name: bucket.clone(),
            vdi_size: 4 * 1024 * 1024 * 1024, // 4GB default
            base_vid: 0,
            copies: 0, // use cluster default
            copy_policy: 0,
            store_policy: 0,
            snap_id: 0,
            vdi_type: 0,
        },
    )
    .await;

    match result {
        Ok(ResponseResult::Vdi { vdi_id, .. }) => {
            if let Err(e) = state.kv.create_bucket(&bucket, vdi_id).await {
                warn!("S3 create bucket kv error: {}", e);
                return StatusCode::INTERNAL_SERVER_ERROR;
            }
            StatusCode::OK
        }
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// DELETE /{bucket} — Delete a bucket.
#[cfg(feature = "http")]
pub async fn delete_bucket(
    State(state): State<HttpState>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    info!("S3 delete bucket: {}", bucket);

    if let Err(_) = state.kv.delete_bucket(&bucket).await {
        return StatusCode::NOT_FOUND;
    }

    let _ = crate::request::exec_local_request(
        state.sys.clone(),
        SdRequest::DelVdi {
            name: bucket,
            snap_id: 0,
        },
    )
    .await;

    StatusCode::NO_CONTENT
}

/// GET /{bucket} — List objects in a bucket.
#[cfg(feature = "http")]
pub async fn list_objects(
    State(state): State<HttpState>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    debug!("S3 list objects: {}", bucket);

    if !state.kv.bucket_exists(&bucket).await {
        return (StatusCode::NOT_FOUND, String::from("<Error><Code>NoSuchBucket</Code></Error>"));
    }

    // Return empty listing (objects are stored as data objects within VDI)
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
        <ListBucketResult><Name>{}</Name><MaxKeys>1000</MaxKeys>\
        <IsTruncated>false</IsTruncated></ListBucketResult>",
        bucket
    );
    (StatusCode::OK, xml)
}

/// HEAD /{bucket} — Check if bucket exists.
#[cfg(feature = "http")]
pub async fn head_bucket(
    State(state): State<HttpState>,
    Path(bucket): Path<String>,
) -> impl IntoResponse {
    debug!("S3 head bucket: {}", bucket);
    if state.kv.bucket_exists(&bucket).await {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

/// PUT /{bucket}/{key} — Upload an object.
#[cfg(feature = "http")]
pub async fn put_object(
    State(state): State<HttpState>,
    Path((bucket, key)): Path<(String, String)>,
    body: Bytes,
) -> impl IntoResponse {
    debug!("S3 put object: {}/{} ({} bytes)", bucket, key, body.len());

    let bucket_info = match state.kv.get_bucket(&bucket).await {
        Ok(b) => b,
        Err(_) => return StatusCode::NOT_FOUND,
    };

    let oid = KvStore::key_to_oid(bucket_info.vid, &key);
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
        Ok(_) => StatusCode::OK,
        Err(e) => {
            warn!("S3 put object error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// GET /{bucket}/{key} — Download an object.
#[cfg(feature = "http")]
pub async fn get_object(
    State(state): State<HttpState>,
    Path((bucket, key)): Path<(String, String)>,
) -> impl IntoResponse {
    debug!("S3 get object: {}/{}", bucket, key);

    let bucket_info = match state.kv.get_bucket(&bucket).await {
        Ok(b) => b,
        Err(_) => return (StatusCode::NOT_FOUND, Vec::<u8>::new()),
    };

    let oid = KvStore::key_to_oid(bucket_info.vid, &key);
    let result = crate::request::exec_local_request(
        state.sys.clone(),
        SdRequest::ReadObj {
            oid,
            offset: 0,
            length: 0, // read all
        },
    )
    .await;

    match result {
        Ok(ResponseResult::Data(data)) => (StatusCode::OK, data),
        _ => (StatusCode::NOT_FOUND, Vec::new()),
    }
}

/// DELETE /{bucket}/{key} — Delete an object.
#[cfg(feature = "http")]
pub async fn delete_object(
    State(state): State<HttpState>,
    Path((bucket, key)): Path<(String, String)>,
) -> impl IntoResponse {
    debug!("S3 delete object: {}/{}", bucket, key);

    let bucket_info = match state.kv.get_bucket(&bucket).await {
        Ok(b) => b,
        Err(_) => return StatusCode::NOT_FOUND,
    };

    let oid = KvStore::key_to_oid(bucket_info.vid, &key);
    let _ = crate::request::exec_local_request(
        state.sys.clone(),
        SdRequest::RemoveObj { oid },
    )
    .await;

    StatusCode::NO_CONTENT
}

/// HEAD /{bucket}/{key} — Check if object exists.
#[cfg(feature = "http")]
pub async fn head_object(
    State(state): State<HttpState>,
    Path((bucket, key)): Path<(String, String)>,
) -> impl IntoResponse {
    debug!("S3 head object: {}/{}", bucket, key);

    let bucket_info = match state.kv.get_bucket(&bucket).await {
        Ok(b) => b,
        Err(_) => return StatusCode::NOT_FOUND,
    };

    let oid = KvStore::key_to_oid(bucket_info.vid, &key);
    let result = crate::request::exec_local_request(
        state.sys.clone(),
        SdRequest::ReadObj {
            oid,
            offset: 0,
            length: 1, // just check existence
        },
    )
    .await;

    match result {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::NOT_FOUND,
    }
}
