use aws_config::Region;
use aws_sdk_s3::{
    config::{self, Credentials},
    primitives::ByteStream,
    Client,
};
use axum::{
    extract::Multipart, http::StatusCode, routing::{get, post}, Extension, Json, Router
};
use serde::Serialize;
use tokio_util::bytes::Bytes;
use std::sync::Arc;
use tower_http::cors::{CorsLayer, Any};
use dotenv::dotenv;

#[derive(Serialize)]
struct ResponseMessage {
    status: u16,
    message: String,
}

#[tokio::main]
async fn main() {
    // initialize tracing
    tracing_subscriber::fmt::init();

    // Load .env file and set initialization variables
    dotenv().ok();

    // Initialize the AWS client
    let s3_client = get_aws_client();

    // Wrap the client in an Arc to share it safely
    let shared_s3_client = Arc::new(s3_client);

    let app = Router::new()
        .route("/upload", post(upload_handler))
        .route("/ping", get(get_ping))
        .fallback(handler_404)
        .layer(CorsLayer::new().allow_origin(Any))
        .layer(Extension(shared_s3_client));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn handler_404() -> (StatusCode, Json<ResponseMessage>) {
    (StatusCode::NOT_FOUND, Json(ResponseMessage{
        status: StatusCode::NOT_FOUND.as_u16(),
        message: "404 not found".to_string()
    }))
}

async fn get_ping() -> &'static str {
    "pong!"
}

fn get_aws_client() -> Client {
    let region = std::env::var("REGION")
        .expect("cannot find REGION env");
    let endpoint = std::env::var("ENDPOINT")
        .expect("cannot find ENDPOINT env");
    let aws3_cred_key_id = std::env::var("AWS3_CRED_KEY_ID")
        .expect("cannot find AWS3_CRED_KEY_ID env");
    let aws3_cred_key_secret = std::env::var("AWS3_CRED_KEY_SECRET")
        .expect("cannot find AWS3_CRED_KEY_SECRET env");

    // build the aws cred
    let cred = Credentials::new(aws3_cred_key_id, aws3_cred_key_secret, None, None, "local");

    // build aws config
    let region = Region::new(region.to_string());
    let conf_builder = config::Builder::new()
        .region(region)
        .credentials_provider(cred)
        .force_path_style(true)
        .endpoint_url(endpoint);
    let conf = conf_builder.build();

    // build aws client
    let client = Client::from_conf(conf);
    client
}

async fn upload_handler(
    Extension(s3_client): Extension<Arc<Client>>,
    mut multipart: Multipart,
) -> (StatusCode, Json<ResponseMessage>) {
    let bucket_name = std::env::var("BUCKET_NAME")
        .expect("cannot find BUCKET_NAME env");

    while let Some(field) = multipart.next_field().await.unwrap() {
        // Extract necessary information before mutable borrow
        let content_type = field.content_type().unwrap().to_string();
        let file_name = field.file_name().unwrap().to_string();

        // Perform mutable borrow
        let data = field.bytes().await.unwrap();

        // Call the function to upload to S3
        let result = upload_to_s3(&s3_client, file_name, content_type.as_str(), data.clone(), &bucket_name).await;

        match result {
            Ok(_) => {
                return (StatusCode::CREATED, Json(ResponseMessage{
                    status: StatusCode::CREATED.as_u16(),
                    message: "File uploaded successfully".to_string()
                }))
            },
            Err(_) => {
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(ResponseMessage{
                    status: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                    message: "Failed to upload file".to_string()
                }))
            },
        }
    }

    (StatusCode::BAD_REQUEST, Json(ResponseMessage{
        status: StatusCode::BAD_REQUEST.as_u16(),
        message: "No file found".to_string()
    }))
}

async fn upload_to_s3(
    s3_client: &Client,
    file_name: String,
    content_type: &str,
    data: Bytes,
    bucket_name: &String
) -> Result<(), aws_sdk_s3::Error> {
    println!("{} {}", file_name, content_type);
    let req = s3_client
        .put_object()
        .bucket(bucket_name)
        .body(ByteStream::from(data))
        .content_type(content_type)
        .key(file_name);
    req.send().await?;
    Ok(())
}