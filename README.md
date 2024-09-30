# Easy DynamoDB

Refer to test code in `lib.rs`

## Creating a new data to DynamoDB

``` rust
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct TestModel {
    key: String,
    id: String,
    created_at: i64,
}

let client = Client::new(
    slog::Logger::root(slog::Discard, o!("goal" => "testing")),
    option_env!("AWS_ACCESS_KEY_ID")
        .expect("AWS_ACCESS_KEY_ID is required")
        .to_string(),
    option_env!("AWS_SECRET_ACCESS_KEY")
        .expect("AWS_SECRET_ACCESS_KEY is required")
        .to_string(),
    option_env!("AWS_REGION")
        .unwrap_or("ap-northeast-2")
        .to_string(),
    option_env!("AWS_DYNAMODB_TABLE")
        .expect("AWS_DYNAMODB_TABLE is required")
        .to_string(),
    option_env!("AWS_DYNAMODB_KEY").unwrap_or("key").to_string(),
    None,
    None,
)
.await
let ts = chrono::Utc::now().timestamp_nanos_opt();
assert!(ts.is_some(), "timestamp is none");
let ts = ts.unwrap();

let result = client
    .create(TestModel {
        key: format!("test_create_key-{ts}"),
        id: format!("test_create_id-{ts}"),
        created_at: ts,
    })
    .await;

```
