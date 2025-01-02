pub mod error;

use aws_config::retry::RetryConfig;
use aws_config::timeout::TimeoutConfig;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_sdk_dynamodb::Error;
use error::DynamoException;
use slog::{debug, o, Logger};

use std::collections::HashMap;
use std::fmt::Debug;
use std::time::Duration;

static mut CLI: Option<Client> = None;

pub trait Document {
    fn document_type() -> String;
}

pub fn init(
    logger: Logger,
    access_key_id: String,
    secret_access_key: String,
    region: String,
    table_name: String,
    key_field: String,

    // TODO: not implement sort key yet
    sort_key_field: Option<String>,
    endpoint: Option<String>,
) {
    unsafe {
        CLI = Some(Client::new(
            logger,
            access_key_id,
            secret_access_key,
            region,
            table_name,
            key_field,
            sort_key_field,
            endpoint,
        ));
    }
}

pub fn get_client(logger: &Logger) -> Client {
    let mut cli = unsafe { CLI.clone().unwrap() };
    cli.log = logger.new(o!("crate" => "easy-dynamodb"));
    cli
}

#[derive(Clone, Debug)]
pub struct Client {
    client: aws_sdk_dynamodb::Client,
    table_name: String,
    log: Logger,
    key_field: String,
    #[allow(dead_code)]
    sort_key_field: Option<String>,
}

impl Client {
    pub fn new(
        logger: Logger,
        access_key_id: String,
        secret_access_key: String,
        region: String,
        table_name: String,
        key_field: String,

        // TODO: not implement sort key yet
        sort_key_field: Option<String>,
        endpoint: Option<String>,
    ) -> Self {
        use aws_config::Region;
        use aws_sdk_dynamodb::Config;

        let timeout_config = TimeoutConfig::builder()
            .operation_attempt_timeout(Duration::from_secs(5))
            .build();

        let retry_config = RetryConfig::standard().with_max_attempts(3);
        let config = Config::builder()
            .credentials_provider(aws_credential_types::Credentials::from_keys(
                access_key_id,
                secret_access_key,
                None,
            ))
            .region(Region::new(region))
            .set_timeout_config(Some(timeout_config))
            .set_retry_config(Some(retry_config))
            .clone();

        let config = match endpoint {
            Some(endpoint_url) => config.endpoint_url(endpoint_url),
            None => config,
        };

        let config = config.build();

        Client {
            client: aws_sdk_dynamodb::Client::from_conf(config),
            table_name,
            key_field,
            sort_key_field,
            log: logger.new(o!("crate" => "easy-dynamodb", "struct" => "Client")),
        }
    }

    pub fn get_client(&self) -> aws_sdk_dynamodb::Client {
        self.client.clone()
    }

    fn get_log(&self, method: &'static str) -> Logger {
        self.log.new(o!("method" => method))
    }

    pub async fn upsert<T>(&self, doc: T) -> Result<(), DynamoException>
    where
        T: Debug + serde::Serialize,
    {
        let log = self.get_log("create");
        debug!(log, "{:?}", doc);
        let value = match serde_json::to_value(doc) {
            Ok(value) => value,
            Err(e) => return Err(DynamoException::DynamoSerializeException(format!("{e:?}"))),
        };
        let item = match serde_dynamo::to_item(value) {
            Ok(item) => item,
            Err(e) => return Err(DynamoException::DynamoSerializeException(format!("{e:?}"))),
        };

        match self
            .client
            .put_item()
            .table_name(&self.table_name)
            .set_item(Some(item))
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(DynamoException::DynamoPutItemException(format!("{e:?}"))),
        }
    }

    pub async fn create<T>(&self, doc: T) -> Result<(), DynamoException>
    where
        T: Debug + serde::Serialize,
    {
        let log = self.get_log("create");
        debug!(log, "{:?}", doc);
        let value = match serde_json::to_value(doc) {
            Ok(value) => value,
            Err(e) => return Err(DynamoException::DynamoSerializeException(format!("{e:?}"))),
        };
        let item: std::collections::HashMap<::std::string::String, AttributeValue> =
            match serde_dynamo::to_item(value) {
                Ok(item) => item,
                Err(e) => return Err(DynamoException::DynamoSerializeException(format!("{e:?}"))),
            };
        let condition = format!("attribute_not_exists({})", self.key_field);

        match self
            .client
            .put_item()
            .table_name(&self.table_name)
            .set_item(Some(item.clone()))
            .condition_expression(&condition)
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(DynamoException::DynamoPutItemException(format!(
                "{e:?}, {item:?}"
            ))),
        }
    }

    pub async fn update<T>(&self, key: &str, fields: Vec<(&str, T)>) -> Result<(), DynamoException>
    where
        T: Debug + serde::Serialize,
    {
        let log = self.get_log("update");
        debug!(log, "{:?} {:?}", key, fields);

        let (mut names, values, condition) = self.to_attribute_names_and_values(fields)?;

        let update_expression = format!("SET {}", &condition.join(", "));

        let key_field = self.key_field.clone();
        let key_value = AttributeValue::S(key.to_string());

        let condition_expression = format!("attribute_exists(#key)");
        names.insert("#key".to_string(), key_field.clone().into());

        debug!(log, "update_expression({:?}), key_field({:?}), key_value({:?}), condition_expression({:?}) names({names:?}), values({values:?})", update_expression, key_field, key_value, condition_expression);

        match self
            .client
            .update_item()
            .table_name(&self.table_name)
            .key(key_field, key_value)
            .update_expression(&update_expression)
            .set_expression_attribute_names(Some(names))
            .set_expression_attribute_values(Some(values))
            .condition_expression(condition_expression)
            .send()
            .await
        {
            Ok(e) => {
                debug!(log, "succeed {:?}", e);
                Ok(())
            }
            Err(e) => Err(DynamoException::DynamoUpdateItemException(format!("{e:?}"))),
        }
    }

    pub async fn delete(&self, key: &str) -> Result<(), DynamoException> {
        let log = self.get_log("delete");
        debug!(log, "{:?}", key);

        match self
            .client
            .delete_item()
            .table_name(&self.table_name)
            .key(self.key_field.clone(), AttributeValue::S(key.to_string()))
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(DynamoException::DynamoDeleteItemException(format!("{e:?}"))),
        }
    }

    pub async fn get<T>(&self, key: &str) -> Result<Option<T>, DynamoException>
    where
        T: Debug + serde::de::DeserializeOwned,
    {
        let log = self.get_log("get");
        debug!(log, "{:?}", key);
        let resp = match self
            .client
            .get_item()
            .table_name(&self.table_name)
            .key(self.key_field.clone(), AttributeValue::S(key.to_string()))
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(e) => return Err(DynamoException::DynamoGetItemException(format!("{e:?}"))),
        };

        Ok(match resp.item {
            Some(item) => {
                debug!(log, "item: {:?}", item);
                let value: T = match serde_dynamo::from_item(item) {
                    Ok(value) => value,
                    Err(e) => {
                        return Err(DynamoException::DynamoSerializeException(format!("{e:?}")))
                    }
                };
                Some(value)
            }
            None => None,
        })
    }

    fn to_attribute_names_and_values<F>(
        &self,
        filter: Vec<(&str, F)>,
    ) -> Result<
        (
            HashMap<String, String>,
            HashMap<String, AttributeValue>,
            Vec<String>,
        ),
        DynamoException,
    >
    where
        F: Debug + serde::Serialize,
    {
        let mut names = HashMap::new();
        let mut values = HashMap::new();
        let mut condition = vec![];

        for (name, value) in filter.iter() {
            names.insert(format!("#{name}"), name.to_string().clone());
            let value = match serde_dynamo::to_attribute_value(value) {
                Ok(value) => value,
                Err(e) => return Err(DynamoException::DynamoSerializeException(format!("{e:?}"))),
            };
            values.insert(format!(":{name}"), value);

            condition.push(format!("#{name} = :{name}"));
        }

        Ok((names, values, condition))
    }

    pub async fn find<T, F>(
        &self,
        index: &str,
        bookmark: Option<String>,
        size: Option<i32>,
        filter: Vec<(&str, F)>,
    ) -> Result<(Option<Vec<T>>, Option<String>), DynamoException>
    where
        T: Debug + serde::de::DeserializeOwned,
        F: Debug + serde::Serialize,
    {
        let log = self.get_log("find");
        debug!(
            log,
            "index: {:?} bookmark: {:?} size: {:?} filter: {:?}", index, bookmark, size, filter
        );

        let (names, values, condition) = self.to_attribute_names_and_values(filter)?;
        let size = size.unwrap_or(10);
        let bookmark = self.decode_bookmark(bookmark);
        let key_condition = &condition.join(" AND ");

        debug!(
            log,
            "key_condition: {:?} names: {:?} values: {:?} size: {:?}",
            key_condition,
            names,
            values,
            size
        );

        let resp = match self
            .client
            .query()
            .table_name(&self.table_name)
            .set_exclusive_start_key(bookmark)
            .index_name(index)
            .key_condition_expression(key_condition)
            .set_expression_attribute_names(Some(names))
            .set_expression_attribute_values(Some(values))
            .limit(size)
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                return Err(DynamoException::DynamoQueryException(format!("{e:?}")));
            }
        };

        crate::debug!(log, "response {:?}", resp);

        let docs = match resp.items {
            Some(items) => match serde_dynamo::from_items(items) {
                Ok(value) => Some(value),
                Err(e) => return Err(DynamoException::DynamoSerializeException(format!("{e:?}"))),
            },
            None => None,
        };

        crate::debug!(log, "docs: {:?}", docs);

        let bookmark = self.encode_bookmark(resp.last_evaluated_key);
        Ok((docs, bookmark))
    }

    pub async fn increment(
        &self,
        key: &str,
        field: &str,
        value: i64,
    ) -> Result<(), DynamoException> {
        let log = self.get_log("increment");
        debug!(log, "{:?} {:?} {:?}", key, field, value);

        let update_expression = format!("ADD #cnt :val");
        let condition_expression = format!("attribute_exists(#key)");

        let mut names = HashMap::new();
        names.insert("#cnt".to_string(), field.into());
        names.insert("#key".to_string(), self.key_field.clone().into());

        let mut values = HashMap::new();
        values.insert(":val".to_string(), AttributeValue::N(value.to_string()));

        match self
            .client
            .update_item()
            .table_name(&self.table_name)
            .key(self.key_field.clone(), AttributeValue::S(key.to_string()))
            .update_expression(&update_expression)
            .condition_expression(condition_expression)
            .set_expression_attribute_names(Some(names))
            .set_expression_attribute_values(Some(values))
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(DynamoException::DynamoIncrementException(format!("{e:?}"))),
        }
    }

    fn encode_bookmark(&self, bookmark: Option<HashMap<String, AttributeValue>>) -> Option<String> {
        if bookmark.is_none() {
            return None;
        }

        let bookmark = bookmark.unwrap();
        let bookmark = BookmarkModel::new(bookmark);
        Some(bookmark.to_string())
    }

    fn decode_bookmark(&self, bookmark: Option<String>) -> Option<HashMap<String, AttributeValue>> {
        if bookmark.is_none() {
            return None;
        }

        let bookmark = BookmarkModel::from_string(&bookmark.unwrap());
        let mut result = HashMap::new();

        for (key, value) in bookmark.keys.iter().zip(bookmark.values.iter()) {
            result.insert(key.clone(), value.clone().into());
        }

        Some(result)
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct BookmarkModel {
    keys: Vec<String>,
    values: Vec<serde_dynamo::AttributeValue>,
}

impl BookmarkModel {
    fn new(bookmark: HashMap<String, AttributeValue>) -> Self {
        let mut keys = vec![];
        let mut values = vec![];

        for (key, value) in bookmark {
            keys.push(key.clone());
            values.push(value.into());
        }
        BookmarkModel { keys, values }
    }

    fn to_string(&self) -> String {
        serde_json::to_string(self).unwrap()
    }

    fn from_string(s: &str) -> Self {
        serde_json::from_str(s).unwrap()
    }
}

impl Client {
    pub async fn table_exists(&self) -> Result<bool, Error> {
        let request = self.client.describe_table().table_name(&self.table_name);

        let resp = request.send().await;
        match resp {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    pub async fn list_tables(&self) -> Result<Vec<String>, Error> {
        let paginator = self.client.list_tables().into_paginator().items().send();
        let table_names = paginator.collect::<Result<Vec<_>, _>>().await?;

        Ok(table_names)
    }

    pub async fn get_total_items(&self) -> Result<u64, Error> {
        use aws_sdk_dynamodb::types::Select;

        let request = self
            .client
            .scan()
            .table_name(&self.table_name)
            .select(Select::Count);

        let resp = request.send().await?;
        let count = resp.count as u64;
        Ok(count)
    }

    pub async fn scan_table_items(&self) -> Result<Vec<HashMap<String, AttributeValue>>, Error> {
        let request = self.client.scan().table_name(&self.table_name);

        let resp = request.send().await?;
        let result = resp.items.unwrap_or_else(|| vec![]);

        Ok(result)
    }
}

#[cfg(test)]
mod dyanomdb_tests {
    use std::thread;

    use super::*;

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct TestModel {
        key: String,
        id: String,
        created_at: i64,
    }

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct TestModelV2 {
        key: String,
        id: String,
        created_at: i64,
        str_field: String,
        bool_field: bool,
    }

    #[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
    struct IndexModel {
        key: String,
        id: String,
        created_at: i64,
        r#type: String,
    }

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    enum TestField {
        #[serde(untagged)]
        N(i64),
        #[serde(untagged)]
        S(String),
        #[serde(untagged)]
        B(bool),
    }

    fn new_cli() -> Client {
        Client::new(
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
    }

    #[tokio::test]
    async fn test_create() {
        let client = new_cli();
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

        assert!(result.is_ok(), "test_create failed: {result:?}");
    }

    #[tokio::test]
    async fn test_create_duplicated_error() {
        let client = new_cli();
        let ts = chrono::Utc::now().timestamp_nanos_opt();
        assert!(ts.is_some(), "timestamp is none");
        let ts = ts.unwrap();

        let result = client
            .create(TestModel {
                key: format!("test_create_duplicated_error_key-{ts}"),
                id: format!("test_create_duplicated_error_id-{ts}"),
                created_at: ts,
            })
            .await;

        assert!(result.is_ok(), "{result:?}");

        let result = client
            .create(TestModel {
                key: format!("test_create_duplicated_error_key-{ts}"),
                id: format!("test_create_duplicated_error_id-{ts}"),
                created_at: 0,
            })
            .await;

        assert!(
            matches!(result, Err(DynamoException::DynamoPutItemException(_))),
            "{result:?}"
        );
    }

    #[tokio::test]
    async fn test_get() {
        let client = new_cli();
        let ts = chrono::Utc::now().timestamp_nanos_opt();
        assert!(ts.is_some(), "timestamp is none");
        let ts = ts.unwrap();

        let result = client
            .create(TestModel {
                key: format!("test_get_key-{ts}"),
                id: format!("test_get_id-{ts}"),
                created_at: ts,
            })
            .await;

        assert!(result.is_ok(), "{result:?}");
        let key = format!(
            "test_get_{}-{ts}",
            option_env!("AWS_DYNAMODB_KEY").unwrap_or("key")
        );

        let doc = client.get(&key).await;
        assert!(matches!(doc, Ok(Some(_))), "{doc:?}");
        let doc: TestModel = doc.unwrap().unwrap();

        assert!(doc.created_at == ts, "{doc:?}");
        assert!(doc.id == format!("test_get_id-{ts}"), "{doc:?}");
        assert!(doc.key == format!("test_get_key-{ts}"), "{doc:?}");
    }

    #[tokio::test]
    async fn test_update() {
        let client = new_cli();
        let ts = chrono::Utc::now().timestamp_nanos_opt();
        assert!(ts.is_some(), "timestamp is none");
        let ts = ts.unwrap();

        let result = client
            .create(TestModel {
                key: format!("test_update_key-{ts}"),
                id: format!("test_update_id-{ts}"),
                created_at: ts,
            })
            .await;

        assert!(result.is_ok(), "{result:?}");

        let key = format!(
            "test_update_{}-{ts}",
            option_env!("AWS_DYNAMODB_KEY").unwrap_or("key")
        );
        let result = client
            .update(
                &key,
                vec![
                    ("created_at", TestField::N(0)),
                    ("str_field", TestField::S("updated".to_string())),
                    ("bool_field", TestField::B(true)),
                ],
            )
            .await;

        assert!(result.is_ok(), "{result:?}");

        let doc_v1 = client.get::<TestModel>(&key).await;
        assert!(matches!(doc_v1, Ok(Some(_))), "{doc_v1:?}");
        let doc_v1 = doc_v1.unwrap().unwrap();

        assert!(doc_v1.created_at == 0, "{doc_v1:?}");
        assert!(doc_v1.id == format!("test_update_id-{ts}"), "{doc_v1:?}");
        assert!(doc_v1.key == format!("test_update_key-{ts}"), "{doc_v1:?}");

        let doc_v2 = client.get::<TestModelV2>(&key).await;
        assert!(matches!(doc_v2, Ok(Some(_))), "{doc_v2:?}");
        let doc_v2 = doc_v2.unwrap().unwrap();

        assert!(doc_v2.bool_field, "{doc_v2:?}");
        assert!(doc_v2.str_field == "updated".to_string(), "{doc_v2:?}");
        assert!(doc_v2.created_at == 0, "{doc_v2:?}");
        assert!(doc_v2.id == format!("test_update_id-{ts}"), "{doc_v2:?}");
        assert!(doc_v2.key == format!("test_update_key-{ts}"), "{doc_v2:?}");
    }

    #[tokio::test]
    async fn test_find() {
        let client = new_cli();

        let key_prefix = "test_find";
        let ts = chrono::Utc::now().timestamp_nanos_opt();
        assert!(ts.is_some(), "timestamp is none");
        let ts = ts.unwrap();

        for i in 0..10 {
            let result = client
                .create(IndexModel {
                    key: format!("{key_prefix}_key-{ts}_{i}"),
                    id: format!("{key_prefix}_id-{ts}_{i}"),
                    created_at: ts,
                    r#type: format!("type-{ts}-1").to_string(),
                })
                .await;

            assert!(result.is_ok(), "{result:?}");
        }

        for i in 0..10 {
            let result = client
                .create(IndexModel {
                    key: format!("{key_prefix}_key-{ts}_{i}_2"),
                    id: format!("{key_prefix}_id-{ts}_{i}_2"),
                    created_at: ts,
                    r#type: format!("type-{ts}-2").to_string(),
                })
                .await;

            assert!(result.is_ok(), "{result:?}");
        }

        thread::sleep(std::time::Duration::from_millis(100));

        let result = client
            .find(
                "type-index",
                None,
                Some(6),
                vec![("type", format!("type-{ts}-1"))],
            )
            .await;

        assert!(matches!(result, Ok((Some(_), Some(_)))), "{result:?}");
        let (docs, bookmark) = result.unwrap();
        let (docs, bookmark): (Vec<IndexModel>, String) = (docs.unwrap(), bookmark.unwrap());

        assert!(docs.len() == 6, "{docs:?}");
        assert!(bookmark.len() > 0, "{bookmark:?}");

        let result = client
            .find(
                "type-index",
                Some(bookmark),
                Some(6),
                vec![("type", format!("type-{ts}-1"))],
            )
            .await;

        assert!(matches!(result, Ok((Some(_), None))), "{result:?}");
        let (docs, _) = result.unwrap();
        let docs: Vec<IndexModel> = docs.unwrap();

        assert!(docs.len() == 4, "{docs:?}");
    }

    #[tokio::test]
    async fn test_delete() {
        let client = new_cli();
        let ts = chrono::Utc::now().timestamp_nanos_opt();
        assert!(ts.is_some(), "timestamp is none");
        let ts = ts.unwrap();

        let result = client
            .create(TestModel {
                key: format!("test_delete_key-{ts}"),
                id: format!("test_delete_id-{ts}"),
                created_at: ts,
            })
            .await;

        thread::sleep(std::time::Duration::from_millis(100));
        assert!(result.is_ok(), "{result:?}");

        let key = format!(
            "test_delete_{}-{ts}",
            option_env!("AWS_DYNAMODB_KEY").unwrap_or("key")
        );
        let result = client.delete(&key).await;

        assert!(result.is_ok(), "{result:?}");

        thread::sleep(std::time::Duration::from_millis(100));
        let doc = client.get::<TestModel>(&key).await;
        assert!(matches!(doc, Ok(None)), "{doc:?}");
    }

    #[tokio::test]
    async fn test_increment() {
        let client = new_cli();
        let ts = chrono::Utc::now().timestamp_nanos_opt();
        assert!(ts.is_some(), "timestamp is none");
        let ts = ts.unwrap();

        let result = client
            .create(TestModel {
                key: format!("test_increment_key-{ts}"),
                id: format!("test_increment_id-{ts}"),
                created_at: ts,
            })
            .await;

        assert!(result.is_ok(), "test_increment creation failed: {result:?}");
        thread::sleep(std::time::Duration::from_millis(100));
        let result = client
            .increment(&format!("test_increment_id-{ts}"), "created_at", 1)
            .await;
        assert!(result.is_ok(), "test_increment addition failed: {result:?}");

        thread::sleep(std::time::Duration::from_millis(100));

        let doc = client
            .get::<TestModel>(&format!("test_increment_id-{ts}"))
            .await;

        assert!(matches!(doc, Ok(Some(_))), "{doc:?}");
        let doc = doc.unwrap().unwrap();
        assert!(doc.created_at == ts + 1, "{doc:?}");
    }
}
