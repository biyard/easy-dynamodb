use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DynamoException {
    UnknownException(String),

    // DynamoDB errors
    DynamoSerializeException(String),
    DynamoDeserializeException(String),
    DynamoInvalidBookmark(String),
    DynamoPutItemException(String),
    DynamoUpdateItemException(String),
    DynamoQueryException(String),
    DynamoGetItemException(String),
    DynamoDeleteItemException(String),
    DynamoIncrementException(String),
}

impl std::fmt::Display for DynamoException {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::str::FromStr for DynamoException {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(DynamoException::UnknownException(s.to_string()))
    }
}

impl std::error::Error for DynamoException {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

impl DynamoException {
    pub fn to_string(&self) -> String {
        format!("BaseError: {:?}", self)
    }
}

unsafe impl Send for DynamoException {}
unsafe impl Sync for DynamoException {}
