pub mod prelude;
use base64::prelude::*;

use reqwest::Method;
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct Client {
    url: url::Url,
    client: reqwest::Client,
}

trait Helper {
    fn apply_if<T, F>(self, val: Option<T>, fun: F) -> Self
    where
        Self: Sized,
        F: FnOnce(Self, T) -> Self;
}

impl Helper for reqwest::RequestBuilder {
    fn apply_if<T, F>(self, val: Option<T>, fun: F) -> Self
    where
        Self: Sized,
        F: FnOnce(Self, T) -> Self,
    {
        if let Some(val) = val {
            fun(self, val)
        } else {
            self
        }
    }
}

#[derive(Default)]
pub struct Kv {
    path: String,
    query: KvQuery,
    payload: Option<serde_json::Value>,
    body: Option<Vec<u8>>,
}

#[derive(Default, Serialize)]
pub struct KvQuery {
    dc: Option<String>,
    recurse: Option<bool>,
    raw: Option<bool>,
    keys: Option<bool>,
    separator: Option<String>,
}

#[derive(Debug)]
pub struct Response {
    status: u16,
    json: Option<serde_json::Value>,
    raw: String,
}

impl Response {
    pub fn raw(self) -> String {
        self.raw
    }

    pub fn json(self) -> Option<serde_json::Value> {
        self.json
    }

    pub fn status(self) -> u16 {
        self.status
    }

    pub fn is_success(&self) -> bool {
        self.status == 200
    }
}

impl Kv {
    pub fn new<S>(path: S) -> Self
    where
        S: Into<String>,
    {
        let path = format!("v1/kv/{}", path.into());
        Self {
            path,
            ..Default::default()
        }
    }

    pub fn dc<S>(mut self, dc: S) -> Self
    where
        S: Into<String>,
    {
        self.query.dc = Some(dc.into());
        self
    }

    pub fn recurse(mut self, value: bool) -> Self {
        self.query.recurse = Some(value);
        self
    }

    pub fn raw(mut self, value: bool) -> Self {
        self.query.raw = Some(value);
        self
    }

    pub fn keys(mut self, value: bool) -> Self {
        self.query.keys = Some(value);
        self
    }

    pub fn separator<S>(mut self, separator: S) -> Self
    where
        S: Into<String>,
    {
        self.query.separator = Some(separator.into());
        self
    }

    pub fn payload(mut self, payload: serde_json::Value) -> Self {
        self.payload = Some(payload);
        self
    }

    pub fn body(mut self, body: Vec<u8>) -> Self {
        self.body = Some(body);
        self
    }

    pub fn apply_if<T, F>(self, val: Option<T>, fun: F) -> Self
    where
        Self: Sized,
        F: FnOnce(Self, T) -> Self,
    {
        if let Some(val) = val {
            fun(self, val)
        } else {
            self
        }
    }

    pub async fn send_request(
        self,
        method: reqwest::Method,
        client: &Client,
    ) -> Result<Response, anyhow::Error> {
        let url = client.url.join(&self.path)?;
        let rs = client
            .client
            .request(method, url)
            .query(&self.query)
            .apply_if(self.payload, |k, v| k.json(&v))
            .apply_if(self.body, |k, v| k.body(v))
            .send()
            .await?;
        let status = rs.status();
        let raw = rs.text().await?;
        let json = serde_json::from_str::<serde_json::Value>(&raw).ok();
        Ok(Response {
            status: status.as_u16(),
            json,
            raw,
        })
    }

    pub async fn get(self, client: &Client) -> Result<Option<Record>, anyhow::Error> {
        let rs = self.send_request(Method::GET, client).await?;
        if rs.status == 404 {
            return Ok(None);
        };
        let mut key: Vec<Record> = rs.try_into()?;
        Ok(key.pop())
    }

    pub async fn put(self, client: &Client) -> Result<Response, anyhow::Error> {
        self.send_request(Method::PUT, client).await
    }

    pub async fn delete(self, client: &Client) -> Result<Response, anyhow::Error> {
        self.send_request(Method::DELETE, client).await
    }

    pub async fn list(self, client: &Client) -> Result<Vec<Record>, anyhow::Error> {
        let rs = self.recurse(true).send_request(Method::GET, client).await?;
        if rs.status == 404 {
            return Ok(vec![]);
        };

        rs.try_into()
    }
}

impl Client {
    pub fn new<S>(url: S) -> Result<Self, anyhow::Error>
    where
        S: Into<String>,
    {
        let client = reqwest::Client::new();
        let url = url.into().parse()?;
        Ok(Self { url, client })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Record {
    create_index: usize,
    flags: usize,
    key: String,
    lock_index: usize,
    modify_index: usize,
    value: String,
}

impl Record {
    pub fn create_index(&self) -> usize {
        self.create_index
    }

    pub fn flags(&self) -> usize {
        self.flags
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn lock_index(&self) -> usize {
        self.lock_index
    }

    pub fn modify_index(&self) -> usize {
        self.modify_index
    }

    pub fn value_as_slice(&self) -> Result<Vec<u8>, anyhow::Error> {
        let value = BASE64_STANDARD.decode(&self.value)?;
        Ok(value)
    }

    pub fn value(&self) -> Result<serde_json::Value, anyhow::Error> {
        let value = self.value_as_slice()?;
        let value: serde_json::Value = serde_json::from_slice(&value.to_vec())?;
        Ok(value)
    }
}

impl TryFrom<Response> for Vec<Record> {
    type Error = anyhow::Error;
    fn try_from(value: Response) -> Result<Self, Self::Error> {
        let Some(json) = value.json else {
            anyhow::bail!("No JSON in response");
        };
        Ok(serde_json::from_value(json)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_works() {
        let client = Client::new("http://localhost:8500").unwrap();
        let keys = vec!["path/to/key0", "path/to/key1"];
        for path in keys {
            Kv::new(path)
                .payload(serde_json::json!({"Some":"Shit"}))
                .put(&client)
                .await
                .unwrap();
        }
        let list = Kv::new("path/").list(&client).await.unwrap();
        assert_eq!(list.len(), 2);
        let record = Kv::new("path/to/key0").get(&client).await.unwrap();
        let value = record.unwrap().value().unwrap();
        assert_eq!(value["Some"], "Shit");
        Kv::new("path/to/key1").delete(&client).await.unwrap();
        let list = Kv::new("path/").list(&client).await.unwrap();
        assert_eq!(list.len(), 1);
    }
}
