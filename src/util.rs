use anyhow::Result;
use axum::{
  extract::Json,
  response::{IntoResponse, Response},
};
use serde::Serialize;

use crate::MinaMeshError;

pub struct Wrapper<T>(pub T);

impl<T: Serialize, E: ToString> IntoResponse for Wrapper<Result<T, E>>
where
  MinaMeshError: From<E>,
{
  fn into_response(self) -> Response {
    match self.0 {
      Ok(v) => Json(v).into_response(),
      Err(err) => {
        let mina_error: MinaMeshError = err.into();
        mina_error.into_response()
      }
    }
  }
}

impl Wrapper<Option<serde_json::Value>> {
  pub fn token_id_or_default(&self) -> Result<String, MinaMeshError> {
    match &self.0 {
      None => Ok(DEFAULT_TOKEN_ID.to_string()),
      Some(serde_json::Value::Object(map)) => {
        // `Value::to_string` re-serialises, so a JSON string comes back wrapped in quotes and
        // matches neither the archive's stored token value nor the daemon's token argument.
        Ok(map.get("token_id").and_then(|v| v.as_str()).unwrap_or(DEFAULT_TOKEN_ID).to_string())
      }
      _ => Err(MinaMeshError::JsonParse(None))?,
    }
  }
}

// cspell:disable-next-line
pub const DEFAULT_TOKEN_ID: &str = "wSHV2S4qX9jFsLjQo8r1BsMLH2ZRKsZx6EJd1sbozGPieEC4Jf";
pub const MINIMUM_USER_COMMAND_FEE: u64 = 1_000_000;

pub fn default_mina_proxy_url() -> String {
  "https://mainnet.minaprotocol.network/graphql".to_string()
}

#[cfg(test)]
mod tests {
  use serde_json::json;

  use super::{Wrapper, DEFAULT_TOKEN_ID};

  #[test]
  fn absent_metadata_gives_the_default_token() {
    assert_eq!(Wrapper(None).token_id_or_default().unwrap(), DEFAULT_TOKEN_ID);
  }

  #[test]
  fn absent_token_id_key_gives_the_default_token() {
    assert_eq!(Wrapper(Some(json!({}))).token_id_or_default().unwrap(), DEFAULT_TOKEN_ID);
  }

  // `Value::to_string` would return this wrapped in quotes, which matches neither the archive's
  // stored token value nor the daemon's token argument.
  #[test]
  fn token_id_is_read_without_its_json_quotes() {
    // cspell:disable-next-line
    let token = "wZbNQrfLKMKakUCkBVaCyHJ2rHtd5ZmvVXFRPXKPFhTLnCsRCK";
    let got = Wrapper(Some(json!({ "token_id": token }))).token_id_or_default().unwrap();
    assert_eq!(got, token);
    assert!(!got.contains('"'));
  }

  #[test]
  fn non_object_metadata_is_rejected() {
    assert!(Wrapper(Some(json!("not an object"))).token_id_or_default().is_err());
  }
}
