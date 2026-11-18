//! The boxd public gRPC API: one TLS channel, and the short-lived JWT every
//! call carries, exchanged from the entry's API key and refreshed before it
//! expires.

use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sandbox_driver::{Error, MutationOutcome};
use serde::Deserialize;
use tokio::sync::Mutex;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Code, Status};

use crate::api::boxd_api_client::BoxdApiClient;

/// Bounds connecting, the token exchange, and each unary call. Streams
/// (exec, file transfer) are bounded by their callers instead.
const CALL_TIMEOUT: Duration = Duration::from_secs(60);
/// A token is replaced this long before boxd says it expires, so a call
/// never starts with one about to lapse.
const REFRESH_MARGIN: Duration = Duration::from_secs(300);
/// gRPC's default 4 MiB receive limit is below what one `FsRead` or exec
/// chunk may carry.
const MAX_MESSAGE: usize = 64 * 1024 * 1024;

pub(crate) type Api = BoxdApiClient<Channel>;

/// The API key, which is a secret; `Debug` never prints it.
#[derive(Clone)]
pub(crate) struct ApiKey(pub(crate) String);

impl fmt::Debug for ApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ApiKey(<redacted>)")
    }
}

struct Token {
    bearer:  MetadataValue<Ascii>,
    refresh: SystemTime,
}

pub(crate) struct Client {
    channel:  Channel,
    http:     reqwest::Client,
    auth_url: String,
    api_key:  ApiKey,
    token:    Mutex<Option<Token>>,
}

#[derive(Deserialize)]
struct Exchanged {
    token:      String,
    expires_at: u64,
}

impl Client {
    /// Opens the channel lazily: nothing is contacted until the first call.
    pub(crate) fn new(api_url: &str, auth_url: String, api_key: ApiKey) -> Result<Self, String> {
        let mut endpoint = Endpoint::from_shared(api_url.to_owned())
            .map_err(|error| format!("api_url: {error}"))?
            .connect_timeout(CALL_TIMEOUT)
            .user_agent(concat!("sandbox-driver/", env!("CARGO_PKG_VERSION")))
            .map_err(|error| format!("user agent: {error}"))?;
        if api_url.starts_with("https://") {
            endpoint = endpoint
                .tls_config(ClientTlsConfig::new().with_webpki_roots())
                .map_err(|error| format!("api_url TLS: {error}"))?;
        }
        let http = reqwest::Client::builder()
            .timeout(CALL_TIMEOUT)
            .build()
            .map_err(|error| format!("HTTP client: {error}"))?;
        Ok(Self {
            channel: endpoint.connect_lazy(),
            http,
            auth_url,
            api_key,
            token: Mutex::new(None),
        })
    }

    pub(crate) fn api(&self) -> Api {
        BoxdApiClient::new(self.channel.clone())
            .max_decoding_message_size(MAX_MESSAGE)
            .max_encoding_message_size(MAX_MESSAGE)
    }

    /// `message` as an authenticated request.
    pub(crate) async fn request<T>(&self, message: T) -> Result<tonic::Request<T>, Error> {
        let mut request = tonic::Request::new(message);
        request
            .metadata_mut()
            .insert("authorization", self.bearer().await?);
        Ok(request)
    }

    /// `message` as an authenticated unary request with the call timeout.
    pub(crate) async fn unary<T>(&self, message: T) -> Result<tonic::Request<T>, Error> {
        let mut request = self.request(message).await?;
        request.set_timeout(CALL_TIMEOUT);
        Ok(request)
    }

    async fn bearer(&self) -> Result<MetadataValue<Ascii>, Error> {
        let mut token = self.token.lock().await;
        if let Some(current) = token.as_ref() {
            if SystemTime::now() < current.refresh {
                return Ok(current.bearer.clone());
            }
        }
        let fresh = self.exchange().await?;
        let bearer = fresh.bearer.clone();
        *token = Some(fresh);
        Ok(bearer)
    }

    /// Trades the API key for a JWT. A refusal is reported as such; nothing
    /// was admitted either way.
    async fn exchange(&self) -> Result<Token, Error> {
        let unavailable = |message: String| Error::Unavailable {
            message,
            outcome: MutationOutcome::NotStarted,
        };
        let response = self
            .http
            .post(&self.auth_url)
            .json(&serde_json::json!({ "api_key": self.api_key.0 }))
            .send()
            .await
            .map_err(|error| {
                unavailable(format!("exchanging the API key: {}", error.without_url()))
            })?;
        let status = response.status().as_u16();
        let message = format!("exchanging the API key: HTTP {status}");
        match status {
            200..=299 => {}
            400 | 401 => return Err(Error::Unauthenticated { message }),
            403 => return Err(Error::PermissionDenied { message }),
            429 => {
                return Err(Error::ResourceExhausted {
                    message,
                    retry_after_ms: None,
                });
            }
            _ => return Err(unavailable(message)),
        }
        let exchanged: Exchanged = response
            .json()
            .await
            .map_err(|error| unavailable(format!("reading the token: {}", error.without_url())))?;
        let bearer = format!("Bearer {}", exchanged.token)
            .parse()
            .map_err(|_| unavailable("boxd returned an unusable token".to_owned()))?;
        let expires = UNIX_EPOCH + Duration::from_secs(exchanged.expires_at);
        Ok(Token {
            bearer,
            refresh: expires.checked_sub(REFRESH_MARGIN).unwrap_or(UNIX_EPOCH),
        })
    }
}

/// Whether boxd said the thing addressed does not exist.
pub(crate) fn not_found(status: &Status) -> bool {
    status.code() == Code::NotFound
}

/// The contract's view of a failed call. `NOT_FOUND` must be handled by the
/// caller, which knows what was addressed; here it is a transport fault,
/// never absence.
pub(crate) fn classify(operation: &str, status: &Status) -> Error {
    let message = format!("{operation}: {}", status.message());
    match status.code() {
        Code::Unauthenticated => Error::Unauthenticated { message },
        Code::PermissionDenied => Error::PermissionDenied { message },
        Code::ResourceExhausted => Error::ResourceExhausted {
            message,
            retry_after_ms: None,
        },
        Code::InvalidArgument | Code::OutOfRange => Error::InvalidSpec {
            path:    operation.to_owned(),
            message: status.message().to_owned(),
        },
        Code::FailedPrecondition | Code::Aborted | Code::AlreadyExists => Error::Busy { message },
        Code::Unimplemented => Error::Unsupported {
            capability: message,
        },
        Code::Unavailable | Code::DeadlineExceeded | Code::Cancelled => Error::Unavailable {
            message,
            outcome: MutationOutcome::Unknown,
        },
        Code::Ok | Code::Unknown | Code::Internal | Code::DataLoss | Code::NotFound => {
            Error::Transport {
                message,
                outcome: MutationOutcome::Unknown,
            }
        }
    }
}

/// Whether boxd refused a call before admitting it, so the resource it
/// addressed is untouched.
pub(crate) fn refused(error: &Error) -> bool {
    matches!(
        error,
        Error::Unauthenticated { .. }
            | Error::PermissionDenied { .. }
            | Error::ResourceExhausted { .. }
            | Error::InvalidSpec { .. }
            | Error::Busy { .. }
            | Error::Unsupported { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_and_absence_are_never_read_as_an_outage() {
        assert!(matches!(
            classify("x", &Status::unauthenticated("expired")),
            Error::Unauthenticated { .. }
        ));
        assert!(matches!(
            classify("x", &Status::not_found("vm")),
            Error::Transport { .. }
        ));
        assert!(matches!(
            classify("x", &Status::unavailable("down")),
            Error::Unavailable {
                outcome: MutationOutcome::Unknown,
                ..
            }
        ));
        assert!(refused(&classify(
            "x",
            &Status::failed_precondition("stopped")
        )));
    }

    #[test]
    fn the_api_key_never_prints() {
        assert_eq!(
            format!("{:?}", ApiKey("bxd_secret".to_owned())),
            "ApiKey(<redacted>)"
        );
    }
}
