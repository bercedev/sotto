//! Transactional transport for lifecycle-contact verification messages.
//!
//! The Resend endpoint is fixed to prevent credential forwarding to arbitrary hosts. Provider
//! response bodies are bounded and never logged or returned to the customer.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::contact_verification::{
    canonical_email, ContactVerificationSender, SendOutcome, VerificationMessage,
};
use crate::error::{Error, Result};

pub struct ResendSender {
    client: reqwest::Client,
    api_key: String,
    from_address: String,
    endpoint: String,
}

impl ResendSender {
    pub fn new(api_key: String, from_address: String) -> Result<Self> {
        Self::with_endpoint(
            api_key,
            from_address,
            "https://api.resend.com/emails".into(),
        )
    }

    pub(crate) fn with_endpoint(
        api_key: String,
        from_address: String,
        endpoint: String,
    ) -> Result<Self> {
        if !api_key.starts_with("re_") || api_key.len() < 8 || api_key.len() > 256 {
            return Err(Error::Config("RESEND_API_KEY is invalid".into()));
        }
        if canonical_email(&from_address).is_err() {
            return Err(Error::Config(
                "SOTTO_CLOUD_CONTACT_EMAIL_FROM is invalid".into(),
            ));
        }
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| Error::Config("contact email HTTP client could not be created".into()))?;
        Ok(Self {
            client,
            api_key,
            from_address,
            endpoint,
        })
    }
}

#[derive(Serialize)]
struct ResendEmailRequest<'a> {
    from: &'a str,
    to: [&'a str; 1],
    subject: &'static str,
    text: String,
}

#[derive(Deserialize)]
struct ResendAccepted {
    id: String,
}

#[async_trait]
impl ContactVerificationSender for ResendSender {
    async fn send(&self, message: VerificationMessage) -> SendOutcome {
        let body = ResendEmailRequest {
            from: &self.from_address,
            to: [&message.destination],
            subject: "Verify your Sotto Cloud email address",
            text: format!(
                "Use this one-time link to verify your Sotto Cloud lifecycle email contact:\n\n{}\n\nThe link expires in 30 minutes. If you did not request this, you can ignore this message.",
                message.verification_url
            ),
        };
        let response = self
            .client
            .post(&self.endpoint)
            .bearer_auth(&self.api_key)
            .header("Idempotency-Key", &message.message_id)
            .json(&body)
            .send()
            .await;
        let Ok(response) = response else {
            return SendOutcome::Unknown;
        };
        let status = response.status();
        if status.is_success() {
            if response
                .content_length()
                .is_some_and(|length| length > 2048)
            {
                return SendOutcome::Unknown;
            }
            let mut response = response;
            let mut response_body = Vec::new();
            loop {
                match response.chunk().await {
                    Ok(Some(chunk)) if response_body.len().saturating_add(chunk.len()) <= 2048 => {
                        response_body.extend_from_slice(&chunk);
                    }
                    Ok(Some(_)) => return SendOutcome::Unknown,
                    Ok(None) => break,
                    Err(_) => return SendOutcome::Unknown,
                }
            }
            return match serde_json::from_slice::<ResendAccepted>(&response_body) {
                Ok(accepted) if !accepted.id.trim().is_empty() => SendOutcome::Accepted {
                    provider_message_id: accepted.id,
                },
                _ => SendOutcome::Unknown,
            };
        }
        if status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            SendOutcome::Unknown
        } else {
            SendOutcome::Rejected
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn mock_resend(
        status: u16,
        payload: String,
        expected_idempotency_key: &'static str,
    ) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
        let address = listener.local_addr().expect("mock address");
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept provider request");
            let mut request = Vec::new();
            let mut chunk = [0_u8; 2048];
            let mut body_start = None;
            let mut body_len = 0;
            loop {
                let count = stream
                    .read(&mut chunk)
                    .await
                    .expect("read provider request");
                assert_ne!(
                    count, 0,
                    "provider client closed before sending full request"
                );
                request.extend_from_slice(&chunk[..count]);
                if body_start.is_none() {
                    if let Some(index) = request.windows(4).position(|window| window == b"\r\n\r\n")
                    {
                        body_start = Some(index + 4);
                        let headers =
                            String::from_utf8_lossy(&request[..index]).to_ascii_lowercase();
                        body_len = headers
                            .lines()
                            .find_map(|line| line.strip_prefix("content-length: "))
                            .and_then(|value| value.parse::<usize>().ok())
                            .expect("content length header");
                    }
                }
                if body_start.is_some_and(|start| request.len() >= start + body_len) {
                    break;
                }
            }
            let request = String::from_utf8_lossy(&request);
            assert!(request.starts_with("POST /emails HTTP/1.1"));
            assert!(request
                .to_ascii_lowercase()
                .contains("authorization: bearer re_test_api_key"));
            assert!(request
                .to_ascii_lowercase()
                .contains(&format!("idempotency-key: {expected_idempotency_key}")));
            assert!(request.contains("to@example.test"));
            assert!(request.contains("contact_token="));
            let reason = if (200..300).contains(&status) {
                "OK"
            } else {
                "Service Error"
            };
            let response = format!(
                "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{payload}",
                payload.len()
            );
            let _ = stream.write_all(response.as_bytes()).await;
        });
        format!("http://{address}/emails")
    }

    #[tokio::test]
    async fn resend_uses_durable_message_id_and_classifies_provider_responses() {
        let endpoint = mock_resend(
            200,
            r#"{"id":"provider-message-1"}"#.into(),
            "contact-verification:test",
        )
        .await;
        let sender = ResendSender::with_endpoint(
            "re_test_api_key".into(),
            "no-reply@example.test".into(),
            endpoint,
        )
        .expect("sender config");
        assert_eq!(
            sender
                .send(VerificationMessage {
                    message_id: "contact-verification:test".into(),
                    destination: "to@example.test".into(),
                    verification_url: "https://app.example.test/cloud#contact_token=token-value"
                        .into(),
                    expires_at_epoch: 1_900_000_000,
                })
                .await,
            SendOutcome::Accepted {
                provider_message_id: "provider-message-1".into()
            }
        );

        let endpoint = mock_resend(
            401,
            r#"{"message":"secret provider detail"}"#.into(),
            "contact-verification:rejected",
        )
        .await;
        let sender = ResendSender::with_endpoint(
            "re_test_api_key".into(),
            "no-reply@example.test".into(),
            endpoint,
        )
        .expect("sender config");
        let rejected = sender
            .send(VerificationMessage {
                message_id: "contact-verification:rejected".into(),
                destination: "to@example.test".into(),
                verification_url: "https://app.example.test/cloud#contact_token=other-token".into(),
                expires_at_epoch: 1_900_000_000,
            })
            .await;
        assert_eq!(rejected, SendOutcome::Rejected);
        assert!(!format!("{rejected:?}").contains("secret provider detail"));

        let endpoint = mock_resend(
            429,
            r#"{"message":"limited"}"#.into(),
            "contact-verification:retry",
        )
        .await;
        let sender = ResendSender::with_endpoint(
            "re_test_api_key".into(),
            "no-reply@example.test".into(),
            endpoint,
        )
        .expect("sender config");
        assert_eq!(
            sender
                .send(VerificationMessage {
                    message_id: "contact-verification:retry".into(),
                    destination: "to@example.test".into(),
                    verification_url: "https://app.example.test/cloud#contact_token=retry-token"
                        .into(),
                    expires_at_epoch: 1_900_000_000,
                })
                .await,
            SendOutcome::Unknown
        );

        let endpoint = mock_resend(200, "x".repeat(2049), "contact-verification:oversize").await;
        let sender = ResendSender::with_endpoint(
            "re_test_api_key".into(),
            "no-reply@example.test".into(),
            endpoint,
        )
        .expect("sender config");
        assert_eq!(
            sender
                .send(VerificationMessage {
                    message_id: "contact-verification:oversize".into(),
                    destination: "to@example.test".into(),
                    verification_url: "https://app.example.test/cloud#contact_token=oversize"
                        .into(),
                    expires_at_epoch: 1_900_000_000,
                })
                .await,
            SendOutcome::Unknown
        );
    }
}
