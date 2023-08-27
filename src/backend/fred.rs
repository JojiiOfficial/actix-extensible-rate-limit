use crate::backend::{Backend, SimpleBackend, SimpleInput, SimpleOutput};
use actix_web::{rt::time::Instant, HttpResponse, ResponseError};
use async_trait::async_trait;
use fred::prelude::*;
use std::{borrow::Cow, time::Duration};
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum Error {
    #[error("Redis error: {0}")]
    Redis(
        #[source]
        #[from]
        fred::error::RedisError,
    ),
}

impl ResponseError for Error {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::InternalServerError().finish()
    }
}

/// A Fixed Window rate limiter [Backend] that uses stores data in Redis.
#[derive(Clone)]
pub struct RedisBackend {
    connection: RedisClient,
    key_prefix: Option<String>,
}

impl RedisBackend {
    pub fn builder(connection: RedisClient) -> Builder {
        Builder {
            connection,
            key_prefix: None,
        }
    }

    fn make_key<'t>(&self, key: &'t str) -> Cow<'t, str> {
        match &self.key_prefix {
            None => Cow::Borrowed(key),
            Some(prefix) => Cow::Owned(format!("{prefix}{key}")),
        }
    }
}

pub struct Builder {
    connection: RedisClient,
    key_prefix: Option<String>,
}

impl Builder {
    /// Apply an optional prefix to all rate limit keys given to this backend.
    ///
    /// This may be useful when the Redis instance is being used for other purposes; the prefix is
    /// used as a 'namespace' to avoid collision with other caches or keys inside Redis.
    pub fn key_prefix(mut self, key_prefix: Option<&str>) -> Self {
        self.key_prefix = key_prefix.map(ToOwned::to_owned);
        self
    }

    pub fn build(self) -> RedisBackend {
        RedisBackend {
            connection: self.connection,
            key_prefix: self.key_prefix,
        }
    }
}

#[async_trait(?Send)]
impl Backend<SimpleInput> for RedisBackend {
    type Output = SimpleOutput;
    type RollbackToken = String;
    type Error = Error;

    async fn request(
        &self,
        input: SimpleInput,
    ) -> Result<(bool, Self::Output, Self::RollbackToken), Self::Error> {
        let key = self.make_key(&input.key);
        let pipeline = self.connection.pipeline();
        let ex = Expiration::EX(input.interval.as_secs() as i64);
        let opt = SetOptions::NX;

        let _: () = pipeline
            .set(key.as_ref(), 0, Some(ex), Some(opt), false)
            .await
            .unwrap();
        let _: () = pipeline.incr(key.as_ref()).await.unwrap();
        let _: () = pipeline.ttl(key.as_ref()).await.unwrap();
        let (_, count, ttl): ((), u64, u64) = pipeline.all().await?;

        let allow = count <= input.max_requests;
        let output = SimpleOutput {
            limit: input.max_requests,
            remaining: input.max_requests.saturating_sub(count),
            reset: Instant::now() + Duration::from_secs(ttl as u64),
        };
        Ok((allow, output, input.key))
    }

    async fn rollback(&self, token: Self::RollbackToken) -> Result<(), Self::Error> {
        let key = self.make_key(&token);

        self.connection.watch(key.as_ref()).await?;
        let trx = &self.connection;
        let old_val: Option<u64> = trx.get(key.as_ref()).await?;
        if let Some(old_val) = old_val {
            if old_val >= 1 {
                trx.decr(key.as_ref()).await?;
            }
        }
        self.connection.unwatch().await?;
        Ok(())
    }
}

#[async_trait(?Send)]
impl SimpleBackend for RedisBackend {
    /// Note that the key prefix (if set) is automatically included, you do not need to prepend
    /// it yourself.
    async fn remove_key(&self, key: &str) -> Result<(), Self::Error> {
        let key = self.make_key(key);
        let con = self.connection.clone();
        con.del(key.as_ref()).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HeaderCompatibleOutput;

    const MINUTE: Duration = Duration::from_secs(60);

    // Each test must use non-overlapping keys (because the tests may be run concurrently)
    // Each test should also reset its key on each run, so that it is in a clean state.
    async fn make_backend(clear_test_key: &str) -> Builder {
        /* let client = redis::Client::open(format!("redis://{host}:{port}")).unwrap();
        let mut manager = ConnectionManager::new(client).await.unwrap();
        manager.del::<_, ()>(clear_test_key).await.unwrap(); */
        // RedisBackend::builder(manager)
        let config = RedisConfig::from_url("redis://default:mypassword@127.0.0.1:6379/1").unwrap();
        let client = RedisClient::new(config, None, None);
        let _ = client.connect();
        let _ = client.wait_for_connect().await.unwrap();
        client.del::<(), _>(clear_test_key).await.unwrap();
        RedisBackend::builder(client)
    }

    #[actix_web::test]
    async fn test_allow_deny() {
        let backend = make_backend("test_allow_deny").await.build();
        let input = SimpleInput {
            interval: MINUTE,
            max_requests: 5,
            key: "test_allow_deny".to_string(),
        };
        for _ in 0..5 {
            // First 5 should be allowed
            let (allow, _, _) = backend.request(input.clone()).await.unwrap();
            assert!(allow);
        }
        // Sixth should be denied
        let (allow, _, _) = backend.request(input.clone()).await.unwrap();
        assert!(!allow);
    }

    #[actix_web::test]
    async fn test_reset() {
        let backend = make_backend("test_reset").await.build();
        let input = SimpleInput {
            interval: Duration::from_secs(3),
            max_requests: 1,
            key: "test_reset".to_string(),
        };
        // Make first request, should be allowed
        let (allow, _, _) = backend.request(input.clone()).await.unwrap();
        assert!(allow);
        // Request again, should be denied
        let (allow, out, _) = backend.request(input.clone()).await.unwrap();
        assert!(!allow);
        // Sleep until reset, should now be allowed
        tokio::time::sleep(Duration::from_secs(out.seconds_until_reset())).await;
        let (allow, _, _) = backend.request(input).await.unwrap();
        assert!(allow);
    }

    #[actix_web::test]
    async fn test_output() {
        let backend = make_backend("test_output").await.build();
        let input = SimpleInput {
            interval: MINUTE,
            max_requests: 2,
            key: "test_output".to_string(),
        };
        // First of 2 should be allowed.
        let (allow, output, _) = backend.request(input.clone()).await.unwrap();
        assert!(allow);
        assert_eq!(output.remaining, 1);
        assert_eq!(output.limit, 2);
        assert!(output.seconds_until_reset() > 0 && output.seconds_until_reset() <= 60);
        // Second of 2 should be allowed.
        let (allow, output, _) = backend.request(input.clone()).await.unwrap();
        assert!(allow);
        assert_eq!(output.remaining, 0);
        assert_eq!(output.limit, 2);
        assert!(output.seconds_until_reset() > 0 && output.seconds_until_reset() <= 60);
        // Should be denied
        let (allow, output, _) = backend.request(input).await.unwrap();
        assert!(!allow);
        assert_eq!(output.remaining, 0);
        assert_eq!(output.limit, 2);
        assert!(output.seconds_until_reset() > 0 && output.seconds_until_reset() <= 60);
    }

    #[actix_web::test]
    async fn test_rollback() {
        let backend = make_backend("test_rollback").await.build();
        let input = SimpleInput {
            interval: MINUTE,
            max_requests: 5,
            key: "test_rollback".to_string(),
        };
        let (_, output, rollback) = backend.request(input.clone()).await.unwrap();
        assert_eq!(output.remaining, 4);
        backend.rollback(rollback).await.unwrap();
        // Remaining requests should still be the same, since the previous call was excluded
        let (_, output, _) = backend.request(input).await.unwrap();
        assert_eq!(output.remaining, 4);
        // Check ttl is not corrupted
        assert!(output.seconds_until_reset() > 0 && output.seconds_until_reset() <= 60);
    }

    #[actix_web::test]
    async fn test_rollback_key_gone() {
        let backend = make_backend("test_rollback_key_gone").await.build();
        let con = backend.connection.clone();
        // The rollback could happen after the key has already expired
        backend
            .rollback("test_rollback_key_gone".to_string())
            .await
            .unwrap();
        // In which case nothing should happen
        assert!(!con
            .exists::<bool, _>("test_rollback_key_gone")
            .await
            .unwrap());
    }

    #[actix_web::test]
    async fn test_remove_key() {
        let backend = make_backend("test_remove_key").await.build();
        let input = SimpleInput {
            interval: MINUTE,
            max_requests: 1,
            key: "test_remove_key".to_string(),
        };
        let (allow, _, _) = backend.request(input.clone()).await.unwrap();
        assert!(allow);
        let (allow, _, _) = backend.request(input.clone()).await.unwrap();
        assert!(!allow);
        backend.remove_key("test_remove_key").await.unwrap();
        // Counter should have been reset
        let (allow, _, _) = backend.request(input).await.unwrap();
        assert!(allow);
    }

    #[actix_web::test]
    async fn test_key_prefix() {
        let backend = make_backend("prefix:test_key_prefix")
            .await
            .key_prefix(Some("prefix:"))
            .build();
        let con = backend.connection.clone();
        let input = SimpleInput {
            interval: MINUTE,
            max_requests: 5,
            key: "test_key_prefix".to_string(),
        };
        backend.request(input.clone()).await.unwrap();
        assert!(con
            .exists::<bool, _>("prefix:test_key_prefix")
            .await
            .unwrap());

        backend.remove_key("test_key_prefix").await.unwrap();
        assert!(!con
            .exists::<bool, _>("prefix:test_key_prefix")
            .await
            .unwrap());
    }
}
