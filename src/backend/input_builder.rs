use crate::backend::SimpleInput;
use actix_web::dev::ServiceRequest;
use actix_web::ResponseError;
use std::future::{ready, Ready};
use std::net::{AddrParseError, IpAddr, Ipv6Addr};
use std::time::Duration;
use thiserror::Error;

type CustomFn = Box<dyn Fn(&ServiceRequest) -> Result<String, actix_web::Error>>;
type ExtCustomFn = Box<
    dyn Fn(&ServiceRequest) -> Result<(String, Option<Duration>, Option<u64>), actix_web::Error>,
>;

pub type SimpleInputFuture = Ready<Result<SimpleInput, actix_web::Error>>;

/// Utility to create a input function that produces a [SimpleInput].
///
/// You should take care to ensure that you are producing unique keys per backend.
///
/// This will not be of any use if you want to use dynamic interval/request policies
/// or perform an asynchronous option; you should instead write your own input function.
pub struct SimpleInputFunctionBuilder {
    interval: Duration,
    max_requests: u64,
    real_ip_key: bool,
    peer_ip_key: bool,
    path_key: bool,
    custom_key: Option<String>,
    custom_fn: Option<CustomFn>,
    ext_custom_fn: Option<ExtCustomFn>,
}

impl SimpleInputFunctionBuilder {
    pub fn new(interval: Duration, max_requests: u64) -> Self {
        Self {
            interval,
            max_requests,
            real_ip_key: false,
            peer_ip_key: false,
            path_key: false,
            custom_key: None,
            custom_fn: None,
            ext_custom_fn: None,
        }
    }

    /// Adds the client's real IP to the rate limiting key.
    ///
    /// # Security
    ///
    /// This calls
    /// [ConnectionInfo::realip_remote_addr()](actix_web::dev::ConnectionInfo::realip_remote_addr)
    /// internally which is only suitable for Actix applications deployed behind a proxy that you
    /// control.
    ///
    /// # IPv6
    ///
    /// IPv6 addresses will be grouped into a single key per /64
    pub fn real_ip_key(mut self) -> Self {
        self.real_ip_key = true;
        self
    }

    /// Adds the connection peer IP to the rate limiting key.
    ///
    /// This is suitable when clients connect directly to the Actix application.
    ///
    /// # IPv6
    ///
    /// IPv6 addresses will be grouped into a single key per /64
    pub fn peer_ip_key(mut self) -> Self {
        self.peer_ip_key = true;
        self
    }

    /// Add the request path to the rate limiting key
    pub fn path_key(mut self) -> Self {
        self.path_key = true;
        self
    }

    /// Add a custom component to the rate limiting key
    pub fn custom_key(mut self, key: &str) -> Self {
        self.custom_key = Some(key.to_owned());
        self
    }

    /// Dynamically add a custom component to the rate limiting key
    pub fn custom_fn<F>(mut self, f: F) -> Self
    where
        F: Fn(&ServiceRequest) -> Result<String, actix_web::Error> + 'static,
    {
        self.custom_fn = Some(Box::new(f));
        self
    }

    /// Similar to `custom_fn`, but providing the option to return alternative `interval`
    /// and `max_requests` for a particular key.
    ///
    /// This method can be used to implement dynamic rate limits for different endpoints
    /// or groups of endpoints, but care must be taken to ensure separate keys are used for
    /// each combination of limits/backend, otherwise the results will likely not match
    /// the expected behaviour.
    ///
    /// # Example
    /// ```
    /// use core::time::Duration;
    /// use actix_extensible_rate_limit::backend::SimpleInputFunctionBuilder;
    ///
    /// let builder = SimpleInputFunctionBuilder::new(Duration::from_secs(15), 30);
    ///
    /// builder
    /// .peer_ip_key()
    /// .ext_custom_fn(|req| {
    ///      let (key, interval, max_requests) = {
    ///          match req.uri().path() {
    ///              "/hallo" => ("hallo", Some(Duration::from_secs(5)), Some( 5)),
    ///              "/ciao"  => ("ciao", Some(Duration::from_secs(5)), Some(90)),
    ///              "/oi"    => ("oi", Some(Duration::from_secs(5)), Some(30)),
    ///              _        => ("default", None, None),
    ///          }
    ///      };
    ///      Ok((key.to_owned(), interval, max_requests))
    /// });
    /// ```
    pub fn ext_custom_fn<F>(mut self, f: F) -> Self
    where
        F: Fn(&ServiceRequest) -> Result<(String, Option<Duration>, Option<u64>), actix_web::Error>
            + 'static,
    {
        self.ext_custom_fn = Some(Box::new(f));
        self
    }

    pub fn build(self) -> impl Fn(&ServiceRequest) -> SimpleInputFuture + 'static {
        move |req| {
            ready((|| {
                let mut interval = self.interval;
                let mut max_requests = self.max_requests;
                let mut components = Vec::new();
                let info = req.connection_info();
                if let Some(custom) = &self.custom_key {
                    components.push(custom.clone());
                }
                if self.real_ip_key {
                    components.push(ip_key(info.realip_remote_addr().unwrap())?)
                }
                if self.peer_ip_key {
                    components.push(ip_key(info.peer_addr().unwrap())?)
                }
                if self.path_key {
                    components.push(req.path().to_owned());
                }
                if let Some(f) = &self.custom_fn {
                    components.push(f(req)?)
                }
                if let Some(f) = &self.ext_custom_fn {
                    let (component, ext_interval, ext_max_requests) = f(req)?;

                    interval = ext_interval.unwrap_or(interval);
                    max_requests = ext_max_requests.unwrap_or(max_requests);

                    components.push(component)
                }
                let key = components.join("-");

                Ok(SimpleInput {
                    interval,
                    max_requests,
                    key,
                })
            })())
        }
    }
}

#[derive(Debug, Error)]
enum Error {
    #[error("Unable to parse remote IP address: {0}")]
    InvalidIpError(
        #[source]
        #[from]
        AddrParseError,
    ),
}

impl ResponseError for Error {}

// Groups IPv6 addresses together, see:
// https://adam-p.ca/blog/2022/02/ipv6-rate-limiting/
// https://support.cloudflare.com/hc/en-us/articles/115001635128-Configuring-Cloudflare-Rate-Limiting
fn ip_key(ip_str: &str) -> Result<String, Error> {
    let ip = ip_str.parse::<IpAddr>()?;
    Ok(match ip {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4() {
                return Ok(v4.to_string());
            }
            let zeroes = [0u16; 4];
            let concat = [&v6.segments()[0..4], &zeroes].concat();
            let concat: [u16; 8] = concat.try_into().unwrap();
            let subnet = Ipv6Addr::from(concat);
            format!("{}/64", subnet)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ip_key() {
        // Check that IPv4 addresses are preserved
        assert_eq!(ip_key("142.250.187.206").unwrap(), "142.250.187.206");
        // Check that IPv4 mapped addresses are preserved
        assert_eq!(ip_key("::FFFF:142.250.187.206").unwrap(), "142.250.187.206");
        // Check that IPv6 addresses are grouped into /64 subnets
        assert_eq!(
            ip_key("2a00:1450:4009:81f::200e").unwrap(),
            "2a00:1450:4009:81f::/64"
        );
    }
}
