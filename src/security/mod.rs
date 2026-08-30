//! Transport security: shared-token authentication and TLS termination.
//!
//! Neither is ever on the steady-state per-message path — authentication is
//! verified once per connection, and TLS is terminated with one handshake per
//! connection in the accept loop. The [`SharedSecurity`] holder lets both be
//! rotated at runtime via `SIGHUP` without restarting the broker.

pub mod auth;
pub mod shared;
pub mod tls;

pub use auth::{secure_eq, AccessControl};
pub use shared::{SharedSecurity, read_token_file};
pub use tls::{build_tls_acceptor, TlsStream};
