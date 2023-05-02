// src/config.rs

//! Configuration for the horloge extension.

// fixme: move more stuff into this configuration

/// The database name that will be used for connecting to SPI.
pub const SPI_DATABASE_NAME: Option<&'static str> = Some("horloge");
/// The user name that will be used for connecting to SPI.
pub const SPI_USER_NAME: Option<&'static str> = None;
