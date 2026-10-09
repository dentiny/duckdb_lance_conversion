#pragma once

// Shared source of truth for C++ and Rust storage defaults.
// Cargo's build.rs generates typed Rust constants from these numeric definitions.
#define LANCE_DEFAULT_STORAGE_TIMEOUT_MS         60000
#define LANCE_DEFAULT_STORAGE_IO_TIMEOUT_MS      10000
#define LANCE_DEFAULT_STORAGE_RETRY_MAX_TIMES    3
#define LANCE_DEFAULT_STORAGE_RETRY_MIN_DELAY_MS 1000
#define LANCE_DEFAULT_STORAGE_RETRY_MAX_DELAY_MS 60000
#define LANCE_DEFAULT_STORAGE_RETRY_FACTOR       2.0
