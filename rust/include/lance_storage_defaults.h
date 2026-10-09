#pragma once

#include <stdint.h>

// Shared source of truth for C++ and Rust storage defaults.
// Cargo's build.rs generates typed Rust constants from these numeric definitions.
inline constexpr uint64_t LANCE_DEFAULT_STORAGE_TIMEOUT_MS = 60000;
inline constexpr uint64_t LANCE_DEFAULT_STORAGE_IO_TIMEOUT_MS = 10000;
inline constexpr uint64_t LANCE_DEFAULT_STORAGE_RETRY_MAX_TIMES = 3;
inline constexpr uint64_t LANCE_DEFAULT_STORAGE_RETRY_MIN_DELAY_MS = 1000;
inline constexpr uint64_t LANCE_DEFAULT_STORAGE_RETRY_MAX_DELAY_MS = 60000;
inline constexpr double LANCE_DEFAULT_STORAGE_RETRY_FACTOR = 2.0;
