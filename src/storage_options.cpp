#include "storage_options.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/secret/secret.hpp"

#include <cmath>
#include <limits>

namespace duckdb {

namespace {

struct OpendalIntegerSetting {
	const char *name;
	const char *description;
	uint64_t LanceOpendalConfig::*member;
	bool allow_zero;
};

const OpendalIntegerSetting OPENDAL_INTEGER_SETTINGS[] = {
    {"lance_conversion_storage_timeout_ms", "Storage control operation timeout per read/write attempt, in milliseconds",
     &LanceOpendalConfig::timeout_ms, false},
    {"lance_conversion_storage_io_timeout_ms",
     "Storage IO operation and body method timeout per read/write attempt, in milliseconds",
     &LanceOpendalConfig::io_timeout_ms, false},
    {"lance_conversion_storage_retry_max_times",
     "Maximum storage retries for reads and writes after the initial attempt (0 disables retries)",
     &LanceOpendalConfig::retry_max_times, true},
    {"lance_conversion_storage_retry_min_delay_ms",
     "Initial storage retry backoff for reads and writes, in milliseconds", &LanceOpendalConfig::retry_min_delay_ms,
     false},
    {"lance_conversion_storage_retry_max_delay_ms",
     "Maximum storage exponential retry backoff for reads and writes, in milliseconds",
     &LanceOpendalConfig::retry_max_delay_ms, false},
};

void SetPositiveOpendalDuration(ClientContext &, SetScope, Value &value) {
	if (value.IsNull() || value.GetValue<int64_t>() <= 0) {
		throw InvalidInputException("Storage timeout and retry delay settings must be positive");
	}
}

void SetOpendalRetryCount(ClientContext &, SetScope, Value &value) {
	if (value.IsNull() || value.GetValue<int64_t>() < 0) {
		throw InvalidInputException("Storage retry_max_times must be non-negative");
	}
}

void SetOpendalRetryFactor(ClientContext &, SetScope, Value &value) {
	auto factor = value.IsNull() ? 0 : value.GetValue<double>();
	if (!std::isfinite(factor) || factor < 1 || factor > std::numeric_limits<float>::max()) {
		throw InvalidInputException("Storage retry_factor must be finite and between 1 and f32::MAX");
	}
}

Value GetOpendalSetting(ClientContext &context, const char *name) {
	Value value;
	if (!context.TryGetCurrentSetting(name, value)) {
		throw InternalException("Missing storage setting: %s", name);
	}
	return value;
}

} // namespace

void RegisterOpendalSettings(ExtensionLoader &loader) {
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	const auto defaults = lance_opendal_default_config();
	for (const auto &setting : OPENDAL_INTEGER_SETTINGS) {
		config.AddExtensionOption(setting.name, setting.description, LogicalType::BIGINT,
		                          Value::BIGINT(defaults.*setting.member),
		                          setting.allow_zero ? SetOpendalRetryCount : SetPositiveOpendalDuration);
	}
	config.AddExtensionOption("lance_conversion_storage_retry_factor",
	                          "Storage exponential retry backoff multiplier for reads and writes", LogicalType::DOUBLE,
	                          Value::DOUBLE(defaults.retry_factor), SetOpendalRetryFactor);
}

// Source readers and Lance COPY writers share the current connection's policy.
LanceOpendalConfig ReadOpendalConfig(ClientContext &context) {
	auto result = lance_opendal_default_config();
	for (const auto &setting : OPENDAL_INTEGER_SETTINGS) {
		result.*setting.member = GetOpendalSetting(context, setting.name).GetValue<uint64_t>();
	}
	result.retry_factor = GetOpendalSetting(context, "lance_conversion_storage_retry_factor").GetValue<double>();
	if (result.retry_max_delay_ms < result.retry_min_delay_ms) {
		throw InvalidInputException("lance_conversion_storage_retry_max_delay_ms must be at least retry_min_delay_ms");
	}
	return result;
}

LanceS3Config LanceS3Options::ToConfig() const {
	LanceS3Config result;
	result.endpoint = endpoint.c_str();
	result.region = region.c_str();
	result.key_id = key_id.c_str();
	result.secret = secret.c_str();
	result.session_token = session_token.c_str();
	result.use_ssl = use_ssl ? 1 : 0;
	result.virtual_host_style = virtual_host_style ? 1 : 0;
	return result;
}

LanceS3Options ReadS3Options(ClientContext &context, const string &path) {
	LanceS3Options result;
	KeyValueSecretReader secret_reader(*context.db, "s3", path);
	secret_reader.TryGetSecretKey("key_id", result.key_id);
	secret_reader.TryGetSecretKey("secret", result.secret);
	secret_reader.TryGetSecretKey("session_token", result.session_token);
	secret_reader.TryGetSecretKey("endpoint", result.endpoint);
	secret_reader.TryGetSecretKey("region", result.region);
	secret_reader.TryGetSecretKey("use_ssl", result.use_ssl);
	string url_style;
	secret_reader.TryGetSecretKey("url_style", url_style);
	if (!url_style.empty() && url_style != "path" && url_style != "vhost") {
		throw InvalidConfigurationException("S3 secret url_style must be either 'path' or 'vhost'");
	}
	result.virtual_host_style = url_style == "vhost";
	return result;
}

} // namespace duckdb
