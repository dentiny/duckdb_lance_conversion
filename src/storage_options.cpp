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

constexpr auto STORAGE_SETTING_PREFIX = "lance_conversion_storage_";

template <bool ALLOW_ZERO>
void SetStorageInteger(ClientContext &, SetScope, Value &value) {
	if (value.IsNull() || value.GetValue<int64_t>() < (ALLOW_ZERO ? 0 : 1)) {
		throw InvalidInputException(ALLOW_ZERO ? "Storage retry_max_times must be non-negative"
		                                       : "Storage timeout and retry delay settings must be positive");
	}
}

void SetStorageRetryFactor(ClientContext &, SetScope, Value &value) {
	auto factor = value.IsNull() ? 0 : value.GetValue<double>();
	if (!std::isfinite(factor) || factor < 1 || factor > std::numeric_limits<float>::max()) {
		throw InvalidInputException("Storage retry_factor must be finite and between 1 and f32::MAX");
	}
}

template <typename T>
T ReadStorageSetting(ClientContext &context, const char *suffix) {
	auto name = string(STORAGE_SETTING_PREFIX) + suffix;
	Value value;
	if (!context.TryGetCurrentSetting(name, value)) {
		throw InternalException("Missing storage setting: %s", name);
	}
	return value.GetValue<T>();
}

} // namespace

void RegisterOpendalSettings(ExtensionLoader &loader) {
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	const auto defaults = lance_opendal_default_config();
	auto register_integer = [&](const char *name, const char *description, uint64_t value, bool allow_zero = false) {
		config.AddExtensionOption(string(STORAGE_SETTING_PREFIX) + name, description, LogicalType::BIGINT,
		                          Value::BIGINT(value),
		                          allow_zero ? SetStorageInteger<true> : SetStorageInteger<false>);
	};
	register_integer("timeout_ms", "Control operation timeout per attempt (ms)", defaults.timeout_ms);
	register_integer("io_timeout_ms", "IO operation and body method timeout per attempt (ms)", defaults.io_timeout_ms);
	register_integer("retry_max_times", "Maximum retries after the initial attempt (0 disables retries)",
	                 defaults.retry_max_times, true);
	register_integer("retry_min_delay_ms", "Initial exponential retry backoff (ms)", defaults.retry_min_delay_ms);
	register_integer("retry_max_delay_ms", "Maximum exponential retry backoff (ms)", defaults.retry_max_delay_ms);
	config.AddExtensionOption(string(STORAGE_SETTING_PREFIX) + "retry_factor",
	                          "Storage exponential retry backoff multiplier for reads and writes", LogicalType::DOUBLE,
	                          Value::DOUBLE(defaults.retry_factor), SetStorageRetryFactor);
}

// Source readers and Lance COPY writers share the current connection's policy.
LanceOpendalConfig ReadOpendalConfig(ClientContext &context) {
	LanceOpendalConfig result {
	    ReadStorageSetting<uint64_t>(context, "timeout_ms"),
	    ReadStorageSetting<uint64_t>(context, "io_timeout_ms"),
	    ReadStorageSetting<uint64_t>(context, "retry_max_times"),
	    ReadStorageSetting<uint64_t>(context, "retry_min_delay_ms"),
	    ReadStorageSetting<uint64_t>(context, "retry_max_delay_ms"),
	    ReadStorageSetting<double>(context, "retry_factor"),
	};
	if (result.retry_max_delay_ms < result.retry_min_delay_ms) {
		throw InvalidInputException("lance_conversion_storage_retry_max_delay_ms must be at least retry_min_delay_ms");
	}
	return result;
}

LanceS3Config LanceS3Options::ToConfig() const {
	return {endpoint.c_str(),
	        region.c_str(),
	        key_id.c_str(),
	        secret.c_str(),
	        session_token.c_str(),
	        use_ssl ? 1 : 0,
	        virtual_host_style ? 1 : 0};
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
