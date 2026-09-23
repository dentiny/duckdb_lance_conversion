#include "storage_options.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/secret/secret.hpp"

namespace duckdb {

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
