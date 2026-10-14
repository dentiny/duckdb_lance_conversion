#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "lance_conversion.h"

namespace duckdb {

class ClientContext;
class ExtensionLoader;

void RegisterOpendalSettings(ExtensionLoader &loader);
LanceOpendalConfig ReadOpendalConfig(ClientContext &context);

struct LanceS3Options {
	string endpoint;
	string region;
	string key_id;
	string secret;
	string session_token;
	bool use_ssl = true;
	bool virtual_host_style = false;
	// Points into the strings above.
	LanceS3Config config;
};

unique_ptr<LanceS3Options> ReadS3Options(ClientContext &context, const string &path);

} // namespace duckdb
