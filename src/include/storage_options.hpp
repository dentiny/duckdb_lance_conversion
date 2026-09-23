#pragma once

#include "duckdb/common/string.hpp"
#include "lance_conversion.h"

namespace duckdb {

class ClientContext;

struct LanceS3Options {
	string endpoint;
	string region;
	string key_id;
	string secret;
	string session_token;
	bool use_ssl = true;
	bool virtual_host_style = false;

	LanceS3Config ToConfig() const;
};

LanceS3Options ReadS3Options(ClientContext &context, const string &path);

} // namespace duckdb
