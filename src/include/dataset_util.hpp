#pragma once

#include "duckdb/common/string.hpp"

namespace duckdb {

class ClientContext;

//! Inspect or delete a .lance dataset using the same OpenDAL settings and S3 secret as COPY.
bool LanceDatasetExists(ClientContext &context, const string &path);
void DeleteLanceDataset(ClientContext &context, const string &path);

} // namespace duckdb
