#pragma once

namespace duckdb {
class ExtensionLoader;

void RegisterHuggingFaceScanFunction(ExtensionLoader &loader);
} // namespace duckdb
