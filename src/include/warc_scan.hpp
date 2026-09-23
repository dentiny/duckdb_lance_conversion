#pragma once

namespace duckdb {
class ExtensionLoader;

void RegisterWarcScanFunction(ExtensionLoader &loader);
} // namespace duckdb
