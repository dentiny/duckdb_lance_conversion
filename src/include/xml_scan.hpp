#pragma once

namespace duckdb {
class ExtensionLoader;

void RegisterXmlScanFunction(ExtensionLoader &loader);
} // namespace duckdb
