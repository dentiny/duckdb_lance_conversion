#pragma once

namespace duckdb {
class ExtensionLoader;

void RegisterLanceCopyFunction(ExtensionLoader &loader);
} // namespace duckdb
