#define DUCKDB_EXTENSION_MAIN

#include "lance_conversion_extension.hpp"
#include "lance_copy.hpp"

namespace duckdb {

void LanceConversionExtension::Load(ExtensionLoader &loader) {
	RegisterLanceCopyFunction(loader);
}
std::string LanceConversionExtension::Name() {
	return "lance_conversion";
}
std::string LanceConversionExtension::Version() const {
#ifdef EXT_VERSION_LANCE_CONVERSION
	return EXT_VERSION_LANCE_CONVERSION;
#else
	return "0.1.0";
#endif
}
} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(lance_conversion, loader) {
	duckdb::RegisterLanceCopyFunction(loader);
}
}
