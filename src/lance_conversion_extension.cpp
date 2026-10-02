#define DUCKDB_EXTENSION_MAIN

#include "lance_conversion_extension.hpp"
#include "huggingface_scan.hpp"
#include "lance_copy.hpp"
#include "warc_scan.hpp"

namespace duckdb {

static void LoadLanceConversion(ExtensionLoader &loader) {
	loader.SetDescription("Converts many data formats to Lance, including Parquet, Hugging Face datasets, and WARC.");
	RegisterLanceCopyFunction(loader);
	RegisterHuggingFaceScanFunction(loader);
	RegisterWarcScanFunction(loader);
}

void LanceConversionExtension::Load(ExtensionLoader &loader) {
	LoadLanceConversion(loader);
}
std::string LanceConversionExtension::Name() {
	return "lance_conversion";
}
std::string LanceConversionExtension::Version() const {
#ifdef EXT_VERSION_LANCE_CONVERSION
	return EXT_VERSION_LANCE_CONVERSION;
#else
	return "0.1.2";
#endif
}
} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(lance_conversion, loader) {
	duckdb::LoadLanceConversion(loader);
}
}
