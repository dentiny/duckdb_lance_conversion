#include "atif_scan.hpp"

#include "arrow_source_scan.hpp"
#include "function_metadata.hpp"
#include "lance_conversion.h"
#include "lance_ffi.hpp"
#include "storage_options.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace {

unique_ptr<FunctionData> BindAtif(ClientContext &context, TableFunctionBindInput &input,
                                  vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull()) {
		throw BinderException("read_atif path must not be NULL");
	}
	auto path = input.inputs[0].GetValue<string>();
	string format;
	auto format_entry = input.named_parameters.find("format");
	if (format_entry != input.named_parameters.end()) {
		format = format_entry->second.GetValue<string>();
	}
	LanceS3Config s3;
	const LanceS3Config *s3_ptr = nullptr;
	LanceS3Options options;
	if (FileSystem::IsRemoteFile(path)) {
		if (!StringUtil::CIStartsWith(path, "s3://")) {
			throw BinderException("read_atif supports only local and s3:// paths");
		}
		options = ReadS3Options(context, path);
		s3 = options.ToConfig();
		s3_ptr = &s3;
	}

	auto opendal_config = ReadOpendalConfig(context);
	SourceStreamFactory *factory = nullptr;
	ThrowIfLanceError(
	    lance_atif_open(path.c_str(), format.empty() ? nullptr : format.c_str(), s3_ptr, &opendal_config, &factory),
	    "ATIF reader");
	return BindSourceStream(context, factory, return_types, names);
}

} // namespace

void RegisterAtifScanFunction(ExtensionLoader &loader) {
	auto function = SourceStreamTableFunction("read_atif", BindAtif);
	function.named_parameters["format"] = LogicalType::VARCHAR;
	RegisterTableFunctionWithMetadata(
	    loader, std::move(function),
	    /*parameter_names=*/ {"path", "format"},
	    /*description=*/"Reads ATIF agent trajectories from a local or S3 JSON or JSONL file.",
	    /*examples=*/ {"SELECT * FROM read_atif('trajectories.jsonl');"},
	    /*categories=*/ {"lance_conversion", "reader"});
}

} // namespace duckdb
