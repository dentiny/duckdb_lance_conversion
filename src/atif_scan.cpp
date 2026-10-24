#include "atif_scan.hpp"

#include "arrow_source_scan.hpp"
#include "function_metadata.hpp"
#include "lance_conversion.h"
#include "lance_ffi.hpp"
#include "source_options.hpp"
#include "storage_options.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {
namespace {

unique_ptr<FunctionData> BindAtif(ClientContext &context, TableFunctionBindInput &input,
                                  vector<LogicalType> &return_types, vector<Identifier> &names) {
	RejectNullParameters("read_atif", "path", input);
	// Trajectories must keep their file order, which DuckDB may otherwise reorder across threads.
	if (!Settings::Get<PreserveInsertionOrderSetting>(context)) {
		throw BinderException("read_atif requires preserve_insertion_order = true");
	}
	auto path = input.inputs[0].GetValue<string>();
	string format;
	auto format_entry = input.named_parameters.find("format");
	if (format_entry != input.named_parameters.end()) {
		format = format_entry->second.GetValue<string>();
	}
	unique_ptr<LanceS3Options> s3;
	if (StringUtil::CIStartsWith(path, "s3://")) {
		s3 = ReadS3Options(context, path);
	} else if (FileSystem::IsRemoteFile(path) && !StringUtil::CIStartsWith(path, "http://") &&
	           !StringUtil::CIStartsWith(path, "https://")) {
		throw BinderException("read_atif supports only local, s3://, and http(s):// paths");
	}

	auto opendal_config = ReadOpendalConfig(context);
	SourceStreamFactory *factory = nullptr;
	ThrowIfLanceError(lance_atif_open(path.c_str(), format.empty() ? nullptr : format.c_str(),
	                                  s3 ? &s3->config : nullptr, &opendal_config, &factory),
	                  "ATIF reader");
	return BindSourceStream(context, factory, return_types, names);
}

} // namespace

void RegisterAtifScanFunction(ExtensionLoader &loader) {
	auto function = SourceStreamTableFunction("read_atif", BindAtif);
	function.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("format", LogicalType::VARCHAR);
	});
	RegisterTableFunctionWithMetadata(
	    loader, std::move(function),
	    /*parameter_names=*/ {"path", "format"},
	    /*description=*/
	    "Reads ATIF agent trajectories from a local, S3, or HTTP(S) JSON or JSONL file, optionally gzipped, embedding "
	    "referenced media.",
	    /*examples=*/ {"SELECT * FROM read_atif('trajectories.jsonl');"},
	    /*categories=*/ {"lance_conversion", "reader"});
}

} // namespace duckdb
