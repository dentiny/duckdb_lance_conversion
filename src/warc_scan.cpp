#include "warc_scan.hpp"

#include "arrow_source_scan.hpp"
#include "function_metadata.hpp"
#include "lance_conversion.h"
#include "lance_ffi.hpp"
#include "source_options.hpp"
#include "storage_options.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace {

unique_ptr<FunctionData> BindWarc(ClientContext &context, TableFunctionBindInput &input,
                                  vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull()) {
		throw BinderException("read_warc path must not be NULL");
	}
	auto path = input.inputs[0].GetValue<string>();
	auto read_options = SourceReadOptions::From(input);
	string index_path;
	auto index_path_entry = input.named_parameters.find("index_path");
	if (index_path_entry != input.named_parameters.end()) {
		index_path = index_path_entry->second.GetValue<string>();
	}
	LanceS3Config s3;
	const LanceS3Config *s3_ptr = nullptr;
	LanceS3Options options;
	auto credential_path = FileSystem::IsRemoteFile(path) ? path : index_path;
	if (FileSystem::IsRemoteFile(credential_path)) {
		if (!StringUtil::CIStartsWith(credential_path, "s3://")) {
			throw BinderException("read_warc supports only local and s3:// paths");
		}
		options = ReadS3Options(context, credential_path);
		s3 = options.ToConfig();
		s3_ptr = &s3;
	}

	auto opendal_config = ReadOpendalConfig(context);
	SourceStreamFactory *factory = nullptr;
	ThrowIfLanceError(lance_warc_open(path.c_str(), index_path.empty() ? nullptr : index_path.c_str(), s3_ptr,
	                                  &opendal_config, read_options.max_read_parallelism, &factory),
	                  "WARC reader");
	return BindSourceStream(context, factory, return_types, names);
}

} // namespace

void RegisterWarcScanFunction(ExtensionLoader &loader) {
	auto function = SourceStreamTableFunction("read_warc", BindWarc);
	function.named_parameters["index_path"] = LogicalType::VARCHAR;
	SourceReadOptions::Register(function);
	RegisterTableFunctionWithMetadata(loader, std::move(function),
	                                  /*parameter_names=*/ {"path", "index_path", "max_read_parallelism"},
	                                  /*description=*/"Reads records from a local or S3 WARC file.",
	                                  /*examples=*/ {"SELECT * FROM read_warc('archive.warc.gz');"},
	                                  /*categories=*/ {"lance_conversion", "reader"});
}

} // namespace duckdb
