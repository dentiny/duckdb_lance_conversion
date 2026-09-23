#include "warc_scan.hpp"

#include "function_metadata.hpp"
#include "lance_conversion.h"
#include "lance_ffi.hpp"
#include "storage_options.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/external_dependencies.hpp"

namespace duckdb {
namespace {

class WarcDependency : public DependencyItem {
public:
	explicit WarcDependency(WarcStreamFactory *factory_p) : factory(factory_p) {
	}

	~WarcDependency() override {
		lance_warc_destroy(factory);
	}

	WarcStreamFactory *factory;
};

unique_ptr<ArrowArrayStreamWrapper> ProduceWarcStream(uintptr_t factory_ptr, ArrowStreamParameters &parameters) {
	auto result = make_uniq<ArrowArrayStreamWrapper>();
	vector<const char *> columns;
	columns.reserve(parameters.projected_columns.columns.size());
	for (const auto &column : parameters.projected_columns.columns) {
		columns.push_back(column.c_str());
	}
	ThrowIfLanceError(lance_warc_get_stream(reinterpret_cast<WarcStreamFactory *>(factory_ptr), columns.data(),
	                                        columns.size(), &result->arrow_array_stream),
	                  "WARC reader");
	return result;
}

unique_ptr<FunctionData> BindWarc(ClientContext &context, TableFunctionBindInput &input,
                                  vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull()) {
		throw BinderException("read_warc path must not be NULL");
	}
	auto path = input.inputs[0].GetValue<string>();
	LanceS3Config s3;
	const LanceS3Config *s3_ptr = nullptr;
	LanceS3Options options;
	if (FileSystem::IsRemoteFile(path)) {
		if (!StringUtil::CIStartsWith(path, "s3://")) {
			throw BinderException("read_warc supports only local and s3:// paths");
		}
		options = ReadS3Options(context, path);
		s3 = options.ToConfig();
		s3_ptr = &s3;
	}

	WarcStreamFactory *factory = nullptr;
	ThrowIfLanceError(lance_warc_open(path.c_str(), s3_ptr, &factory), "WARC reader");
	auto dependency = make_shared_ptr<WarcDependency>(factory);
	auto result = make_uniq<ArrowScanFunctionData>(ProduceWarcStream, reinterpret_cast<uintptr_t>(factory), dependency);
	ThrowIfLanceError(lance_warc_get_schema(factory, &result->schema_root.arrow_schema), "WARC reader");
	ArrowTableFunction::PopulateArrowTableSchema(context, result->arrow_table, result->schema_root.arrow_schema);
	names = result->arrow_table.GetNames();
	return_types = result->arrow_table.GetTypes();
	result->all_types = return_types;
	return std::move(result);
}

} // namespace

void RegisterWarcScanFunction(ExtensionLoader &loader) {
	TableFunction function("read_warc", {LogicalType::VARCHAR}, ArrowTableFunction::ArrowScanFunction, BindWarc,
	                       ArrowTableFunction::ArrowScanInitGlobal, ArrowTableFunction::ArrowScanInitLocal);
	function.projection_pushdown = true;
	RegisterTableFunctionWithMetadata(loader, std::move(function),
	                                  /*parameter_names=*/ {"path"},
	                                  /*description=*/"Reads records from a local or S3 WARC file.",
	                                  /*examples=*/ {"SELECT * FROM read_warc('archive.warc.gz');"},
	                                  /*categories=*/ {"lance_conversion", "reader"});
}

} // namespace duckdb
