#include "lance_copy.hpp"
#include <memory>
#include "lance_conversion.h"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"

namespace duckdb {
namespace {

struct LanceS3Options {
	string endpoint;
	string region;
	string key_id;
	string secret;
	string session_token;
	bool use_ssl = true;
	bool virtual_host_style = false;
};

LanceS3Options ReadS3Options(ClientContext &context, const string &path) {
	LanceS3Options result;
	KeyValueSecretReader secret_reader(context, "s3", path);
	secret_reader.TryGetSecretKey("key_id", result.key_id);
	secret_reader.TryGetSecretKey("secret", result.secret);
	secret_reader.TryGetSecretKey("session_token", result.session_token);
	secret_reader.TryGetSecretKey("endpoint", result.endpoint);
	secret_reader.TryGetSecretKey("region", result.region);
	secret_reader.TryGetSecretKey("use_ssl", result.use_ssl);
	string url_style;
	secret_reader.TryGetSecretKey("url_style", url_style);
	if (!url_style.empty() && url_style != "path" && url_style != "vhost") {
		throw InvalidConfigurationException("S3 secret url_style must be either 'path' or 'vhost'");
	}
	result.virtual_host_style = url_style == "vhost";
	return result;
}

struct LanceErrorDeleter {
	void operator()(char *error) const {
		lance_conversion_error_free(error);
	}
};

using LanceError = std::unique_ptr<char, LanceErrorDeleter>;

void ThrowIfLanceError(LanceError error) {
	if (error) {
		throw IOException("Lance conversion: %s", error.get());
	}
}

struct LanceBindData : public FunctionData {
	vector<string> names;
	vector<LogicalType> types;
	ClientProperties properties;
	bool overwrite = false;
	bool s3 = false;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<LanceBindData>();
		result->names = names;
		result->types = types;
		result->properties = properties;
		result->overwrite = overwrite;
		result->s3 = s3;
		return std::move(result);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<LanceBindData>();
		return names == other.names && types == other.types && overwrite == other.overwrite && s3 == other.s3;
	}
};

struct LanceGlobalState : public GlobalFunctionData {
	LanceConversionWriter *writer = nullptr;
	~LanceGlobalState() override {
		lance_conversion_destroy(writer);
	}
};

void ValidateDuckDBType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_NS:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ:
		return;
	case LogicalTypeId::LIST:
		ValidateDuckDBType(ListType::GetChildType(type));
		return;
	case LogicalTypeId::ARRAY:
		ValidateDuckDBType(ArrayType::GetChildType(type));
		return;
	case LogicalTypeId::STRUCT:
		for (const auto &child : StructType::GetChildTypes(type)) {
			ValidateDuckDBType(child.second);
		}
		return;
	default:
		throw NotImplementedException("FORMAT LANCE does not support %s; cast it explicitly", type.ToString());
	}
}

unique_ptr<FunctionData> LanceBind(ClientContext &context, CopyFunctionBindInput &input, const vector<string> &names,
                                   const vector<LogicalType> &types) {
	auto result = make_uniq<LanceBindData>();
	result->names = names;
	result->types = types;
	result->properties = context.GetClientProperties();
	result->properties.arrow_use_list_view = false;
	result->properties.produce_arrow_string_view = false;
	result->properties.arrow_lossless_conversion = false;
	result->properties.arrow_offset_size = ArrowOffsetSize::REGULAR;
	result->properties.arrow_output_version = ArrowFormatVersion::V1_0;
	for (const auto &type : types) {
		ValidateDuckDBType(type);
	}
	for (auto &option : input.info.options) {
		if (!StringUtil::CIEquals(option.first, "overwrite")) {
			throw BinderException("Unsupported option for FORMAT LANCE: %s", option.first);
		}
		if (option.second.size() > 1 || (!option.second.empty() && option.second[0].IsNull())) {
			throw BinderException("OVERWRITE requires a boolean");
		}
		result->overwrite =
		    option.second.empty() || option.second[0].CastAs(context, LogicalType::BOOLEAN).GetValue<bool>();
	}
	auto &fs = FileSystem::GetFileSystem(context);
	if (FileSystem::IsRemoteFile(input.info.file_path)) {
		if (!StringUtil::CIStartsWith(input.info.file_path, "s3://")) {
			throw BinderException("FORMAT LANCE supports only local and s3:// destinations");
		}
		result->s3 = true;
		return std::move(result);
	}
	if (fs.FileExists(fs.ExpandPath(input.info.file_path)) ||
	    (!result->overwrite && fs.DirectoryExists(fs.ExpandPath(input.info.file_path)))) {
		throw IOException("Lance destination must not exist: %s", input.info.file_path);
	}
	return std::move(result);
}

unique_ptr<GlobalFunctionData> LanceInitialize(ClientContext &context, FunctionData &bind_p, const string &path) {
	auto &bind = bind_p.Cast<LanceBindData>();
	ArrowSchemaWrapper schema;
	ArrowConverter::ToArrowSchema(&schema.arrow_schema, bind.types, bind.names, bind.properties);
	auto state = make_uniq<LanceGlobalState>();
	LanceS3Config s3;
	const LanceS3Config *s3_ptr = nullptr;
	if (bind.s3) {
		auto options = ReadS3Options(context, path);
		s3.endpoint = options.endpoint.c_str();
		s3.region = options.region.c_str();
		s3.key_id = options.key_id.c_str();
		s3.secret = options.secret.c_str();
		s3.session_token = options.session_token.c_str();
		s3.use_ssl = options.use_ssl ? 1 : 0;
		s3.virtual_host_style = options.virtual_host_style ? 1 : 0;
		s3_ptr = &s3;
		ThrowIfLanceError(LanceError(
		    lance_conversion_open(path.c_str(), &schema.arrow_schema, bind.overwrite ? 1 : 0, s3_ptr, &state->writer)));
		return std::move(state);
	}
	ThrowIfLanceError(LanceError(
	    lance_conversion_open(path.c_str(), &schema.arrow_schema, bind.overwrite ? 1 : 0, s3_ptr, &state->writer)));
	return std::move(state);
}

unique_ptr<LocalFunctionData> LanceLocalInitialize(ExecutionContext &context, FunctionData &bind) {
	return make_uniq<LocalFunctionData>();
}

void LanceSinkChunk(ExecutionContext &context, FunctionData &bind_p, GlobalFunctionData &global_p,
                    LocalFunctionData &local, DataChunk &input) {
	auto &bind = bind_p.Cast<LanceBindData>();
	auto &global = global_p.Cast<LanceGlobalState>();
	ArrowArrayWrapper array;
	ArrowConverter::ToArrowArray(input, &array.arrow_array, bind.properties, {});
	ThrowIfLanceError(LanceError(lance_conversion_push(global.writer, &array.arrow_array)));
}

void LanceFinalize(ClientContext &context, FunctionData &bind, GlobalFunctionData &global_p) {
	ThrowIfLanceError(LanceError(lance_conversion_finish(global_p.Cast<LanceGlobalState>().writer)));
}

CopyFunctionExecutionMode LanceExecutionMode(bool preserve_order, bool supports_batch_index) {
	return CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
}

CopyFunction MakeLanceCopyFunction() {
	CopyFunction function("lance");
	function.extension = "lance";
	function.copy_to_bind = LanceBind;
	function.copy_to_initialize_global = LanceInitialize;
	function.copy_to_initialize_local = LanceLocalInitialize;
	function.copy_to_sink = LanceSinkChunk;
	function.copy_to_finalize = LanceFinalize;
	function.execution_mode = LanceExecutionMode;
	return function;
}

// Own the plan so DuckDB file rotation/overwrite options cannot operate on a dataset directory.
BoundStatement LancePlan(Binder &binder, CopyStatement &statement) {
	auto function = MakeLanceCopyFunction();
	auto query = statement.info->select_statement->Copy();
	auto source = binder.Bind(*query);
	QueryResult::DeduplicateColumns(source.names);
	CopyFunctionBindInput input(*statement.info);
	auto data = LanceBind(binder.context, input, source.names, source.types);
	auto copy = make_uniq<LogicalCopyToFile>(function, std::move(data), statement.info->Copy());
	copy->file_path = statement.info->file_path;
	copy->file_extension = "lance";
	copy->use_tmp_file = false;
	copy->overwrite_mode = CopyOverwriteMode::COPY_ERROR_ON_CONFLICT;
	copy->per_thread_output = false;
	copy->rotate = false;
	copy->partition_output = false;
	copy->write_partition_columns = false;
	copy->return_type = CopyFunctionReturnType::CHANGED_ROWS;
	copy->names = source.names;
	copy->expected_types = source.types;
	copy->AddChild(std::move(source.plan));
	BoundStatement result;
	result.names = GetCopyFunctionReturnNames(copy->return_type);
	result.types = GetCopyFunctionReturnLogicalTypes(copy->return_type);
	result.plan = std::move(copy);
	return result;
}

} // namespace

void RegisterLanceCopyFunction(ExtensionLoader &loader) {
	auto function = MakeLanceCopyFunction();
	function.plan = LancePlan;
	loader.RegisterFunction(function);
}

} // namespace duckdb
