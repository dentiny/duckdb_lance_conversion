#include "lance_copy.hpp"
#include "lance_ffi.hpp"
#include "storage_options.hpp"
#include "lance_conversion.h"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"

namespace duckdb {
namespace {

struct LanceBindData : public FunctionData {
	vector<string> names;
	vector<LogicalType> types;
	ClientProperties properties;
	LanceWriteMode mode = LANCE_WRITE_MODE_CREATE;
	bool s3 = false;
	int64_t blob_inline_size_threshold = 2 * 1024 * 1024;
	int64_t blob_dedicated_size_threshold = 16 * 1024 * 1024;
	int64_t target_file_size = 512 * 1024 * 1024;
	vector<string> blob_columns;
	vector<string> scalar_index_columns;
	vector<string> vector_index_columns;
	vector<string> text_index_columns;
	vector<string> bloom_filter_index_columns;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<LanceBindData>();
		result->names = names;
		result->types = types;
		result->properties = properties;
		result->mode = mode;
		result->s3 = s3;
		result->blob_inline_size_threshold = blob_inline_size_threshold;
		result->blob_dedicated_size_threshold = blob_dedicated_size_threshold;
		result->target_file_size = target_file_size;
		result->blob_columns = blob_columns;
		result->scalar_index_columns = scalar_index_columns;
		result->vector_index_columns = vector_index_columns;
		result->text_index_columns = text_index_columns;
		result->bloom_filter_index_columns = bloom_filter_index_columns;
		return std::move(result);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<LanceBindData>();
		return names == other.names && types == other.types && mode == other.mode && s3 == other.s3 &&
		       blob_inline_size_threshold == other.blob_inline_size_threshold &&
		       blob_dedicated_size_threshold == other.blob_dedicated_size_threshold &&
		       target_file_size == other.target_file_size && blob_columns == other.blob_columns &&
		       scalar_index_columns == other.scalar_index_columns &&
		       vector_index_columns == other.vector_index_columns && text_index_columns == other.text_index_columns &&
		       bloom_filter_index_columns == other.bloom_filter_index_columns;
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

int64_t ParseSizeOption(ClientContext &context, const string &name, const vector<Value> &values) {
	if (values.size() != 1 || values[0].IsNull()) {
		throw BinderException("%s requires one non-negative integer", name);
	}
	auto value = values[0].CastAs(context, LogicalType::BIGINT).GetValue<int64_t>();
	if (value < 0) {
		throw BinderException("%s must be non-negative", name);
	}
	return value;
}

vector<string> ParseBlobColumns(ClientContext &context, const vector<Value> &values, const vector<string> &names,
                                const vector<LogicalType> &types) {
	if (values.empty()) {
		throw BinderException("BLOB_COLUMNS requires at least one column");
	}
	vector<string> result;
	for (const auto &value : values) {
		if (value.IsNull()) {
			throw BinderException("BLOB_COLUMNS cannot contain NULL");
		}
		auto requested = value.CastAs(context, LogicalType::VARCHAR).GetValue<string>();
		idx_t index;
		for (index = 0; index < names.size(); index++) {
			if (StringUtil::CIEquals(names[index], requested)) {
				break;
			}
		}
		if (index == names.size()) {
			throw BinderException("BLOB_COLUMNS column not found: %s", requested);
		}
		if (types[index].id() != LogicalTypeId::VARCHAR) {
			throw BinderException("BLOB_COLUMNS column must be VARCHAR: %s", names[index]);
		}
		for (const auto &column : result) {
			if (StringUtil::CIEquals(column, names[index])) {
				throw BinderException("Duplicate BLOB_COLUMNS column: %s", names[index]);
			}
		}
		result.push_back(names[index]);
	}
	return result;
}

vector<string> ParseIndexColumns(ClientContext &context, const string &option_name, const vector<Value> &values,
                                 const vector<string> &names) {
	if (values.empty()) {
		throw BinderException("%s requires at least one column", option_name);
	}
	vector<string> result;
	for (const auto &value : values) {
		if (value.IsNull()) {
			throw BinderException("%s cannot contain NULL", option_name);
		}
		auto requested = value.CastAs(context, LogicalType::VARCHAR).GetValue<string>();
		idx_t index;
		for (index = 0; index < names.size(); index++) {
			if (StringUtil::CIEquals(names[index], requested)) {
				break;
			}
		}
		if (index == names.size()) {
			throw BinderException("%s column not found: %s", option_name, requested);
		}
		for (const auto &column : result) {
			if (StringUtil::CIEquals(column, names[index])) {
				throw BinderException("Duplicate index column: %s", names[index]);
			}
		}
		result.push_back(names[index]);
	}
	return result;
}

void ValidateIndexColumns(const LanceBindData &data) {
	vector<string> columns;
	for (const auto *group : {&data.scalar_index_columns, &data.vector_index_columns, &data.text_index_columns,
	                          &data.bloom_filter_index_columns}) {
		for (const auto &column : *group) {
			for (const auto &existing : columns) {
				if (StringUtil::CIEquals(column, existing)) {
					throw BinderException("Column cannot have multiple indexes in one COPY: %s", column);
				}
			}
			for (const auto &blob_column : data.blob_columns) {
				if (StringUtil::CIEquals(column, blob_column)) {
					throw BinderException("BLOB_COLUMNS column cannot also be indexed: %s", column);
				}
			}
			columns.push_back(column);
		}
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
	bool has_write_mode = false;
	for (auto &option : input.info.options) {
		if (StringUtil::CIEquals(option.first, "overwrite") || StringUtil::CIEquals(option.first, "append")) {
			if (has_write_mode) {
				throw BinderException("Only one of OVERWRITE or APPEND can be specified");
			}
			has_write_mode = true;
			if (option.second.size() > 1 || (!option.second.empty() && option.second[0].IsNull())) {
				throw BinderException("%s requires a boolean", option.first);
			}
			auto enabled =
			    option.second.empty() || option.second[0].CastAs(context, LogicalType::BOOLEAN).GetValue<bool>();
			if (enabled) {
				result->mode = StringUtil::CIEquals(option.first, "overwrite") ? LANCE_WRITE_MODE_OVERWRITE
				                                                               : LANCE_WRITE_MODE_APPEND;
			}
		} else if (StringUtil::CIEquals(option.first, "blob_inline_size_threshold")) {
			result->blob_inline_size_threshold = ParseSizeOption(context, option.first, option.second);
		} else if (StringUtil::CIEquals(option.first, "blob_dedicated_size_threshold")) {
			result->blob_dedicated_size_threshold = ParseSizeOption(context, option.first, option.second);
			if (result->blob_dedicated_size_threshold == 0) {
				throw BinderException("BLOB_DEDICATED_SIZE_THRESHOLD must be greater than zero");
			}
		} else if (StringUtil::CIEquals(option.first, "target_file_size")) {
			result->target_file_size = ParseSizeOption(context, option.first, option.second);
			if (result->target_file_size == 0) {
				throw BinderException("TARGET_FILE_SIZE must be greater than zero");
			}
		} else if (StringUtil::CIEquals(option.first, "blob_columns")) {
			result->blob_columns = ParseBlobColumns(context, option.second, names, types);
		} else if (StringUtil::CIEquals(option.first, "scalar_index_columns")) {
			result->scalar_index_columns = ParseIndexColumns(context, option.first, option.second, names);
		} else if (StringUtil::CIEquals(option.first, "vector_index_columns")) {
			result->vector_index_columns = ParseIndexColumns(context, option.first, option.second, names);
		} else if (StringUtil::CIEquals(option.first, "text_index_columns")) {
			result->text_index_columns = ParseIndexColumns(context, option.first, option.second, names);
		} else if (StringUtil::CIEquals(option.first, "bloom_filter_index_columns")) {
			result->bloom_filter_index_columns = ParseIndexColumns(context, option.first, option.second, names);
		} else {
			throw BinderException("Unsupported option for FORMAT LANCE: %s", option.first);
		}
	}
	ValidateIndexColumns(*result);
	auto &fs = FileSystem::GetFileSystem(context);
	if (FileSystem::IsRemoteFile(input.info.file_path)) {
		if (!StringUtil::CIStartsWith(input.info.file_path, "s3://")) {
			throw BinderException("FORMAT LANCE supports only local and s3:// destinations");
		}
		result->s3 = true;
		return std::move(result);
	}
	if (result->mode == LANCE_WRITE_MODE_CREATE && (fs.FileExists(fs.ExpandPath(input.info.file_path)) ||
	                                                fs.DirectoryExists(fs.ExpandPath(input.info.file_path)))) {
		throw IOException("Lance destination must not exist: %s", input.info.file_path);
	}
	return std::move(result);
}

unique_ptr<GlobalFunctionData> LanceInitialize(ClientContext &context, FunctionData &bind_p, const string &path) {
	auto &bind = bind_p.Cast<LanceBindData>();
	ArrowSchemaWrapper schema;
	ArrowConverter::ToArrowSchema(&schema.arrow_schema, bind.types, bind.names, bind.properties);
	auto state = make_uniq<LanceGlobalState>();
	LanceWriteConfig write_config;
	write_config.mode = bind.mode;
	write_config.blob_inline_size_threshold = bind.blob_inline_size_threshold;
	write_config.blob_dedicated_size_threshold = bind.blob_dedicated_size_threshold;
	write_config.target_file_size = bind.target_file_size;
	vector<const char *> blob_columns;
	for (const auto &column : bind.blob_columns) {
		blob_columns.push_back(column.c_str());
	}
	write_config.blob_columns = blob_columns.data();
	write_config.blob_column_count = blob_columns.size();
	auto string_pointers = [](const vector<string> &columns) {
		vector<const char *> result;
		for (const auto &column : columns) {
			result.push_back(column.c_str());
		}
		return result;
	};
	auto scalar_index_columns = string_pointers(bind.scalar_index_columns);
	auto vector_index_columns = string_pointers(bind.vector_index_columns);
	auto text_index_columns = string_pointers(bind.text_index_columns);
	auto bloom_filter_index_columns = string_pointers(bind.bloom_filter_index_columns);
	write_config.scalar_index_columns = scalar_index_columns.data();
	write_config.scalar_index_column_count = scalar_index_columns.size();
	write_config.vector_index_columns = vector_index_columns.data();
	write_config.vector_index_column_count = vector_index_columns.size();
	write_config.text_index_columns = text_index_columns.data();
	write_config.text_index_column_count = text_index_columns.size();
	write_config.bloom_filter_index_columns = bloom_filter_index_columns.data();
	write_config.bloom_filter_index_column_count = bloom_filter_index_columns.size();
	LanceS3Config s3;
	const LanceS3Config *s3_ptr = nullptr;
	LanceS3Options options;
	if (bind.s3) {
		options = ReadS3Options(context, path);
		s3 = options.ToConfig();
		s3_ptr = &s3;
	}
	ThrowIfLanceError(lance_conversion_open(path.c_str(), &schema.arrow_schema, &write_config, s3_ptr, &state->writer),
	                  "Lance conversion");
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
	ThrowIfLanceError(lance_conversion_push(global.writer, &array.arrow_array), "Lance conversion");
}

void LanceFinalize(ClientContext &context, FunctionData &bind, GlobalFunctionData &global_p) {
	ThrowIfLanceError(lance_conversion_finish(global_p.Cast<LanceGlobalState>().writer), "Lance conversion");
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
