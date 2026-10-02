#include "lance_copy.hpp"
#include "lance_ffi.hpp"
#include "storage_options.hpp"
#include "lance_conversion.h"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/parsed_data/sample_options.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"

#include <cmath>

namespace duckdb {
namespace {

struct LanceBindData : public FunctionData {
	// Source schema
	vector<string> names;
	vector<LogicalType> types;
	ClientProperties properties;
	// Write mode and destination
	LanceWriteMode mode = LANCE_WRITE_MODE_CREATE;
	bool s3 = false;
	// Storage configuration
	int64_t blob_inline_size_threshold = 2 * 1024 * 1024;
	int64_t blob_dedicated_size_threshold = 16 * 1024 * 1024;
	int64_t target_file_size = 512 * 1024 * 1024;
	string storage_version;
	// Column compression
	vector<string> compression_columns;
	vector<string> compression_algorithms;
	// Blob columns
	vector<string> blob_columns;
	// Index columns
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
		result->storage_version = storage_version;
		result->compression_columns = compression_columns;
		result->compression_algorithms = compression_algorithms;
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
		       target_file_size == other.target_file_size && storage_version == other.storage_version &&
		       compression_columns == other.compression_columns &&
		       compression_algorithms == other.compression_algorithms && blob_columns == other.blob_columns &&
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

using ColumnIndexMap = case_insensitive_map_t<idx_t>;

ColumnIndexMap BuildColumnIndexMap(const vector<string> &names) {
	ColumnIndexMap result;
	for (idx_t index = 0; index < names.size(); index++) {
		result.emplace(names[index], index);
	}
	return result;
}

vector<string> ParseBlobColumns(ClientContext &context, const vector<Value> &values, const vector<string> &names,
                                const vector<LogicalType> &types, const ColumnIndexMap &column_indexes) {
	if (values.empty()) {
		throw BinderException("BLOB_COLUMNS requires at least one column");
	}
	vector<string> result;
	case_insensitive_set_t seen;
	for (const auto &value : values) {
		if (value.IsNull()) {
			throw BinderException("BLOB_COLUMNS cannot contain NULL");
		}
		auto requested = value.CastAs(context, LogicalType::VARCHAR).GetValue<string>();
		auto entry = column_indexes.find(requested);
		if (entry == column_indexes.end()) {
			throw BinderException("BLOB_COLUMNS column not found: %s", requested);
		}
		auto index = entry->second;
		if (types[index].id() != LogicalTypeId::VARCHAR) {
			throw BinderException("BLOB_COLUMNS column must be VARCHAR: %s", names[index]);
		}
		if (!seen.insert(names[index]).second) {
			throw BinderException("Duplicate BLOB_COLUMNS column: %s", names[index]);
		}
		result.push_back(names[index]);
	}
	return result;
}

vector<string> ParseIndexColumns(ClientContext &context, const string &option_name, const vector<Value> &values,
                                 const vector<string> &names, const ColumnIndexMap &column_indexes) {
	if (values.empty()) {
		throw BinderException("%s requires at least one column", option_name);
	}
	vector<string> result;
	case_insensitive_set_t seen;
	for (const auto &value : values) {
		if (value.IsNull()) {
			throw BinderException("%s cannot contain NULL", option_name);
		}
		auto requested = value.CastAs(context, LogicalType::VARCHAR).GetValue<string>();
		auto entry = column_indexes.find(requested);
		if (entry == column_indexes.end()) {
			throw BinderException("%s column not found: %s", option_name, requested);
		}
		auto index = entry->second;
		if (!seen.insert(names[index]).second) {
			throw BinderException("Duplicate index column: %s", names[index]);
		}
		result.push_back(names[index]);
	}
	return result;
}

void ParseColumnCompression(ClientContext &context, const vector<Value> &values, const vector<string> &names,
                            const ColumnIndexMap &column_indexes, LanceBindData &result) {
	if (values.size() != 1 || values[0].type().id() != LogicalTypeId::STRUCT || values[0].IsNull()) {
		throw BinderException("COLUMN_COMPRESSION requires a struct such as {'column': 'zstd'}");
	}
	auto &entries = StructType::GetChildTypes(values[0].type());
	auto &algorithms = StructValue::GetChildren(values[0]);
	case_insensitive_set_t seen(result.compression_columns.begin(), result.compression_columns.end());
	for (idx_t i = 0; i < entries.size(); i++) {
		const auto &requested = entries[i].first;
		auto entry = column_indexes.find(requested);
		if (entry == column_indexes.end()) {
			throw BinderException("COLUMN_COMPRESSION column not found: %s", requested);
		}
		auto index = entry->second;
		if (!seen.insert(names[index]).second) {
			throw BinderException("Duplicate COLUMN_COMPRESSION column: %s", names[index]);
		}
		if (algorithms[i].IsNull()) {
			throw BinderException("COLUMN_COMPRESSION algorithm cannot be NULL: %s", names[index]);
		}
		result.compression_columns.push_back(names[index]);
		result.compression_algorithms.push_back(algorithms[i].CastAs(context, LogicalType::VARCHAR).GetValue<string>());
	}
}

void ValidateIndexColumns(const LanceBindData &data) {
	case_insensitive_set_t blob_columns(data.blob_columns.begin(), data.blob_columns.end());
	case_insensitive_set_t indexed;
	for (const auto *group : {&data.scalar_index_columns, &data.vector_index_columns, &data.text_index_columns,
	                          &data.bloom_filter_index_columns}) {
		for (const auto &column : *group) {
			if (!indexed.insert(column).second) {
				throw BinderException("Column cannot have multiple indexes in one COPY: %s", column);
			}
			if (blob_columns.count(column)) {
				throw BinderException("BLOB_COLUMNS column cannot also be indexed: %s", column);
			}
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
	auto column_indexes = BuildColumnIndexMap(names);
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
		} else if (StringUtil::CIEquals(option.first, "storage_version")) {
			if (option.second.size() != 1 || option.second[0].IsNull()) {
				throw BinderException("STORAGE_VERSION requires one version string");
			}
			result->storage_version = option.second[0].CastAs(context, LogicalType::VARCHAR).GetValue<string>();
		} else if (StringUtil::CIEquals(option.first, "column_compression")) {
			ParseColumnCompression(context, option.second, names, column_indexes, *result);
		} else if (StringUtil::CIEquals(option.first, "blob_columns")) {
			result->blob_columns = ParseBlobColumns(context, option.second, names, types, column_indexes);
		} else if (StringUtil::CIEquals(option.first, "scalar_index_columns")) {
			result->scalar_index_columns =
			    ParseIndexColumns(context, option.first, option.second, names, column_indexes);
		} else if (StringUtil::CIEquals(option.first, "vector_index_columns")) {
			result->vector_index_columns =
			    ParseIndexColumns(context, option.first, option.second, names, column_indexes);
		} else if (StringUtil::CIEquals(option.first, "text_index_columns")) {
			result->text_index_columns = ParseIndexColumns(context, option.first, option.second, names, column_indexes);
		} else if (StringUtil::CIEquals(option.first, "bloom_filter_index_columns")) {
			result->bloom_filter_index_columns =
			    ParseIndexColumns(context, option.first, option.second, names, column_indexes);
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
	write_config.storage_version = bind.storage_version.empty() ? nullptr : bind.storage_version.c_str();
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
	auto compression_columns = string_pointers(bind.compression_columns);
	auto compression_algorithms = string_pointers(bind.compression_algorithms);
	write_config.compression_columns = compression_columns.data();
	write_config.compression_algorithms = compression_algorithms.data();
	write_config.compression_column_count = compression_columns.size();
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
	auto writer = global_p.Cast<LanceGlobalState>().writer;
	ThrowIfLanceError(lance_conversion_finish(writer), "Lance conversion");
	LanceWriteMetrics metrics;
	lance_conversion_get_metrics(writer, &metrics);
	auto &profiler = QueryProfiler::Get(context);
	profiler.AddToCounter(MetricType::TOTAL_BYTES_READ, metrics.bytes_read);
	profiler.AddToCounter(MetricType::TOTAL_BYTES_WRITTEN, metrics.bytes_written);
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

enum class SampleKind : uint8_t { PERCENT, ROWS };

struct SampleOptionSpec {
	const char *name;
	SampleKind kind;
};

constexpr SampleOptionSpec SAMPLE_OPTION_SPECS[] = {
    {"sample_percent", SampleKind::PERCENT},
    {"sample_rows", SampleKind::ROWS},
};

Value ParseSampleSize(ClientContext &context, SampleKind kind, const vector<Value> &values) {
	switch (kind) {
	case SampleKind::PERCENT: {
		auto value = values[0].CastAs(context, LogicalType::DOUBLE).GetValue<double>();
		if (std::isnan(value) || value < 0 || value > 100) {
			throw InvalidInputException("SAMPLE_PERCENT must be between 0 and 100");
		}
		return Value::DOUBLE(value);
	}
	case SampleKind::ROWS: {
		auto value = values[0].CastAs(context, LogicalType::BIGINT).GetValue<int64_t>();
		if (value < 0) {
			throw InvalidInputException("SAMPLE_ROWS must be non-negative");
		}
		if (NumericCast<idx_t>(value) > SampleOptions::MAX_SAMPLE_ROWS) {
			throw InvalidInputException("SAMPLE_ROWS must not exceed %llu", SampleOptions::MAX_SAMPLE_ROWS);
		}
		return Value::BIGINT(value);
	}
	}
	throw InternalException("Unknown sample kind");
}

SampleMethod GetSampleMethod(SampleKind kind) {
	switch (kind) {
	case SampleKind::PERCENT:
		return SampleMethod::BERNOULLI_SAMPLE;
	case SampleKind::ROWS:
		return SampleMethod::RESERVOIR_SAMPLE;
	}
	throw InternalException("Unknown sample kind");
}

// Removes SAMPLE_PERCENT / SAMPLE_ROWS from the COPY options and returns the equivalent DuckDB sample.
unique_ptr<SampleOptions> TakeSampleOptions(ClientContext &context, CopyInfo &info) {
	unique_ptr<SampleOptions> sample;
	for (const auto &spec : SAMPLE_OPTION_SPECS) {
		auto entry = info.options.find(spec.name);
		if (entry == info.options.end()) {
			continue;
		}
		if (sample) {
			throw BinderException("Only one of SAMPLE_PERCENT or SAMPLE_ROWS can be specified");
		}
		if (entry->second.size() != 1 || entry->second[0].IsNull()) {
			throw BinderException("%s requires one value", StringUtil::Upper(spec.name));
		}
		sample = make_uniq<SampleOptions>();
		sample->sample_size = ParseSampleSize(context, spec.kind, entry->second);
		sample->is_percentage = spec.kind == SampleKind::PERCENT;
		sample->method = GetSampleMethod(spec.kind);
		info.options.erase(entry);
	}
	return sample;
}

unique_ptr<QueryNode> ApplySample(unique_ptr<QueryNode> query, unique_ptr<SampleOptions> sample) {
	auto subquery = make_uniq<SelectStatement>();
	subquery->node = std::move(query);
	auto result = make_uniq<SelectNode>();
	result->select_list.push_back(make_uniq<StarExpression>());
	result->from_table = make_uniq<SubqueryRef>(std::move(subquery));
	result->sample = std::move(sample);
	return std::move(result);
}

// Own the plan so DuckDB file rotation/overwrite options cannot operate on a dataset directory.
BoundStatement LancePlan(Binder &binder, CopyStatement &statement) {
	auto function = MakeLanceCopyFunction();
	auto info = statement.info->Copy();
	auto query = info->select_statement->Copy();
	auto sample = TakeSampleOptions(binder.context, *info);
	if (sample) {
		query = ApplySample(std::move(query), std::move(sample));
	}
	auto source = binder.Bind(*query);
	QueryResult::DeduplicateColumns(source.names);
	CopyFunctionBindInput input(*info);
	auto data = LanceBind(binder.context, input, source.names, source.types);
	auto copy = make_uniq<LogicalCopyToFile>(function, std::move(data), std::move(info));
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
