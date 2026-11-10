#include "xml_scan.hpp"

#include "arrow_source_scan.hpp"
#include "function_metadata.hpp"
#include "lance_conversion.h"
#include "lance_ffi.hpp"
#include "source_options.hpp"
#include "storage_options.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace {

void ReadPositive(const TableFunctionBindInput &input, const char *name, uint64_t &value) {
	auto entry = input.named_parameters.find(name);
	if (entry == input.named_parameters.end()) {
		return;
	}
	value = entry->second.GetValue<uint64_t>();
	if (value == 0) {
		throw InvalidInputException("read_xml_records %s must be positive", name);
	}
}

unique_ptr<FunctionData> BindXml(ClientContext &context, TableFunctionBindInput &input,
                                 vector<LogicalType> &return_types, vector<Identifier> &names) {
	RejectNullParameters("read_xml_records", "path", input);
	auto path = input.inputs[0].GetValue<string>();
	auto record_path_entry = input.named_parameters.find("record_path");
	if (record_path_entry == input.named_parameters.end()) {
		throw BinderException("read_xml_records requires record_path, such as record_path := '/catalog/book'");
	}
	auto record_path = record_path_entry->second.GetValue<string>();

	// Bindings point into the map value's strings, which outlive lance_xml_open.
	auto options = lance_xml_default_options();
	vector<LanceXmlNamespace> namespaces;
	auto namespaces_entry = input.named_parameters.find("namespaces");
	if (namespaces_entry != input.named_parameters.end()) {
		for (const auto &binding : MapValue::GetChildren(namespaces_entry->second)) {
			auto &key_value = StructValue::GetChildren(binding);
			if (key_value[0].IsNull() || key_value[1].IsNull()) {
				throw BinderException("read_xml_records namespaces must not contain NULL prefixes or URIs");
			}
			namespaces.push_back({StringValue::Get(key_value[0]).c_str(), StringValue::Get(key_value[1]).c_str()});
		}
	}
	options.namespaces = namespaces.data();
	options.namespace_count = namespaces.size();
	ReadPositive(input, "sample_records", options.sample_records);
	ReadPositive(input, "sample_bytes", options.sample_bytes);

	unique_ptr<LanceS3Options> s3;
	if (StringUtil::CIStartsWith(path, "s3://")) {
		s3 = ReadS3Options(context, path);
	} else if (FileSystem::IsRemoteFile(path) && !StringUtil::CIStartsWith(path, "http://") &&
	           !StringUtil::CIStartsWith(path, "https://")) {
		throw BinderException("read_xml_records supports only local, s3://, and http(s):// paths");
	}

	auto opendal_config = ReadOpendalConfig(context);
	SourceStreamFactory *factory = nullptr;
	ThrowIfLanceError(lance_xml_open(path.c_str(), record_path.c_str(), &options, s3 ? &s3->config : nullptr,
	                                 &opendal_config, &factory),
	                  "XML reader");
	return BindSourceStream(context, factory, return_types, names);
}

} // namespace

void RegisterXmlScanFunction(ExtensionLoader &loader) {
	auto function = SourceStreamTableFunction("read_xml_records", BindXml);
	function.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("record_path", LogicalType::VARCHAR);
		options.Add("namespaces", LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR));
		options.Add("sample_records", LogicalType::UBIGINT);
		options.Add("sample_bytes", LogicalType::UBIGINT);
	});
	RegisterTableFunctionWithMetadata(
	    loader, std::move(function),
	    /*parameter_names=*/ {"path", "record_path", "namespaces", "sample_records", "sample_bytes"},
	    /*description=*/
	    "Reads one row per XML element at record_path from a local, S3, or HTTP(S) file, optionally gzipped, "
	    "inferring the schema from the leading records.",
	    /*examples=*/ {"SELECT * FROM read_xml_records('catalog.xml', record_path := '/catalog/book');"},
	    /*categories=*/ {"lance_conversion", "reader"});
}

} // namespace duckdb
