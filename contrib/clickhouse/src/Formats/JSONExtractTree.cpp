#include <Formats/JSONExtractTree.h>

#if USE_SIMDJSON
#include <Common/JSONParsers/SimdJSONParser.h>
#endif
#if USE_RAPIDJSON
#include <Common/JSONParsers/RapidJSONParser.h>
#endif
#include <Common/JSONParsers/DummyJSONParser.h>

namespace DB
{

#if USE_SIMDJSON
template void jsonElementToString<SimdJSONParser>(const SimdJSONParser::Element & element, WriteBuffer & buf, const FormatSettings & format_settings);
template std::unique_ptr<JSONExtractTreeNode<SimdJSONParser>> buildJSONExtractTree<SimdJSONParser>(const DataTypePtr & type, const char * source_for_exception_message);
#endif

#if USE_RAPIDJSON
template void jsonElementToString<RapidJSONParser>(const RapidJSONParser::Element & element, WriteBuffer & buf, const FormatSettings & format_settings);
template std::unique_ptr<JSONExtractTreeNode<RapidJSONParser>> buildJSONExtractTree<RapidJSONParser>(const DataTypePtr & type, const char * source_for_exception_message);
template bool tryGetNumericValueFromJSONElement<RapidJSONParser, Float64>(Float64 & value, const RapidJSONParser::Element & element, bool convert_bool_to_integer, bool allow_type_conversion, String & error);
#else
template void jsonElementToString<DummyJSONParser>(const DummyJSONParser::Element & element, WriteBuffer & buf, const FormatSettings & format_settings);
template std::unique_ptr<JSONExtractTreeNode<DummyJSONParser>> buildJSONExtractTree<DummyJSONParser>(const DataTypePtr & type, const char * source_for_exception_message);
#endif

}

