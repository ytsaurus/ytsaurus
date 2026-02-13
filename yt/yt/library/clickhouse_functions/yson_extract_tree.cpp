#include "yson_extract_tree.h"

#include "unescaped_yson.h"
#include "yson_parser_adapter.h"

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yt/error/error.h>

#include <util/stream/str.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>

#include <base/extended_types.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>

#include <Core/AccurateComparison.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/Serializations/SerializationDecimal.h>

#include <Formats/FormatSettings.h>
#include <Formats/JSONExtractTree.h>

#include <IO/parseDateTimeBestEffort.h>

namespace DB::ErrorCodes {
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int INCORRECT_DATA;
} // namespace ErrorCodes


namespace NYT::NClickHouseServer {

namespace {

class TStringColumnCharsOutput
    : public IOutputStream
{
public:
    TStringColumnCharsOutput(DB::ColumnString::Chars& stringChars)
        : StringChars_(stringChars)
    { }

    void DoWrite(const void* buf, size_t len) override
    {
        auto* bytes = static_cast<const UInt8*>(buf);
        StringChars_.insert(StringChars_.end(), bytes, bytes + len);
    }

private:
    DB::ColumnString::Chars& StringChars_;
};

template <typename NumberType>
class NumericNode : public IYsonTreeNodeExtractor
{
public:
    explicit NumericNode(bool isBoolType = false)
        : IsBoolType_(isBoolType)
    { }

    TError ExtractNodeToColumn(
        DB::IColumn& column,
        const TYsonParserAdapter::Element& node,
        const DB::JSONExtractInsertSettings& insertSettings,
        const DB::FormatSettings& formatSettings) const override
    {
        if (node.isNull()) {
            if (formatSettings.null_as_default) {
                column.insertDefault();
                return {};
            }

            return TError("Cannot parse %v value from null", DB::TypeName<NumberType>);
        }

        if (IsBoolType_ && !insertSettings.allow_type_conversion) {
            if (!node.isBool()) {
                return TError("");
            }
            assert_cast<DB::ColumnVector<NumberType>&>(column).insertValue(node.getBool());
            return {};
        }

        NumberType value;
        auto error = TryGetNumericValueFromYsonElement<NumberType>(value, node, /*convertBoolToNumber*/ true, insertSettings.allow_type_conversion);
        if (!error.IsOK()) {
            return TError("Cannot read %v value from JSON element: %v",
                DB::TypeName<NumberType>,
                NYson::ConvertToYsonString(node.GetNode(), NYson::EYsonFormat::Pretty)) << error;
        }

        if (IsBoolType_) {
            value = static_cast<bool>(value);
        }

        auto& colVec = assert_cast<DB::ColumnVector<NumberType>&>(column);
        colVec.insertValue(value);
        return {};
    }

protected:
    bool IsBoolType_;
};

class StringNode : public IYsonTreeNodeExtractor
{
public:
    TError ExtractNodeToColumn(
        DB::IColumn& column,
        const TYsonParserAdapter::Element& node,
        const DB::JSONExtractInsertSettings& insertSettings,
        const DB::FormatSettings& formatSettings) const override
    {
        if (node.isNull()) {
            if (formatSettings.null_as_default) {
                column.insertDefault();
                return {};
            }
            return TError("Cannot parse String value from null");
        }

        auto& colStr = assert_cast<DB::ColumnString&>(column);
        if (!node.isString()) {
            if (!insertSettings.allow_type_conversion) {
                return TError("");
            }

            auto& chars = colStr.getChars();
            TStringColumnCharsOutput charsOutput(chars);
            TExtendedYsonWriter writer(&charsOutput);
            // TString result;
            // TStringOutput stringOutput(result);
            // TExtendedYsonWriter writer(&stringOutput);
            Serialize(*node.GetNode(), &writer);
            // chars.insert(chars.end(), result.data(), result.data() + result.size());
            chars.push_back(0);
            colStr.getOffsets().push_back(chars.size());
        } else {
            auto value = node.getString();
            colStr.insertData(value.data(), value.size());
        }
        return {};
    }
};

class ArrayNode : public IYsonTreeNodeExtractor
{
public:
    explicit ArrayNode(std::unique_ptr<IYsonTreeNodeExtractor> nested)
        : Nested_(std::move(nested))
    { }

    TError ExtractNodeToColumn(
        DB::IColumn& column,
        const TYsonParserAdapter::Element& node,
        const DB::JSONExtractInsertSettings& insertSettings,
        const DB::FormatSettings& formatSettings) const override
    {
        if (node.isNull()) {
            if (formatSettings.null_as_default) {
                column.insertDefault();
                return {};
            }
            return TError("Cannot parse Array value from null");
        }

        if (!node.isArray()) {
            return TError("Cannot read Array value from YSON element: %v",
                NYson::ConvertToYsonString(node.GetNode(), NYson::EYsonFormat::Pretty));
        }

        auto array = node.getArray();

        auto& colArr = assert_cast<DB::ColumnArray&>(column);
        auto& data = colArr.getData();
        size_t oldSize = data.size();
        bool wereValidElements = false;

        for (auto value : array) {
            auto error = Nested_->ExtractNodeToColumn(data, value, insertSettings, formatSettings);
            if (error.IsOK()) {
                wereValidElements = true;
            } else if (insertSettings.insert_default_on_invalid_elements_in_complex_types) {
                data.insertDefault();
            } else {
                data.popBack(data.size() - oldSize);
                return error;
            }
        }

        if (data.size() != oldSize && !wereValidElements) {
            data.popBack(data.size() - oldSize);
            return TError("No valid elements found in array");
        }

        colArr.getOffsets().push_back(data.size());
        return {};
    }

private:
    std::unique_ptr<IYsonTreeNodeExtractor> Nested_;
};

class TupleNode : public IYsonTreeNodeExtractor
{
public:
    TupleNode(std::vector<std::unique_ptr<IYsonTreeNodeExtractor>> nested, const std::vector<DB::String>& explicitNames)
        : Nested_(std::move(nested))
        , ExplicitNames_(explicitNames)
    {
        for (size_t i = 0; i != ExplicitNames_.size(); ++i) {
            NameToIndexMap_.emplace(ExplicitNames_[i], i);
        }
    }

    TError ExtractNodeToColumn(
        DB::IColumn& column,
        const TYsonParserAdapter::Element& node,
        const DB::JSONExtractInsertSettings& insertSettings,
        const DB::FormatSettings& formatSettings) const override
    {
        if (node.isNull()) {
            if (formatSettings.null_as_default) {
                column.insertDefault();
                return {};
            }
            return TError("Cannot parse Tuple value from null");
        }

        auto& tuple = assert_cast<DB::ColumnTuple&>(column);
        size_t oldSize = column.size();
        bool wereValidElements = false;

        auto setSize = [&](size_t size) {
            for (size_t i = 0; i != tuple.tupleSize(); ++i) {
                auto& col = tuple.getColumn(i);
                if (col.size() != size) {
                    if (col.size() > size) {
                        col.popBack(col.size() - size);
                    } else {
                        while (col.size() < size) {
                            col.insertDefault();
                        }
                    }
                }
            }
        };

        if (node.isArray()) {
            auto array = node.getArray();
            auto it = array.begin();

            for (size_t index = 0; (index != Nested_.size()) && (it != array.end()); ++index) {
                auto error = Nested_[index]->ExtractNodeToColumn(tuple.getColumn(index), *it++, insertSettings, formatSettings);
                if (error.IsOK()) {
                    wereValidElements = true;
                } else if (insertSettings.insert_default_on_invalid_elements_in_complex_types) {
                    tuple.getColumn(index).insertDefault();
                } else {
                    setSize(oldSize);
                    return error << TErrorAttribute("tuple_element", index);
                }
            }

            setSize(oldSize + static_cast<size_t>(wereValidElements));
            return wereValidElements ? TError() : TError("No valid elements found in tuple array");
        }

        if (node.isObject()) {
            auto object = node.getObject();
            if (NameToIndexMap_.empty()) {
                auto it = object.begin();
                for (size_t index = 0; (index != Nested_.size()) && (it != object.end()); ++index) {
                    auto error = Nested_[index]->ExtractNodeToColumn(tuple.getColumn(index), (*it++).second, insertSettings, formatSettings);
                    if (error.IsOK()) {
                        wereValidElements = true;
                    } else if (insertSettings.insert_default_on_invalid_elements_in_complex_types) {
                        tuple.getColumn(index).insertDefault();
                    } else {
                        setSize(oldSize);
                        return error << TErrorAttribute("tuple_element", index);
                    }
                }
            } else {
                for (const auto& [key, value] : object) {
                    auto indexIt = NameToIndexMap_.find(DB::String(key));
                    if (indexIt != NameToIndexMap_.end()) {
                        auto error = Nested_[indexIt->second]->ExtractNodeToColumn(
                            tuple.getColumn(indexIt->second), value, insertSettings, formatSettings);
                        if (error.IsOK()) {
                            wereValidElements = true;
                        } else if (!insertSettings.insert_default_on_invalid_elements_in_complex_types) {
                            setSize(oldSize);
                            return error << TErrorAttribute("tuple_element", key);
                        }
                    }
                }
            }

            setSize(oldSize + static_cast<size_t>(wereValidElements));
            return wereValidElements ? TError() : TError("No valid elements found in tuple object");
        }

        return TError("Cannot read Tuple value from YSON element: %v",
            NYson::ConvertToYsonString(node.GetNode(), NYson::EYsonFormat::Pretty));
    }

private:
    std::vector<std::unique_ptr<IYsonTreeNodeExtractor>> Nested_;
    std::vector<DB::String> ExplicitNames_;
    std::unordered_map<std::string_view, size_t> NameToIndexMap_;
};

class MapNode : public IYsonTreeNodeExtractor
{
public:
    explicit MapNode(std::unique_ptr<IYsonTreeNodeExtractor> value)
        : Value_(std::move(value))
    { }

    TError ExtractNodeToColumn(
        DB::IColumn& column,
        const TYsonParserAdapter::Element& node,
        const DB::JSONExtractInsertSettings& insertSettings,
        const DB::FormatSettings& formatSettings) const override
    {
        if (node.isNull()) {
            if (formatSettings.null_as_default) {
                column.insertDefault();
                return {};
            }
            return TError("Cannot parse Map value from null");
        }

        if (!node.isObject()) {
            return TError("Cannot read Map value from YSON element: %v",
                NYson::ConvertToYsonString(node.GetNode(), NYson::EYsonFormat::Pretty));
        }

        auto& mapCol = assert_cast<DB::ColumnMap&>(column);
        auto& offsets = mapCol.getNestedColumn().getOffsets();
        auto& tupleCol = mapCol.getNestedData();
        auto& keyCol = tupleCol.getColumn(0);
        auto& valueCol = tupleCol.getColumn(1);
        size_t oldSize = tupleCol.size();

        auto object = node.getObject();
        auto it = object.begin();
        for (; it != object.end(); ++it) {
            auto pair = *it;

            // Insert key
            keyCol.insertData(pair.first.data(), pair.first.size());

            // Insert value
            auto error = Value_->ExtractNodeToColumn(valueCol, pair.second, insertSettings, formatSettings);
            if (!error.IsOK()) {
                if (insertSettings.insert_default_on_invalid_elements_in_complex_types) {
                    valueCol.insertDefault();
                } else {
                    keyCol.popBack(keyCol.size() - offsets.back());
                    valueCol.popBack(valueCol.size() - offsets.back());
                    return error << TErrorAttribute("map_key", pair.first);
                }
            }
        }

        offsets.push_back(oldSize + object.size());
        return {};
    }

private:
    std::unique_ptr<IYsonTreeNodeExtractor> Value_;
};

class NullableNode : public IYsonTreeNodeExtractor
{
public:
    explicit NullableNode(std::unique_ptr<IYsonTreeNodeExtractor> nested)
        : Nested_(std::move(nested))
    { }

    TError ExtractNodeToColumn(
        DB::IColumn& column,
        const TYsonParserAdapter::Element& node,
        const DB::JSONExtractInsertSettings& insertSettings,
        const DB::FormatSettings& formatSettings) const override
    {
        if (node.isNull()) {
            column.insertDefault();
            return {};
        }

        auto& colNull = assert_cast<DB::ColumnNullable&>(column);
        auto error = Nested_->ExtractNodeToColumn(colNull.getNestedColumn(), node, insertSettings, formatSettings);
        if (!error.IsOK()) {
            return error;
        }
        colNull.getNullMapColumn().insertValue(0);
        return {};
    }

private:
    std::unique_ptr<IYsonTreeNodeExtractor> Nested_;
};


template <typename DateType, typename ColumnNumericType>
class DateNode : public IYsonTreeNodeExtractor
{
public:
    TError ExtractNodeToColumn(
        DB::IColumn& column,
        const TYsonParserAdapter::Element& node,
        const DB::JSONExtractInsertSettings&,
        const DB::FormatSettings& formatSettings) const override
    {
        if (node.isNull()) {
            if (formatSettings.null_as_default) {
                column.insertDefault();
                return {};
            }
            return TError("Cannot parse Date value from null");
        }

        if (!node.isString()) {
            return TError("Cannot read Date value from YSON element: %v",
                NYson::ConvertToYsonString(node.GetNode(), NYson::EYsonFormat::Pretty));
        }

        auto data = node.getString();
        DB::ReadBufferFromMemory buf(data.data(), data.size());
        DateType date;
        if (!DB::tryReadDateText(date, buf) || !buf.eof()) {
            return TError("Cannot parse Date value here: %v", data);
        }

        assert_cast<DB::ColumnVector<ColumnNumericType>&>(column).insertValue(date);
        return {};
    }
};

class DateTimeNode : public IYsonTreeNodeExtractor
{
public:
    explicit DateTimeNode(const DB::DataTypeDateTime& datetimeType)
        : TimeZone_(datetimeType.getTimeZone())
        , UtcTimeZone_(DateLUT::instance("UTC"))
    { }

    TError ExtractNodeToColumn(
        DB::IColumn& column,
        const TYsonParserAdapter::Element& node,
        const DB::JSONExtractInsertSettings& insertSettings,
        const DB::FormatSettings& formatSettings) const override
    {
        if (node.isNull()) {
            if (formatSettings.null_as_default) {
                column.insertDefault();
                return {};
            }
            return TError("Cannot parse DateTime value from null");
        }

        time_t value;
        if (node.isString()) {
            if (!TryParse(value, node.getString(), formatSettings.date_time_input_format)) {
                return TError("Cannot parse DateTime value here: %v", node.getString());
            }
        } else if (node.isUInt64() && insertSettings.allow_type_conversion) {
            value = node.getUInt64();
        } else {
            return TError("Cannot read DateTime value from YSON element: %v",
                NYson::ConvertToYsonString(node.GetNode(), NYson::EYsonFormat::Pretty));
        }

        assert_cast<DB::ColumnDateTime&>(column).insert(value);
        return {};
    }

    bool TryParse(time_t& value, std::string_view data, DB::FormatSettings::DateTimeInputFormat /*dateTimeInputFormat*/) const
    {
        DB::ReadBufferFromMemory buf(data.data(), data.size());
        // Only support Basic format for now
        return DB::tryReadDateTimeText(value, buf, TimeZone_) && buf.eof();
    }

private:
    const DateLUTImpl & TimeZone_;
    const DateLUTImpl & UtcTimeZone_;
};

template <typename DecimalType>
class DecimalNode : public IYsonTreeNodeExtractor
{
public:
    explicit DecimalNode(const DB::DataTypePtr& type)
        : Scale_(assert_cast<const DB::DataTypeDecimal<DecimalType>&>(*type).getScale())
    { }

    TError ExtractNodeToColumn(
        DB::IColumn& column,
        const TYsonParserAdapter::Element& node,
        const DB::JSONExtractInsertSettings&,
        const DB::FormatSettings& formatSettings) const override
    {
        DecimalType value{};

        switch (node.type()) {
            case DB::ElementType::DOUBLE:
                value = DB::convertToDecimal<DB::DataTypeNumber<Float64>, DB::DataTypeDecimal<DecimalType>>(node.getDouble(), Scale_);
                break;
            case DB::ElementType::UINT64:
                value = DB::convertToDecimal<DB::DataTypeNumber<UInt64>, DB::DataTypeDecimal<DecimalType>>(node.getUInt64(), Scale_);
                break;
            case DB::ElementType::INT64:
                value = DB::convertToDecimal<DB::DataTypeNumber<Int64>, DB::DataTypeDecimal<DecimalType>>(node.getInt64(), Scale_);
                break;
            case DB::ElementType::STRING:
            {
                auto rb = DB::ReadBufferFromMemory{node.getString()};
                if (!DB::SerializationDecimal<DecimalType>::tryReadText(value, rb, DB::DecimalUtils::max_precision<DecimalType>, Scale_)) {
                    return TError("Cannot parse Decimal value here: %v", node.getString());
                }
                break;
            }
            case DB::ElementType::NULL_VALUE:
            {
                if (!formatSettings.null_as_default) {
                    return TError("Cannot convert null to Decimal value");
                }
                break;
            }
            default:
            {
                return TError("Cannot read Decimal value from YSON element: %v",
                    NYson::ConvertToYsonString(node.GetNode(), NYson::EYsonFormat::Pretty));
            }
        }

        assert_cast<DB::ColumnDecimal<DecimalType>&>(column).insertValue(value);
        return {};
    }

private:
    UInt32 Scale_;
};

class DateTime64Node : public IYsonTreeNodeExtractor
{
public:
    explicit DateTime64Node(const DB::DataTypeDateTime64& datetime64Type)
        : Scale_(datetime64Type.getScale())
        , TimeZone_(datetime64Type.getTimeZone())
        , UtcTimeZone_(DateLUT::instance("UTC"))
    { }

    TError ExtractNodeToColumn(
        DB::IColumn& column,
        const TYsonParserAdapter::Element& node,
        const DB::JSONExtractInsertSettings& insertSettings,
        const DB::FormatSettings& formatSettings) const override
    {
        if (node.isNull()) {
            if (formatSettings.null_as_default) {
                column.insertDefault();
                return {};
            }
            return TError("Cannot parse DateTime64 value from null");
        }

        DB::DateTime64 value;
        if (node.isString()) {
            if (!TryParse(value, node.getString(), formatSettings.date_time_input_format)) {
                return TError("Cannot parse DateTime64 value here: %v", node.getString());
            }
        } else {
            if (!insertSettings.allow_type_conversion) {
                return TError("");
            }

            switch (node.type()) {
                case DB::ElementType::DOUBLE:
                    value = DB::convertToDecimal<DB::DataTypeNumber<Float64>, DB::DataTypeDecimal<DB::DateTime64>>(node.getDouble(), Scale_);
                    break;
                case DB::ElementType::UINT64:
                    value.value = node.getUInt64();
                    break;
                case DB::ElementType::INT64:
                    value.value = node.getInt64();
                    break;
                default:
                    return TError("Cannot read DateTime64 value from YSON element: %v",
                        NYson::ConvertToYsonString(node.GetNode(), NYson::EYsonFormat::Pretty));
            }
        }

        assert_cast<DB::ColumnDateTime64&>(column).insert(value);
        return {};
    }

    bool TryParse(DB::DateTime64& value, std::string_view data, DB::FormatSettings::DateTimeInputFormat /*dateTimeInputFormat*/) const
    {
        DB::ReadBufferFromMemory buf(data.data(), data.size());
        // Only support Basic format for now
        return DB::tryReadDateTime64Text(value, Scale_, buf, TimeZone_) && buf.eof();
    }

private:
    UInt32 Scale_;
    const DateLUTImpl & TimeZone_;
    const DateLUTImpl & UtcTimeZone_;
};

} // namespace

std::unique_ptr<IYsonTreeNodeExtractor> CreateYsonTreeNodeExtractor(const DB::DataTypePtr& type/*, const char* source_for_exception_message*/)
{
    switch (type->getTypeId()) {
        case DB::TypeIndex::UInt8:
            return std::make_unique<NumericNode<UInt8>>(isBool(type));
        case DB::TypeIndex::UInt16:
            return std::make_unique<NumericNode<UInt16>>();
        case DB::TypeIndex::UInt32:
            return std::make_unique<NumericNode<UInt32>>();
        case DB::TypeIndex::UInt64:
            return std::make_unique<NumericNode<UInt64>>();
        case DB::TypeIndex::UInt128:
            return std::make_unique<NumericNode<UInt128>>();
        case DB::TypeIndex::UInt256:
            return std::make_unique<NumericNode<UInt256>>();
        case DB::TypeIndex::Int8:
            return std::make_unique<NumericNode<Int8>>();
        case DB::TypeIndex::Int16:
            return std::make_unique<NumericNode<Int16>>();
        case DB::TypeIndex::Int32:
            return std::make_unique<NumericNode<Int32>>();
        case DB::TypeIndex::Int64:
            return std::make_unique<NumericNode<Int64>>();
        case DB::TypeIndex::Int128:
            return std::make_unique<NumericNode<Int128>>();
        case DB::TypeIndex::Int256:
            return std::make_unique<NumericNode<Int256>>();
        case DB::TypeIndex::Float32:
            return std::make_unique<NumericNode<Float32>>();
        case DB::TypeIndex::Float64:
            return std::make_unique<NumericNode<Float64>>();
        case DB::TypeIndex::String:
            return std::make_unique<StringNode>();
        case DB::TypeIndex::Date:
            return std::make_unique<DateNode<DayNum, UInt16>>();
        case DB::TypeIndex::Date32:
            return std::make_unique<DateNode<ExtendedDayNum, Int32>>();
        case DB::TypeIndex::DateTime:
            return std::make_unique<DateTimeNode>(assert_cast<const DB::DataTypeDateTime&>(*type));
        case DB::TypeIndex::DateTime64:
            return std::make_unique<DateTime64Node>(assert_cast<const DB::DataTypeDateTime64&>(*type));
        case DB::TypeIndex::Decimal32:
            return std::make_unique<DecimalNode<DB::Decimal32>>(type);
        case DB::TypeIndex::Decimal64:
            return std::make_unique<DecimalNode<DB::Decimal64>>(type);
        case DB::TypeIndex::Decimal128:
            return std::make_unique<DecimalNode<DB::Decimal128>>(type);
        case DB::TypeIndex::Decimal256:
            return std::make_unique<DecimalNode<DB::Decimal256>>(type);
        case DB::TypeIndex::Nullable:
            return std::make_unique<NullableNode>(CreateYsonTreeNodeExtractor(assert_cast<const DB::DataTypeNullable&>(*type).getNestedType()));
        case DB::TypeIndex::Array:
            return std::make_unique<ArrayNode>(CreateYsonTreeNodeExtractor(assert_cast<const DB::DataTypeArray&>(*type).getNestedType()));
        case DB::TypeIndex::Tuple:
        {
            const auto& tuple = assert_cast<const DB::DataTypeTuple&>(*type);
            const auto& tupleElements = tuple.getElements();
            std::vector<std::unique_ptr<IYsonTreeNodeExtractor>> elements;
            elements.reserve(tupleElements.size());
            for (const auto& tupleElement : tupleElements) {
                elements.emplace_back(CreateYsonTreeNodeExtractor(tupleElement));
            }
            return std::make_unique<TupleNode>(std::move(elements), tuple.haveExplicitNames() ? tuple.getElementNames() : DB::Strings{});
        }
        case DB::TypeIndex::Map:
        {
            const auto& mapType = assert_cast<const DB::DataTypeMap&>(*type);
            const auto& keyType = mapType.getKeyType();
            if (!DB::isString(DB::removeLowCardinality(keyType))) {
                throw DB::Exception(
                    DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "YSON* function doesn't support the return type schema: {} with key type not String",
                    type->getName());
            }

            const auto& valueType = mapType.getValueType();
            return std::make_unique<MapNode>(CreateYsonTreeNodeExtractor(valueType));
        }
        default:
            throw DB::Exception(
                DB::ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "YSON* function doesn't support the return type schema: {}",
                type->getName());
    }
}

} // namespace DB
