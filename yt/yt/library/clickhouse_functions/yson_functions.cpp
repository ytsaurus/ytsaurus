#include "yson_parser_adapter.h"
#include "yson_extract_tree.h"

#include <yt/yt/core/ytree/convert.h>

#include <Formats/JSONExtractTree.h>

#include <Functions/FunctionFactory.h>

#include <DataTypes/DataTypeString.h>
#include <Formats/FormatSettings.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <Columns/ColumnConst.h>


namespace DB::ErrorCodes {

////////////////////////////////////////////////////////////////////////////////

extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;

////////////////////////////////////////////////////////////////////////////////

} // namespace DB::ErrorCodes;


namespace NYT::NClickHouseServer {

using namespace NYson;
using namespace NYTree;

using namespace DB;

using YsonElement = typename TYsonParserAdapter::Element;

////////////////////////////////////////////////////////////////////////////////

template <typename T>
concept Preparable = requires (T t)
{
    t.prepare(
        std::declval<const ColumnsWithTypeAndName&>(),
        std::declval<const DataTypePtr&>()
    );
};

//! This is copy of FunctionsJsonHelpers from
//! https://github.com/ClickHouse/ClickHouse/blob/25.3/src/Functions/FunctionsJSON.cpp#L505
class FunctionYsonHelpers
{
public:
    template <typename Name, typename Impl>
    class Executor
    {
    public:
        static ColumnPtr Run(const ColumnsWithTypeAndName& arguments, const DataTypePtr& resultType, size_t inputRowCount, const FormatSettings& formatSettings)
        {
            MutableColumnPtr result{resultType->createColumn()};
            result->reserve(inputRowCount);

            if (arguments.empty()) {
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at least one argument", String(Name::name));
            }

            const auto& firstColumn = arguments[0];
            if (!isString(firstColumn.type)) {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "The first argument of function {} should be a string containing YSON, illegal type: {}",
                        String(Name::name), firstColumn.type->getName());
            }

            const ColumnPtr& argYson = firstColumn.column;

            const auto* colYsonConst = typeid_cast<const ColumnConst*>(argYson.get());
            const auto* colYson = colYsonConst ? colYsonConst->getDataColumnPtr().get() : argYson.get();
            const auto* colYsonString = typeid_cast<const ColumnString*>(colYson);

            if (!colYsonString) {
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {}", argYson->getName());
            }

            const auto& chars = colYsonString->getChars();
            const auto& offsets = colYsonString->getOffsets();

            auto numIndexArgs = Impl::getNumberOfIndexArguments(arguments);
            std::vector<Move> moves = PrepareMoves(Name::name, arguments, 1, numIndexArgs);

            TYsonParserAdapter parser;
            #if 0
            // Optional: Allocates memory to parse documents faster
            size_t maxSize = 0;
            for (size_t i = 1; i < offsets.size(); ++i) {
                size_t size = offsets[i] - offsets[i - 1];
                maxSize = std::max(maxSize, size);
            }
            if (maxSize) {
                parser.reserve(max_size - 1);
            }
            #endif

            Impl impl;
            if constexpr (Preparable<Impl>) {
                impl.prepare(arguments, resultType);
            }

            YsonElement document;
            bool documentOk = false;
            if (colYsonConst) {
                std::string_view json{reinterpret_cast<const char*>(chars.data()), offsets[0] - 1};
                documentOk = parser.parse(json, document);
            }

            for (size_t i = 0; i < inputRowCount; ++i) {
                if (!colYsonConst) {
                    std::string_view json{reinterpret_cast<const char *>(&chars[offsets[i - 1]]), offsets[i] - offsets[i - 1] - 1};
                    documentOk = parser.parse(json, document);
                }

                bool addedToColumn = false;
                if (documentOk) {
                    YsonElement element;
                    std::string_view lastKey;
                    bool movesOk = PerformMoves(arguments, i, document, moves, element, lastKey);

                    if (movesOk) {
                        addedToColumn = impl.InsertResultToColumn(*result, element, lastKey, formatSettings);
                    }
                }

                if (!addedToColumn) {
                    result->insertDefault();
                }
            }
            return result;
        }
    };

private:
    enum class MoveType : uint8_t
    {
        Key,
        Index,
        ConstKey,
        ConstIndex,
    };

    struct Move
    {
        explicit Move(MoveType type, size_t index = 0)
            : Type(type)
            , Index(index)
        { }
        Move(MoveType type, const String& key)
            : Type(type)
            , Key(key)
        { }

        MoveType Type;
        size_t Index = 0;
        String Key;
    };

    static std::vector<Move> PrepareMoves(
        const char* functionName,
        const ColumnsWithTypeAndName& columns,
        size_t firstIndexArgument,
        size_t numIndexArgs)
    {
        std::vector<Move> moves;
        moves.reserve(numIndexArgs);
        for (size_t i = firstIndexArgument; i < firstIndexArgument + numIndexArgs; ++i) {
            const auto& column = columns[i];
            if (!isString(column.type) && !isNativeInteger(column.type)) {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The argument {} of function {} should be a string specifying key "
                    "or an integer specifying index, illegal type: {}",
                    std::to_string(i + 1), String(functionName), column.type->getName());
            }

            if (column.column && isColumnConst(*column.column)) {
                const auto& columnConst = assert_cast<const ColumnConst&>(*column.column);
                if (isString(column.type)) {
                    moves.emplace_back(MoveType::ConstKey, columnConst.getValue<String>());
                } else {
                    moves.emplace_back(MoveType::ConstIndex, columnConst.getInt(0));
                }
            } else {
                if (isString(column.type)) {
                    moves.emplace_back(MoveType::Key, "");
                } else {
                    moves.emplace_back(MoveType::Index, 0);
                }
            }
        }
        return moves;
    }

    static bool PerformMoves(
        const ColumnsWithTypeAndName& arguments,
        size_t row,
        const YsonElement& document,
        const std::vector<Move>& moves,
        YsonElement& element,
        std::string_view& lastKey)
    {
        YsonElement resElement = document;
        std::string_view key;

        bool movesOk = true;
        for (size_t j = 0; movesOk && j != moves.size(); ++j) {
            switch (moves[j].Type) {
                case MoveType::ConstIndex: {
                    movesOk = MoveToElementByIndex(resElement, static_cast<int>(moves[j].Index), key);
                    break;
                }
                case MoveType::ConstKey: {
                    key = moves[j].Key;
                    movesOk = MoveToElementByKey(resElement, key);
                    break;
                }
                case MoveType::Index: {
                    Int64 index = (*arguments[j + 1].column)[row].safeGet<Int64>();
                    movesOk = MoveToElementByIndex(resElement, static_cast<int>(index), key);
                    break;
                }
                case MoveType::Key: {
                    key = arguments[j + 1].column->getDataAt(row).toView();
                    movesOk = MoveToElementByKey(resElement, key);
                    break;
                }
            }
        }

        if (movesOk) {
            element = resElement;
            lastKey = key;
        }
        return movesOk;
    }

    static bool MoveToElementByIndex(YsonElement& element, int index, std::string_view& outKey)
    {
        if (element.isArray()) {
            auto array = element.getArray();
            if (index >= 0) {
                --index;
            } else {
                index += array.size();
            }

            if (static_cast<size_t>(index) >= array.size()) {
                return false;
            }
            element = array[index];
            outKey = {};
            return true;
        }

        return false;
    }

    static bool MoveToElementByKey(YsonElement& element, std::string_view key)
    {
        if (!element.isObject()) {
            return false;
        }
        auto object = element.getObject();
        return object.find(key, element);
    }
};

//! This is an analog of JSONOverloadResolver from
//! https://github.com/ClickHouse/ClickHouse/blob/25.3/src/Functions/FunctionsJSON.cpp#L505
//! Functions to parse YSONs and extract values from it.
//! The first argument of all these functions gets a YSON,
//! after that there is an arbitrary number of arguments specifying path to a desired part from the YSON's root.
//! For example,
//! select YSONExtractInt('{a = "hello"; b = [-100; 200.0; 300]}', 'b', 1) = -100
template <typename Name, typename Impl>
class TFunctionYson
    : public IFunction
    , public WithConstContext
{
public:
    explicit TFunctionYson(ContextPtr context_)
        : WithConstContext(context_)
    { }

    static constexpr auto name = Name::name;

    static FunctionPtr create(ContextPtr context_)
    {
        return std::make_shared<TFunctionYson>(context_);
    }

    String getName() const override
    {
        return Name::name;
    }

    bool isVariadic() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo& /*arguments*/) const override
    {
        return true;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return false;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName& arguments) const override
    {
        return Impl::getReturnType(Name::name, arguments);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName& arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return FunctionYsonHelpers::Executor<Name, Impl>::Run(arguments, result_type, input_rows_count, FormatSettings_);
    }

private:
    FormatSettings FormatSettings_;
};

////////////////////////////////////////////////////////////////////////////////

class TYsonHasImpl
{
public:
    static DataTypePtr getReturnType(const char*, const ColumnsWithTypeAndName&)
    {
        return std::make_shared<DataTypeUInt8>();
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName& arguments)
    {
        return arguments.size() - 1;
    }

    static bool InsertResultToColumn(IColumn& dest, const YsonElement&, std::string_view, const FormatSettings&)
    {
        auto& colVec = assert_cast<ColumnVector<UInt8>&>(dest);
        colVec.insertValue(1);
        return true;
    }
};

class TYsonLengthImpl
{
public:
    static DataTypePtr getReturnType(const char*, const ColumnsWithTypeAndName&)
    {
        return std::make_shared<DataTypeUInt64>();
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName& arguments)
    {
        return arguments.size() - 1;
    }

    static bool InsertResultToColumn(IColumn& dest, const YsonElement& element, std::string_view, const FormatSettings&)
    {
        if (!element.isArray() && !element.isObject()) {
            return false;
        }
        auto size = element.isArray() ? element.getArray().size() : element.getObject().size();
        auto& colVec = assert_cast<ColumnVector<UInt64>&>(dest);
        colVec.insertValue(size);
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TYsonKeyImpl
{
public:
    static DataTypePtr getReturnType(const char*, const ColumnsWithTypeAndName&)
    {
        return std::make_shared<DataTypeString>();
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName& arguments)
    {
        return arguments.size() - 1;
    }

    static bool InsertResultToColumn(IColumn& dest, const YsonElement&, std::string_view lastKey, const FormatSettings&)
    {
        if (lastKey.empty()) {
            return false;
        }
        auto& colStr = assert_cast<ColumnString&>(dest);
        colStr.insertData(lastKey.data(), lastKey.size());
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TYsonTypeImpl
{
public:
    static DataTypePtr getReturnType(const char*, const ColumnsWithTypeAndName&)
    {
        static const std::vector<std::pair<String, Int8>> values = {
            {"Array", '['},
            {"Object", '{'},
            {"String", '"'},
            {"Int64", 'i'},
            {"UInt64", 'u'},
            {"Double", 'd'},
            {"Bool", 'b'},
            {"Null", 0}, /// the default value for the column.
        };
        return std::make_shared<DataTypeEnum<Int8>>(values);
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName& arguments)
    {
        return arguments.size() - 1;
    }

    static bool InsertResultToColumn(IColumn& dest, const YsonElement& element, std::string_view, const FormatSettings&)
    {
        static const std::unordered_map<ElementType, Int8> typeToInt8 = {
            {ElementType::INT64, 'i'},
            {ElementType::UINT64, 'u'},
            {ElementType::DOUBLE, 'd'},
            {ElementType::STRING, '"'},
            {ElementType::ARRAY, '['},
            {ElementType::OBJECT, '{'},
            {ElementType::BOOL, 'b'},
            {ElementType::NULL_VALUE, 0},
        };
        auto& colVec = assert_cast<ColumnVector<Int8>&>(dest);
        colVec.insertValue(typeToInt8.at(element.type()));
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename NumberType>
class TYsonExtractNumericImpl
{
public:
    static DataTypePtr getReturnType(const char*, const ColumnsWithTypeAndName&)
    {
        return std::make_shared<DataTypeNumber<NumberType>>();
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName& arguments)
    {
        return arguments.size() - 1;
    }

    static bool InsertResultToColumn(IColumn& dest, const YsonElement& element, std::string_view, const FormatSettings&)
    {
        NumberType value;
        auto error = TryGetNumericValueFromYsonElement<NumberType>(value, element, /*convertBoolToNumber*/false, /*allowTypeConversion*/true);
        if (!error.IsOK()) {
            return false;
        }

        auto& colVec = assert_cast<ColumnVector<NumberType>&>(dest);
        colVec.insertValue(value);
        return true;
    }
};

using TYsonExtractInt64Impl = TYsonExtractNumericImpl<Int64>;
using TYsonExtractUInt64Impl = TYsonExtractNumericImpl<UInt64>;
using TYsonExtractFloat64Impl = TYsonExtractNumericImpl<Float64>;

////////////////////////////////////////////////////////////////////////////////

class TYsonExtractBoolImpl
{
public:
    static DataTypePtr getReturnType(const char*, const ColumnsWithTypeAndName&)
    {
        return std::make_shared<DataTypeUInt8>();
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName& arguments)
    {
        return arguments.size() - 1;
    }

    static bool InsertResultToColumn(IColumn& dest, const YsonElement& element, std::string_view, const FormatSettings&)
    {
        bool value;
        switch (element.type())
        {
            case ElementType::BOOL:
                value = element.getBool();
                break;
            case ElementType::INT64:
                value = element.getInt64() != 0;
                break;
            case ElementType::UINT64:
                value = element.getUInt64() != 0;
                break;
            default:
                return false;
        }
        auto& colVec = assert_cast<ColumnVector<UInt8>&>(dest);
        colVec.insertValue(static_cast<UInt8>(value));
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TYsonExtractImpl
{
public:
    static DataTypePtr getReturnType(const char* functionName, const ColumnsWithTypeAndName& arguments)
    {
        if (arguments.size() < 2) {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at least two arguments", String(functionName));
        }

        const auto& col = arguments.back();
        const auto* colTypeConst = typeid_cast<const ColumnConst*>(col.column.get());
        if (!colTypeConst || !isString(col.type)) {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "The last argument of function {} should "
                "be a constant string specifying the return data type, illegal value: {}",
                String(functionName), col.name);
        }

        return DataTypeFactory::instance().get(colTypeConst->getValue<String>());
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName& arguments)
    {
        return arguments.size() - 2;
    }

    void prepare(const ColumnsWithTypeAndName&, const DataTypePtr& resultType)
    {
        Extractor_ = CreateYsonTreeNodeExtractor(resultType);
        InsertSettings_.insert_default_on_invalid_elements_in_complex_types = true;
    }

    bool InsertResultToColumn(IColumn& dest, const YsonElement& element, std::string_view, const FormatSettings& formatSettings)
    {
        auto error = Extractor_->ExtractNodeToColumn(dest, element, InsertSettings_, formatSettings);
        return error.IsOK();
    }

private:
    std::unique_ptr<IYsonTreeNodeExtractor> Extractor_;
    JSONExtractInsertSettings InsertSettings_;
};

////////////////////////////////////////////////////////////////////////////////

class TYsonExtractRawImpl
{
public:
    static DataTypePtr getReturnType(const char*, const ColumnsWithTypeAndName&)
    {
        return std::make_shared<DataTypeString>();
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName& arguments)
    {
        return arguments.size() - 1;
    }

    static bool InsertResultToColumn(IColumn& dest, const YsonElement& element, const std::string_view&, const FormatSettings&)
    {
        auto& colStr = static_cast<ColumnString&>(dest);
        auto& chars = colStr.getChars();
        auto ysonString = ConvertToYsonString(element.GetNode());
        // Add +1 to save zero at the end of the string.
        auto ysonStringBuf = ysonString.AsStringBuf();
        chars.insert(ysonStringBuf.data(), ysonStringBuf.data() + ysonStringBuf.size() + 1);
        colStr.getOffsets().push_back(chars.size());
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TYsonExtractStringImpl
{
public:
    static DataTypePtr getReturnType(const char*, const ColumnsWithTypeAndName&)
    {
        return std::make_shared<DataTypeString>();
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName& arguments)
    {
        return arguments.size() - 1;
    }

    static bool InsertResultToColumn(IColumn& dest, const YsonElement& element, std::string_view, const FormatSettings& formatSettings)
    {
        if (element.isNull()) {
            return false;
        }

        if (!element.isString()) {
            return TYsonExtractRawImpl::InsertResultToColumn(dest, element, {}, formatSettings);
        }

        auto str = element.getString();
        auto& colStr = assert_cast<ColumnString&>(dest);
        colStr.insertData(str.data(), str.size());
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TYsonExtractKeysAndValuesImpl
{
public:
    static DataTypePtr getReturnType(const char* functionName, const ColumnsWithTypeAndName& arguments)
    {
        if (arguments.size() < 2) {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at least two arguments", String(functionName));
        }

        const auto& col = arguments.back();
        const auto* colTypeConst = typeid_cast<const ColumnConst*>(col.column.get());
        if (!colTypeConst || !isString(col.type)) {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "The last argument of function {} should "
                "be a constant string specifying the values' data type, illegal value: {}",
                String(functionName), col.name);
        }

        DataTypePtr keyType = std::make_unique<DataTypeString>();
        DataTypePtr valueType = DataTypeFactory::instance().get(colTypeConst->getValue<String>());
        DataTypePtr tupleType = std::make_unique<DataTypeTuple>(DataTypes{keyType, valueType});
        return std::make_unique<DataTypeArray>(tupleType);
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName& arguments)
    {
        return arguments.size() - 2;
    }

    void prepare(const ColumnsWithTypeAndName&, const DataTypePtr& resultType)
    {
        const auto tupleType = typeid_cast<const DataTypeArray*>(resultType.get())->getNestedType();
        const auto valueType = typeid_cast<const DataTypeTuple*>(tupleType.get())->getElements()[1];
        Extractor_ = CreateYsonTreeNodeExtractor(valueType);
        InsertSettings_.insert_default_on_invalid_elements_in_complex_types = true;
    }

    bool InsertResultToColumn(IColumn& dest, const YsonElement& element, std::string_view, const FormatSettings& formatSettings)
    {
        if (!element.isObject()) {
            return false;
        }

        auto object = element.getObject();

        auto& colArr = assert_cast<ColumnArray&>(dest);
        auto& colTuple = assert_cast<ColumnTuple&>(colArr.getData());
        size_t oldSize = colTuple.size();
        auto & colKey = assert_cast<ColumnString&>(colTuple.getColumn(0));
        auto & colValue = colTuple.getColumn(1);

        for (const auto& [key, value] : object) {
            auto error = Extractor_->ExtractNodeToColumn(colValue, value, InsertSettings_, formatSettings);
            if (error.IsOK()) {
                colKey.insertData(key.data(), key.size());
            }
        }

        if (colTuple.size() == oldSize) {
            return false;
        }

        colArr.getOffsets().push_back(colTuple.size());
        return true;
    }

private:
    std::unique_ptr<IYsonTreeNodeExtractor> Extractor_;
    JSONExtractInsertSettings InsertSettings_;
};

////////////////////////////////////////////////////////////////////////////////

class TYsonExtractArrayRawImpl
{
public:
    static DataTypePtr getReturnType(const char*, const ColumnsWithTypeAndName&)
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName& arguments)
    {
        return arguments.size() - 1;
    }

    static bool InsertResultToColumn(IColumn& dest, const YsonElement& element, const std::string_view&, const FormatSettings&)
    {
        if (!element.isArray()) {
            return false;
        }

        auto array = element.getArray();
        auto& colRes = assert_cast<ColumnArray&>(dest);

        for (auto value : array) {
            TYsonExtractRawImpl::InsertResultToColumn(colRes.getData(), value, {}, {});
        }

        colRes.getOffsets().push_back(colRes.getOffsets().back() + array.size());
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TYsonExtractKeysAndValuesRawImpl
{
public:
    static DataTypePtr getReturnType(const char*, const ColumnsWithTypeAndName&)
    {
        DataTypePtr stringType = std::make_unique<DataTypeString>();
        DataTypePtr tupleType = std::make_unique<DataTypeTuple>(DataTypes{stringType, stringType});
        return std::make_unique<DataTypeArray>(tupleType);
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName& arguments)
    {
        return arguments.size() - 1;
    }

    bool InsertResultToColumn(IColumn& dest, const YsonElement& element, const std::string_view&, const FormatSettings&)
    {
        if (!element.isObject()) {
            return false;
        }

        auto object = element.getObject();

        auto& colArr = assert_cast<ColumnArray&>(dest);
        auto& colTuple = assert_cast<ColumnTuple&>(colArr.getData());
        auto& colKey = assert_cast<ColumnString&>(colTuple.getColumn(0));
        auto& colValue = assert_cast<ColumnString&>(colTuple.getColumn(1));

        for (auto [key, value] : object) {
            colKey.insertData(key.data(), key.size());
            TYsonExtractRawImpl::InsertResultToColumn(colValue, value, {}, {});
        }

        colArr.getOffsets().push_back(colArr.getOffsets().back() + object.size());
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TNameYsonHas { static constexpr auto name{"YSONHas"}; };
struct TNameYsonLength { static constexpr auto name{"YSONLength"}; };
struct TNameYsonKey { static constexpr auto name{"YSONKey"}; };
struct TNameYsonType { static constexpr auto name{"YSONType"}; };
struct TNameYsonExtractInt { static constexpr auto name{"YSONExtractInt"}; };
struct TNameYsonExtractUInt { static constexpr auto name{"YSONExtractUInt"}; };
struct TNameYsonExtractFloat { static constexpr auto name{"YSONExtractFloat"}; };
struct TNameYsonExtractBool { static constexpr auto name{"YSONExtractBool"}; };
struct TNameYsonExtractString { static constexpr auto name{"YSONExtractString"}; };
struct TNameYsonExtract { static constexpr auto name{"YSONExtract"}; };
struct TNameYsonExtractKeysAndValues { static constexpr auto name{"YSONExtractKeysAndValues"}; };
struct TNameYsonExtractRaw { static constexpr auto name{"YSONExtractRaw"}; };
struct TNameYsonExtractArrayRaw { static constexpr auto name{"YSONExtractArrayRaw"}; };
struct TNameYsonExtractKeysAndValuesRaw { static constexpr auto name{"YSONExtractKeysAndValuesRaw"}; };

////////////////////////////////////////////////////////////////////////////////

REGISTER_FUNCTION(CHYT_Yson)
{
    factory.registerFunction<TFunctionYson<TNameYsonHas, TYsonHasImpl>>();
    factory.registerFunction<TFunctionYson<TNameYsonLength, TYsonLengthImpl>>();
    factory.registerFunction<TFunctionYson<TNameYsonKey, TYsonKeyImpl>>();
    factory.registerFunction<TFunctionYson<TNameYsonType, TYsonTypeImpl>>();
    factory.registerFunction<TFunctionYson<TNameYsonExtractInt, TYsonExtractInt64Impl>>();
    factory.registerFunction<TFunctionYson<TNameYsonExtractUInt, TYsonExtractUInt64Impl>>();
    factory.registerFunction<TFunctionYson<TNameYsonExtractFloat, TYsonExtractFloat64Impl>>();
    factory.registerFunction<TFunctionYson<TNameYsonExtractBool, TYsonExtractBoolImpl>>();
    factory.registerFunction<TFunctionYson<TNameYsonExtractString, TYsonExtractStringImpl>>();
    factory.registerFunction<TFunctionYson<TNameYsonExtract, TYsonExtractImpl>>();
    factory.registerFunction<TFunctionYson<TNameYsonExtractKeysAndValues, TYsonExtractKeysAndValuesImpl>>();
    factory.registerFunction<TFunctionYson<TNameYsonExtractRaw, TYsonExtractRawImpl>>();
    factory.registerFunction<TFunctionYson<TNameYsonExtractArrayRaw, TYsonExtractArrayRawImpl>>();
    factory.registerFunction<TFunctionYson<TNameYsonExtractKeysAndValuesRaw, TYsonExtractKeysAndValuesRawImpl>>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
