#include "yson_parser_adapter.h"

#include <yt/yt/core/ytree/convert.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsJSON.h>

namespace NYT::NClickHouseServer {

using namespace NYson;
using namespace NYTree;

using namespace DB;

////////////////////////////////////////////////////////////////////////////////

//! This is an analog of FunctionJSON from
//! https://github.com/ClickHouse/ClickHouse/blob/19.16/dbms/src/Functions/FunctionsJSON.h#L46
//! Functions to parse YSONs and extract values from it.
//! The first argument of all these functions gets a YSON,
//! after that there is an arbitrary number of arguments specifying path to a desired part from the YSON's root.
//! For example,
//! select YSONExtractInt('{a = "hello"; b = [-100; 200.0; 300]}', 'b', 1) = -100
template <typename Name, template<typename> typename Impl>
class TFunctionYson
    : public IFunction
    , public WithConstContext
{
public:

    TFunctionYson(ContextPtr context_)
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

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return true;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return false;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName& arguments) const override
    {
        return Impl<TYsonParserAdapter>::getReturnType(Name::name, arguments);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName& arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return FunctionJSONHelpers::Executor<Name, Impl, TYsonParserAdapter>::run(arguments, result_type, input_rows_count);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename Parser>
class TYsonExtractRawImpl
{
public:
    using Element = typename Parser::Element;

    static DataTypePtr getReturnType(const char*, const ColumnsWithTypeAndName&)
    {
        return std::make_shared<DataTypeString>();
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName& arguments)
    {
        return arguments.size() - 1;
    }

    static bool insertResultToColumn(IColumn& dest, const Element& element, const std::string_view&)
    {
        ColumnString& col_str = static_cast<ColumnString&>(dest);
        auto& chars = col_str.getChars();
        auto ysonString = ConvertToYsonString(element.GetNode());
        // Add +1 to save zero at the end of the string.
        auto ysonStringBuf = ysonString.AsStringBuf();
        chars.insert(ysonStringBuf.data(), ysonStringBuf.data() + ysonStringBuf.size() + 1);
        col_str.getOffsets().push_back(chars.size());
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename Parser>
class TYsonExtractArrayRawImpl
{
public:
    using Element = typename Parser::Element;

    static DataTypePtr getReturnType(const char*, const ColumnsWithTypeAndName&)
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName& arguments)
    {
        return arguments.size() - 1;
    }

    static bool insertResultToColumn(IColumn& dest, const Element& element, const std::string_view&)
    {
        if (!element.isArray()) {
            return false;
        }

        auto array = element.getArray();
        ColumnArray& col_res = assert_cast<ColumnArray&>(dest);

        for (auto value : array) {
            TYsonExtractRawImpl<Parser>::insertResultToColumn(col_res.getData(), value, {});
        }

        col_res.getOffsets().push_back(col_res.getOffsets().back() + array.size());
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename Parser>
class TYsonExtractKeysAndValuesRawImpl
{
public:
    using Element = typename Parser::Element;

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

    bool insertResultToColumn(IColumn& dest, const Element& element, const std::string_view&)
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
            TYsonExtractRawImpl<Parser>::insertResultToColumn(colValue, value, {});
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
    // We parametrize JSON* implementation with TYsonParserAdapter in TFunctionYson,
    // so it works fine with YSON.
    factory.registerFunction<TFunctionYson<TNameYsonHas, JSONHasImpl>>();
    factory.registerFunction<TFunctionYson<TNameYsonLength, JSONLengthImpl>>();
    factory.registerFunction<TFunctionYson<TNameYsonKey, JSONKeyImpl>>();
    factory.registerFunction<TFunctionYson<TNameYsonType, JSONTypeImpl>>();
    factory.registerFunction<TFunctionYson<TNameYsonExtractInt, JSONExtractInt64Impl>>();
    factory.registerFunction<TFunctionYson<TNameYsonExtractUInt, JSONExtractUInt64Impl>>();
    factory.registerFunction<TFunctionYson<TNameYsonExtractFloat, JSONExtractFloat64Impl>>();
    factory.registerFunction<TFunctionYson<TNameYsonExtractBool, JSONExtractBoolImpl>>();
    factory.registerFunction<TFunctionYson<TNameYsonExtractString, JSONExtractStringImpl>>();
    factory.registerFunction<TFunctionYson<TNameYsonExtract, JSONExtractImpl>>();
    factory.registerFunction<TFunctionYson<TNameYsonExtractKeysAndValues, JSONExtractKeysAndValuesImpl>>();
    // These are wrong. They serialize unpacked data to json,
    // so we use our own implementation for YSON*Raw functions.
    // factory.registerFunction<TFunctionYson<TNameYsonExtractArrayRaw, JSONExtractArrayRawImpl>>();
    // factory.registerFunction<TFunctionYson<TNameYsonExtractRaw, JSONExtractRawImpl>>();
    // factory.registerFunction<TFunctionYson<TNameYsonExtractArrayRaw, JSONExtractKeysAndValuesRawImpl>>();
    factory.registerFunction<TFunctionYson<TNameYsonExtractRaw, TYsonExtractRawImpl>>();
    factory.registerFunction<TFunctionYson<TNameYsonExtractArrayRaw, TYsonExtractArrayRawImpl>>();
    factory.registerFunction<TFunctionYson<TNameYsonExtractKeysAndValuesRaw, TYsonExtractKeysAndValuesRawImpl>>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
