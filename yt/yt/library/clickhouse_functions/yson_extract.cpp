#include "yson_extract.h"

#include "yson_parser_adapter.h"

#include <yt/core/ytree/convert.h>

#include <Functions/FunctionsJSON.h>
#include <Functions/FunctionFactory.h>

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
class TFunctionYson : public IFunction
{
public:

    TFunctionYson(const Context& context_) : context(context_)
    { }

    static constexpr auto name = Name::name;

    static FunctionPtr create(const Context& context_)
    {
        return std::make_shared<TFunctionYson>(context_);
    }

    virtual String getName() const override
    {
        return Name::name;
    }

    virtual bool isVariadic() const override
    {
        return true;
    }

    virtual size_t getNumberOfArguments() const override
    {
        return 0;
    }

    virtual bool useDefaultImplementationForConstants() const override
    {
        return false;
    }

    virtual DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName& arguments) const override
    {
        return Impl<TYsonParserAdapter>::getReturnType(Name::name, arguments);
    }

    virtual ColumnPtr executeImpl(const ColumnsWithTypeAndName& arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return FunctionJSONHelpers::Executor<Name, Impl, TYsonParserAdapter>::run(arguments, result_type, input_rows_count);
    }

private:
    const Context& context;
};

////////////////////////////////////////////////////////////////////////////////

template <typename Parser>
class TYsonExtractRawImpl
{
public:
    using Element = typename Parser::Element;

    static DataTypePtr getReturnType(const char *, const ColumnsWithTypeAndName &)
    {
        return std::make_shared<DataTypeString>();
    }

    static size_t getNumberOfIndexArguments(const ColumnsWithTypeAndName & arguments) { return arguments.size() - 1; }

    static bool insertResultToColumn(IColumn & dest, const Element & element, const std::string_view &)
    {
        ColumnString & col_str = static_cast<ColumnString &>(dest);
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

////////////////////////////////////////////////////////////////////////////////

void RegisterYsonExtractFunctions()
{
    auto& factory = FunctionFactory::instance();

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
    factory.registerFunction<TFunctionYson<TNameYsonExtractArrayRaw, JSONExtractArrayRawImpl>>();
    // This is wrong. It converts unpacked data to json, so we use our own implementation for YSONExtractRaw.
    // factory.registerFunction<TFunctionYson<TNameYsonExtractRaw, JSONExtractRawImpl>>();
    factory.registerFunction<TFunctionYson<TNameYsonExtractRaw, TYsonExtractRawImpl>>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
