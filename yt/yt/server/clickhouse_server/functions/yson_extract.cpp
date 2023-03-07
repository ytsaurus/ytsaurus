#include "yson_extract.h"

#include "yson_parser_adapter.h"

#include <yt/core/ytree/convert.h>

#include <common/StringRef.h>
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
        return Impl<TYsonParserAdapter>::getType(Name::name, arguments);
    }

    virtual void executeImpl(Block& block, const ColumnNumbers& arguments, size_t result_pos, size_t input_rows_count) override
    {
        Executor<TYsonParserAdapter>::run(block, arguments, result_pos, input_rows_count);
    }

private:
    const Context& context;

    /// This Executor is fully copy-pasted from FunctionJSON.
    template <typename JSONParser>
    class Executor
    {
    public:
        static void run(Block & block, const ColumnNumbers & arguments, size_t result_pos, size_t input_rows_count)
        {
            MutableColumnPtr to{block.getByPosition(result_pos).type->createColumn()};
            to->reserve(input_rows_count);

            if (arguments.size() < 1)
                throw Exception{"Function " + String(Name::name) + " requires at least one argument", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

            const auto & first_column = block.getByPosition(arguments[0]);
            if (!isString(first_column.type))
                throw Exception{"The first argument of function " + String(Name::name) + " should be a string containing JSON, illegal type: " + first_column.type->getName(),
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

            const ColumnPtr & arg_json = first_column.column;
            auto col_json_const = typeid_cast<const ColumnConst *>(arg_json.get());
            auto col_json_string
                = typeid_cast<const ColumnString *>(col_json_const ? col_json_const->getDataColumnPtr().get() : arg_json.get());

            if (!col_json_string)
                throw Exception{"Illegal column " + arg_json->getName(), ErrorCodes::ILLEGAL_COLUMN};

            const ColumnString::Chars & chars = col_json_string->getChars();
            const ColumnString::Offsets & offsets = col_json_string->getOffsets();

            std::vector<Move> moves = prepareListOfMoves(block, arguments);

            /// Preallocate memory in parser if necessary.
            JSONParser parser;
            if (parser.need_preallocate)
                parser.preallocate(calculateMaxSize(offsets));

            Impl<JSONParser> impl;

            /// prepare() does Impl-specific preparation before handling each row.
            impl.prepare(Name::name, block, arguments, result_pos);

            bool json_parsed_ok = false;
            if (col_json_const)
            {
                StringRef json{reinterpret_cast<const char *>(&chars[0]), offsets[0] - 1};
                json_parsed_ok = parser.parse(json);
            }

            for (const auto i : ext::range(0, input_rows_count))
            {
                if (!col_json_const)
                {
                    StringRef json{reinterpret_cast<const char *>(&chars[offsets[i - 1]]), offsets[i] - offsets[i - 1] - 1};
                    json_parsed_ok = parser.parse(json);
                }

                bool ok = json_parsed_ok;
                if (ok)
                {
                    auto it = parser.getRoot();

                    /// Perform moves.
                    for (size_t j = 0; (j != moves.size()) && ok; ++j)
                    {
                        switch (moves[j].type)
                        {
                            case MoveType::ConstIndex:
                                ok = moveIteratorToElementByIndex(it, moves[j].index);
                                break;
                            case MoveType::ConstKey:
                                ok = moveIteratorToElementByKey(it, moves[j].key);
                                break;
                            case MoveType::Index:
                            {
                                const Field field = (*block.getByPosition(arguments[j + 1]).column)[i];
                                ok = moveIteratorToElementByIndex(it, field.get<Int64>());
                                break;
                            }
                            case MoveType::Key:
                            {
                                const Field field = (*block.getByPosition(arguments[j + 1]).column)[i];
                                ok = moveIteratorToElementByKey(it, field.get<String>().data());
                                break;
                            }
                        }
                    }

                    if (ok)
                        ok = impl.addValueToColumn(*to, it);
                }

                /// We add default value (=null or zero) if something goes wrong, we don't throw exceptions in these JSON functions.
                if (!ok)
                    to->insertDefault();
            }
            block.getByPosition(result_pos).column = std::move(to);
        }

    private:
        /// Represents a move of a JSON iterator described by a single argument passed to a JSON function.
        /// For example, the call JSONExtractInt('{"a": "hello", "b": [-100, 200.0, 300]}', 'b', 1)
        /// contains two moves: {MoveType::ConstKey, "b"} and {MoveType::ConstIndex, 1}.
        /// Keys and indices can be nonconst, in this case they are calculated for each row.
        enum class MoveType
        {
            Key,
            Index,
            ConstKey,
            ConstIndex,
        };

        struct Move
        {
            Move(MoveType type_, size_t index_ = 0) : type(type_), index(index_) {}
            Move(MoveType type_, const String & key_) : type(type_), key(key_) {}
            MoveType type;
            size_t index = 0;
            String key;
        };

        static std::vector<Move> prepareListOfMoves(Block & block, const ColumnNumbers & arguments)
        {
            constexpr size_t num_extra_arguments = Impl<JSONParser>::num_extra_arguments;
            const size_t num_moves = arguments.size() - num_extra_arguments - 1;
            std::vector<Move> moves;
            moves.reserve(num_moves);
            for (const auto i : ext::range(0, num_moves))
            {
                const auto & column = block.getByPosition(arguments[i + 1]);
                if (!isString(column.type) && !isInteger(column.type))
                    throw Exception{"The argument " + std::to_string(i + 2) + " of function " + String(Name::name)
                                        + " should be a string specifying key or an integer specifying index, illegal type: " + column.type->getName(),
                                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

                if (isColumnConst(*column.column))
                {
                    const auto & column_const = static_cast<const ColumnConst &>(*column.column);
                    if (isString(column.type))
                        moves.emplace_back(MoveType::ConstKey, column_const.getField().get<String>());
                    else
                        moves.emplace_back(MoveType::ConstIndex, column_const.getField().get<Int64>());
                }
                else
                {
                    if (isString(column.type))
                        moves.emplace_back(MoveType::Key, "");
                    else
                        moves.emplace_back(MoveType::Index, 0);
                }
            }
            return moves;
        }

        using Iterator = typename JSONParser::Iterator;

        /// Performs moves of types MoveType::Index and MoveType::ConstIndex.
        static bool moveIteratorToElementByIndex(Iterator & it, int index)
        {
            if (JSONParser::isArray(it))
            {
                if (index > 0)
                    return JSONParser::arrayElementByIndex(it, index - 1);
                else
                    return JSONParser::arrayElementByIndex(it, JSONParser::sizeOfArray(it) + index);
            }
            if (JSONParser::isObject(it))
            {
                if (index > 0)
                    return JSONParser::objectMemberByIndex(it, index - 1);
                else
                    return JSONParser::objectMemberByIndex(it, JSONParser::sizeOfObject(it) + index);
            }
            return false;
        }

        /// Performs moves of types MoveType::Key and MoveType::ConstKey.
        static bool moveIteratorToElementByKey(Iterator & it, const String & key)
        {
            if (JSONParser::isObject(it))
                return JSONParser::objectMemberByName(it, key);
            return false;
        }

        static size_t calculateMaxSize(const ColumnString::Offsets & offsets)
        {
            size_t max_size = 0;
            for (const auto i : ext::range(0, offsets.size()))
                if (max_size < offsets[i] - offsets[i - 1])
                    max_size = offsets[i] - offsets[i - 1];

            if (max_size < 1)
                max_size = 1;
            return max_size;
        }
    };
};

////////////////////////////////////////////////////////////////////////////////

template <typename Parser>
class TYsonExtractRawImpl
{
public:
    static DataTypePtr getType(const char *, const ColumnsWithTypeAndName &)
    {
        return std::make_shared<DataTypeString>();
    }

    using Iterator = typename Parser::Iterator;
    static bool addValueToColumn(IColumn & dest, const Iterator & it)
    {
        ColumnString & col_str = static_cast<ColumnString &>(dest);
        auto & chars = col_str.getChars();
        auto ysonString = ConvertToYsonString(it.Value);
        // Add +1 to save zero at the end of the string.
        chars.insert(ysonString.GetData().data(), ysonString.GetData().data() + ysonString.GetData().size() + 1);
        col_str.getOffsets().push_back(chars.size());
        return true;
    }

    static constexpr size_t num_extra_arguments = 0;
    static void prepare(const char *, const Block &, const ColumnNumbers &, size_t) {}
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
    // This is wrong. It converts unpacked data to json, so we use our own implementation for YSONExtractRaw.
    // factory.registerFunction<TFunctionYson<TNameYsonExtractRaw, JSONExtractRawImpl>>();
    factory.registerFunction<TFunctionYson<TNameYsonExtractRaw, TYsonExtractRawImpl>>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
