#include "yson_parser_adapter.h"
#include "unescaped_yson.h"

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_client.h>
#include <yt/yt/core/ytree/ypath_resolver.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsJSON.h>
#include <Functions/IFunction.h>

namespace DB {

////////////////////////////////////////////////////////////////////////////////

template <>
struct NearestFieldTypeImpl<TString>
{
    using Type = std::string;
};

////////////////////////////////////////////////////////////////////////////////

namespace ErrorCodes {

////////////////////////////////////////////////////////////////////////////////

extern const int CANNOT_SELECT;
extern const int INCOMPATIBLE_COLUMNS;
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
extern const int UNKNOWN_TYPE;
extern const int IP_ADDRESS_NOT_ALLOWED;
extern const int UNKNOWN_USER;
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NO_SUCH_COLUMN_IN_TABLE;

////////////////////////////////////////////////////////////////////////////////

} // namespace ErrorCodes


} // namespace DB

namespace NYT::NClickHouseServer {

using namespace NYTree;
using namespace NYson;

using namespace DB;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class T>
bool ParseArray(TYsonPullParserCursor* cursor, std::vector<T>* array)
{
    array->clear();
    if ((*cursor)->GetType() != EYsonItemType::BeginList) {
        return false;
    }
    cursor->Next();

    while ((*cursor)->GetType() != EYsonItemType::EndList) {
        if (auto value = TryParseValue<T>(cursor)) {
            array->emplace_back(std::move(*value));
        } else {
            return false;
        }
        cursor->Next();
    }
    cursor->Next();
    return true;
}

template <class T>
bool TryGetArray(TStringBuf yson, const NYPath::TYPath& ypath, std::vector<T>* array)
{
    if (auto maybeAny = TryGetAny(yson, ypath)) {
        auto ysonStringBuf = TYsonStringBuf(*maybeAny);
        TMemoryInput input(ysonStringBuf.AsStringBuf());
        TYsonPullParser parser(&input, ysonStringBuf.GetType());
        TYsonPullParserCursor cursor(&parser);
        if (ParseArray(&cursor, array)) {
            return true;
        }
    }
    return false;
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class TYTOutputType, bool Strict, class TName>
class TYPathFunctionBase : public IFunction
{
public:
    static constexpr auto name = TName::Name;

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return true;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes& arguments) const override
    {
        if (!isString(removeNullable(arguments[0])) && !WhichDataType(removeNullable(arguments[0])).isNothing()) {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}",
                arguments[0]->getName(),
                getName());
        }
        if (!isString(removeNullable(arguments[1])) && !WhichDataType(removeNullable(arguments[1])).isNothing()) {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}",
                arguments[1]->getName(),
                getName());
        }

        if (OutputDataType_->canBeInsideNullable() && (arguments[0]->isNullable() || arguments[1]->isNullable())) {
            return makeNullable(OutputDataType_);
        } else {
            return OutputDataType_;
        }
    }

    bool useDefaultImplementationForNulls() const override
    {
        return false;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName& arguments, const DataTypePtr& resultType, size_t inputRowCount) const override
    {
        const IColumn* columnYsonOrNull = arguments[0].column.get();
        const IColumn* columnYson = columnYsonOrNull;
        if (auto* nullableColumnYson = checkAndGetColumn<ColumnNullable>(columnYson)) {
            columnYson = &nullableColumnYson->getNestedColumn();
        }

        const IColumn* columnPathOrNull = arguments[1].column.get();
        const IColumn* columnPath = columnPathOrNull;
        if (auto* nullableColumnPath = checkAndGetColumn<ColumnNullable>(columnPath)) {
            columnPath = &nullableColumnPath->getNestedColumn();
        }

        auto columnTo = resultType->createColumn();
        columnTo->reserve(inputRowCount);

        TYTOutputType value{};
        for (size_t i = 0; i < inputRowCount; ++i) {
            if (columnYsonOrNull->isNullAt(i) || columnPathOrNull->isNullAt(i)) {
                // Default is Null if columnTo is nullable, default type value otherwise.
                columnTo->insertDefault();
                continue;
            }

            const auto& yson = columnYson->getDataAt(i);
            const auto& path = columnPath->getDataAt(i);
            constexpr bool isFundumental = std::is_fundamental_v<TYTOutputType> || std::is_same_v<TYTOutputType, TString>;
            bool dataIsFound = TryGetData(TStringBuf(yson.data, yson.size), TYPath(path.data, path.size), &value);

            if (dataIsFound) {
                if constexpr (isFundumental) {
                    columnTo->insert(toField(value));
                } else {
                    // TODO(dakovalkov): This looks weird.
                    // NB: Arrays are only not fundumental types which can be passed as TYTOutputType here.
                    columnTo->insertData(reinterpret_cast<char*>(value.data()), value.size() * sizeof(value[0]));
                }
            } else {
                if (Strict) {
                    THROW_ERROR_EXCEPTION("Failed to extract value from yson")
                        << TErrorAttribute("yson", TStringBuf(yson.data, yson.size))
                        << TErrorAttribute("path", TStringBuf(path.data, path.size));
                }
                columnTo->insertDefault();
            }
        }

        return columnTo;
    }

protected:
    DataTypePtr OutputDataType_;

private:
    static bool TryGetData(TStringBuf yson, const NYPath::TYPath& ypath, TYTOutputType* data)
    {
        constexpr bool isFundumental = std::is_fundamental_v<TYTOutputType> || std::is_same_v<TYTOutputType, TString>;
        if constexpr (isFundumental) {
            if (auto maybeValue = TryGetValue<TYTOutputType>(yson, ypath)) {
                *data = std::move(*maybeValue);
                return true;
            }
            return false;
        } else {
            return NDetail::TryGetArray(yson, ypath, data);
        }
    }
};

template <class TCHOutputDataType, class TYTOutputType, bool Strict, class TName>
class TScalarYPathFunction
    : public TYPathFunctionBase<TYTOutputType, Strict, TName>
{
public:
    TScalarYPathFunction()
    {
        this->OutputDataType_ = std::make_shared<TCHOutputDataType>();
    }

    static FunctionPtr create(ContextPtr /*context*/)
    {
        return std::make_shared<TScalarYPathFunction>();
    }
};

template <class TCHOutputElementDataType, class TYTOutputType, bool Strict, class TName>
class TArrayYPathFunction
    : public TYPathFunctionBase<TYTOutputType, Strict, TName>
{
public:
    TArrayYPathFunction()
    {
        this->OutputDataType_ = std::make_shared<DataTypeArray>(std::make_shared<TCHOutputElementDataType>());
    }

    static FunctionPtr create(ContextPtr /* context */)
    {
        return std::make_shared<TArrayYPathFunction>();
    }
};

////////////////////////////////////////////////////////////////////////////////

// The boilerplate code below is an adaptation of similar technique from
// https://github.com/yandex/ClickHouse/blob/master/dbms/src/Functions/FunctionsExternalDictionaries.h#L867
// Note that we should implement not only the virtual getName() const method of the class,
// but also the constexpr name field, which is taken from the TName template argument.
struct TNameYPathInt64Strict { static constexpr auto Name = "YPathInt64Strict"; };
struct TNameYPathUInt64Strict { static constexpr auto Name = "YPathUInt64Strict"; };
struct TNameYPathBooleanStrict { static constexpr auto Name = "YPathBooleanStrict"; };
struct TNameYPathDoubleStrict { static constexpr auto Name = "YPathDoubleStrict"; };
struct TNameYPathStringStrict { static constexpr auto Name = "YPathStringStrict"; };

struct TNameYPathInt64 { static constexpr auto Name = "YPathInt64"; };
struct TNameYPathUInt64 { static constexpr auto Name = "YPathUInt64"; };
struct TNameYPathBoolean { static constexpr auto Name = "YPathBoolean"; };
struct TNameYPathDouble { static constexpr auto Name = "YPathDouble"; };
struct TNameYPathString { static constexpr auto Name = "YPathString"; };

struct TNameYPathArrayInt64Strict { static constexpr auto Name = "YPathArrayInt64Strict"; };
struct TNameYPathArrayUInt64Strict { static constexpr auto Name = "YPathArrayUInt64Strict"; };
struct TNameYPathArrayBooleanStrict { static constexpr auto Name = "YPathArrayBooleanStrict"; };
struct TNameYPathArrayDoubleStrict { static constexpr auto Name = "YPathArrayDoubleStrict"; };

struct TNameYPathArrayInt64 { static constexpr auto Name = "YPathArrayInt64"; };
struct TNameYPathArrayUInt64 { static constexpr auto Name = "YPathArrayUInt64"; };
struct TNameYPathArrayBoolean { static constexpr auto Name = "YPathArrayBoolean"; };
struct TNameYPathArrayDouble { static constexpr auto Name = "YPathArrayDouble"; };


using TFunctionYPathInt64Strict = TScalarYPathFunction<DataTypeInt64, i64, true, TNameYPathInt64Strict>;
using TFunctionYPathUInt64Strict = TScalarYPathFunction<DataTypeUInt64, ui64, true, TNameYPathUInt64Strict>;
using TFunctionYPathBooleanStrict = TScalarYPathFunction<DataTypeUInt8, bool, true, TNameYPathBooleanStrict>;
using TFunctionYPathDoubleStrict = TScalarYPathFunction<DataTypeFloat64, double, true, TNameYPathDoubleStrict>;
using TFunctionYPathStringStrict = TScalarYPathFunction<DataTypeString, TString, true, TNameYPathStringStrict>;

using TFunctionYPathInt64 = TScalarYPathFunction<DataTypeInt64, i64, false, TNameYPathInt64>;
using TFunctionYPathUInt64 = TScalarYPathFunction<DataTypeUInt64, ui64, false, TNameYPathUInt64>;
using TFunctionYPathBoolean = TScalarYPathFunction<DataTypeUInt8, bool, false, TNameYPathBoolean>;
using TFunctionYPathDouble = TScalarYPathFunction<DataTypeFloat64, double, false, TNameYPathDouble>;
using TFunctionYPathString = TScalarYPathFunction<DataTypeString, TString, false, TNameYPathString>;

using TFunctionYPathArrayInt64Strict = TArrayYPathFunction<DataTypeInt64, std::vector<i64>, true, TNameYPathArrayInt64Strict>;
using TFunctionYPathArrayUInt64Strict = TArrayYPathFunction<DataTypeUInt64, std::vector<ui64>, true, TNameYPathArrayUInt64Strict>;
using TFunctionYPathArrayBooleanStrict = TArrayYPathFunction<DataTypeUInt8, std::vector<bool>, true, TNameYPathArrayBooleanStrict>;
using TFunctionYPathArrayDoubleStrict = TArrayYPathFunction<DataTypeFloat64, std::vector<double>, true, TNameYPathArrayDoubleStrict>;

using TFunctionYPathArrayInt64 = TArrayYPathFunction<DataTypeInt64, std::vector<i64>, false, TNameYPathArrayInt64>;
using TFunctionYPathArrayUInt64 = TArrayYPathFunction<DataTypeUInt64, std::vector<ui64>, false, TNameYPathArrayUInt64>;
using TFunctionYPathArrayBoolean = TArrayYPathFunction<DataTypeUInt8, std::vector<bool>, false, TNameYPathArrayBoolean>;
using TFunctionYPathArrayDouble = TArrayYPathFunction<DataTypeFloat64, std::vector<double>, false, TNameYPathArrayDouble>;

////////////////////////////////////////////////////////////////////////////////

template <bool Strict, class TName>
class TFunctionYPathRawImpl : public IFunction
{
public:
    static constexpr auto name = TName::Name;

    std::string getName() const override
    {
        return name;
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

    static FunctionPtr create(ContextPtr /*context*/)
    {
        return std::make_shared<TFunctionYPathRawImpl>();
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName& arguments) const override
    {
        if (arguments.size() < 2) {
            throw Exception(
                ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                "Too few arguments, should be at least 2");
        }
        if (arguments.size() > 3) {
            throw Exception(
                ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION,
                "Too many arguments, should be at most 3");
        }

        if (!isString(removeNullable(arguments[0].type)) && !WhichDataType(removeNullable(arguments[0].type)).isNothing()) {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}",
                arguments[0].type->getName(),
                getName());
        }
        if (!isString(removeNullable(arguments[1].type)) && !WhichDataType(removeNullable(arguments[1].type)).isNothing()) {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}",
                arguments[1].type->getName(),
                getName());
        }
        if (arguments.size() == 3) {
            if (!isString(removeNullable(arguments[2].type)) && !WhichDataType(removeNullable(arguments[2].type)).isNothing()) {
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of third argument of function {}",
                    arguments[2].type->getName(),
                    getName());
            }
        }

        // If the path doesn't exist and the function isn't strict, we return Null.
        if (!Strict || arguments[0].type->isNullable() || arguments[1].type->isNullable() || arguments[2].type->isNullable()) {
            return makeNullable(std::make_shared<DataTypeString>());
        } else {
            return std::make_shared<DataTypeString>();
        }
    }

    bool useDefaultImplementationForNulls() const override
    {
        return false;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName& arguments, const DataTypePtr & resultType, size_t inputRowCount) const override
    {
        const IColumn* columnYsonOrNull = arguments[0].column.get();
        const IColumn* columnYson = columnYsonOrNull;
        if (auto* nullableColumnYson = checkAndGetColumn<ColumnNullable>(columnYson)) {
            columnYson = &nullableColumnYson->getNestedColumn();
        }

        const IColumn* columnPathOrNull = arguments[1].column.get();
        const IColumn* columnPath = columnPathOrNull;
        if (auto* nullableColumnPath = checkAndGetColumn<ColumnNullable>(columnPath)) {
            columnPath = &nullableColumnPath->getNestedColumn();
        }

        EExtendedYsonFormat ysonFormat = EExtendedYsonFormat::Binary;
        const IColumn* columnFormatOrNull = nullptr;
        const IColumn* columnFormat = nullptr;
        if (arguments.size() == 3) {
            columnFormatOrNull = arguments[2].column.get();
            columnFormat = columnFormatOrNull;
            if (auto* nullableColumnFormat = checkAndGetColumn<ColumnNullable>(columnFormat)) {
                columnFormat = &nullableColumnFormat->getNestedColumn();
            }
            if (DB::isColumnConst(*columnFormat) && inputRowCount > 0) {
                const auto& format = columnFormat->getDataAt(0);
                ysonFormat = ConvertTo<EExtendedYsonFormat>(TStringBuf(format.data, format.size));
                columnFormat = nullptr;
            }
        }

        auto columnTo = resultType->createColumn();
        columnTo->reserve(inputRowCount);

        for (size_t i = 0; i < inputRowCount; ++i) {
            if (columnYsonOrNull->isNullAt(i) || columnPathOrNull->isNullAt(i) || (columnFormat && columnFormatOrNull->isNullAt(i))) {
                // Default is Null.
                columnTo->insertDefault();
                continue;
            }

            if (columnFormat) {
                const auto& format = columnFormat->getDataAt(i);
                ysonFormat = ConvertTo<EExtendedYsonFormat>(TStringBuf(format.data, format.size));
            }

            const auto& yson = columnYson->getDataAt(i);
            const auto& path = columnPath->getDataAt(i);
            auto node = ConvertToNode(TYsonStringBuf(TStringBuf(yson.data, yson.size)));

            INodePtr subNode;
            if constexpr (Strict) {
                try {
                    subNode = GetNodeByYPath(node, TString(path.data, path.size));
                } catch (const std::exception& ex) {
                    // Rethrow the error with additional context.
                    THROW_ERROR_EXCEPTION("Failed to extract value from yson")
                        << TErrorAttribute("yson", TStringBuf(yson.data, yson.size))
                        << TErrorAttribute("path", TStringBuf(path.data, path.size))
                        << ex;
                }
            } else {
                TNodeWalkOptions options = FindNodeByYPathOptions;
                options.NodeCannotHaveChildrenHandler = [] (const INodePtr& /*node*/) {
                    return nullptr;
                };
                subNode = WalkNodeByYPath(node, TString(path.data, path.size), options);
            }

            if (subNode) {
                auto convertedYson = ConvertToYsonStringExtendedFormat(subNode, ysonFormat);
                columnTo->insertData(convertedYson.AsStringBuf().Data(), convertedYson.AsStringBuf().Size());
            } else {
                columnTo->insertDefault();
            }
        }

        return columnTo;
    }
};

////////////////////////////////////////////////////////////////////////////////

// TODO(dakovalkov): Strict version is a fake. It does not detect all possible errors.
// Support the real strict version when users expose us.
template <bool Strict, class TName>
class TFunctionYPathExtractImpl : public IFunction
{
public:
    static constexpr auto name = TName::Name;

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 3;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return true;
    }

    static FunctionPtr create(ContextPtr /* context */)
    {
        return std::make_shared<TFunctionYPathExtractImpl>();
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName& arguments) const override
    {
        if (!isString(removeNullable(arguments[0].type)) && !WhichDataType(removeNullable(arguments[0].type)).isNothing()) {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}",
                arguments[0].type->getName(),
                getName());
        }
        if (!isString(removeNullable(arguments[1].type)) && !WhichDataType(removeNullable(arguments[1].type)).isNothing()) {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}",
                arguments[1].type->getName(),
                getName());
        }
        const auto& type = arguments[2];
        auto typeConst = typeid_cast<const ColumnConst *>(type.column.get());
        if (!typeConst || !isString(type.type)) {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of third argument of function {}, only const string is supported",
                type.type->getName(),
                getName());
        }

        return DataTypeFactory::instance().get(typeConst->getValue<String>());
    }

    bool useDefaultImplementationForNulls() const override
    {
        return false;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName& arguments, const DataTypePtr & returnType, size_t inputRowCount) const override
    {
        const IColumn* columnYsonOrNull = arguments[0].column.get();
        const IColumn* columnYson = columnYsonOrNull;
        if (auto* nullableColumnYson = checkAndGetColumn<ColumnNullable>(columnYson)) {
            columnYson = &nullableColumnYson->getNestedColumn();
        }

        const IColumn* columnPathOrNull = arguments[1].column.get();
        const IColumn* columnPath = columnPathOrNull;
        if (auto* nullableColumnPath = checkAndGetColumn<ColumnNullable>(columnPath)) {
            columnPath = &nullableColumnPath->getNestedColumn();
        }

        auto extractTree = JSONExtractTree<TYsonParserAdapter>::build(name, returnType);

        auto columnTo = returnType->createColumn();
        columnTo->reserve(inputRowCount);

        for (size_t i = 0; i < inputRowCount; ++i) {
            if (columnYsonOrNull->isNullAt(i) || columnPathOrNull->isNullAt(i)) {
                // Default is Null.
                columnTo->insertDefault();
                continue;
            }

            const auto& yson = columnYson->getDataAt(i);
            const auto& path = columnPath->getDataAt(i);
            auto node = ConvertToNode(TYsonStringBuf(TStringBuf(yson.data, yson.size)));

            INodePtr subNode;
            if constexpr (Strict) {
                try {
                    subNode = GetNodeByYPath(node, TString(path.data, path.size));
                } catch (const std::exception& ex) {
                    // Rethrow the error with additional context.
                    THROW_ERROR_EXCEPTION("Failed to extract value from yson")
                        << TErrorAttribute("yson", TStringBuf(yson.data, yson.size))
                        << TErrorAttribute("path", TStringBuf(path.data, path.size))
                        << ex;
                }
            } else {
                TNodeWalkOptions options = FindNodeByYPathOptions;
                options.NodeCannotHaveChildrenHandler = [] (const INodePtr& /*node*/) {
                    return nullptr;
                };
                subNode = WalkNodeByYPath(node, TString(path.data, path.size), options);
            }

            if (!subNode || !extractTree->insertResultToColumn(*columnTo, subNode)) {
                if constexpr (Strict) {
                    THROW_ERROR_EXCEPTION("Error converting extracted value")
                        << TErrorAttribute("yson", TStringBuf(yson.data, yson.size))
                        << TErrorAttribute("path", TStringBuf(path.data, path.size));
                } else {
                    // Just ignore errors.
                    columnTo->insertDefault();
                }
            }
        }

        return columnTo;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TNameYPathRawStrict { static constexpr auto Name = "YPathRawStrict"; };
struct TNameYPathExtractStrict { static constexpr auto Name = "YPathExtractStrict"; };

struct TNameYPathRaw { static constexpr auto Name = "YPathRaw"; };
struct TNameYPathExtract { static constexpr auto Name = "YPathExtract"; };

////////////////////////////////////////////////////////////////////////////////

using TFunctionYPathRawStrict = TFunctionYPathRawImpl<true, TNameYPathRawStrict>;
using TFunctionYPathExtractStrict = TFunctionYPathExtractImpl<true, TNameYPathExtractStrict>;

using TFunctionYPathRaw = TFunctionYPathRawImpl<false, TNameYPathRaw>;
using TFunctionYPathExtract = TFunctionYPathExtractImpl<false, TNameYPathExtract>;

////////////////////////////////////////////////////////////////////////////////

REGISTER_FUNCTION(CHYT_YPath)
{
    factory.registerFunction<TFunctionYPathInt64Strict>();
    factory.registerFunction<TFunctionYPathUInt64Strict>();
    factory.registerFunction<TFunctionYPathBooleanStrict>();
    factory.registerFunction<TFunctionYPathDoubleStrict>();
    factory.registerFunction<TFunctionYPathStringStrict>();

    factory.registerFunction<TFunctionYPathInt64>();
    factory.registerFunction<TFunctionYPathUInt64>();
    factory.registerFunction<TFunctionYPathBoolean>();
    factory.registerFunction<TFunctionYPathDouble>();
    factory.registerFunction<TFunctionYPathString>();

    factory.registerFunction<TFunctionYPathArrayInt64Strict>();
    factory.registerFunction<TFunctionYPathArrayUInt64Strict>();
    factory.registerFunction<TFunctionYPathArrayDoubleStrict>();
    factory.registerFunction<TFunctionYPathArrayBooleanStrict>();

    factory.registerFunction<TFunctionYPathArrayInt64>();
    factory.registerFunction<TFunctionYPathArrayUInt64>();
    factory.registerFunction<TFunctionYPathArrayDouble>();
    factory.registerFunction<TFunctionYPathArrayBoolean>();

    factory.registerFunction<TFunctionYPathRawStrict>();
    factory.registerFunction<TFunctionYPathExtractStrict>();

    factory.registerFunction<TFunctionYPathRaw>();
    factory.registerFunction<TFunctionYPathExtract>();
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
