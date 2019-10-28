#include "functions.h"

#include "private.h"

#include <yt/core/yson/string.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/ypath_client.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

namespace DB
{

////////////////////////////////////////////////////////////////////////////////

template <>
struct NearestFieldTypeImpl<TString>
{
    using Type = std::string;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace DB

namespace NYT::NClickHouseServer {

using namespace NYTree;
using namespace NYson;

using namespace DB;

////////////////////////////////////////////////////////////////////////////////

template <class TYTOutputType, bool Strict, class TName>
class TYPathFunctionBase : public IFunction
{
public:
    static constexpr auto name = TName::Name;

    virtual std::string getName() const override
    {
        return name;
    }

    virtual size_t getNumberOfArguments() const override
    {
        return 2;
    }

    virtual DataTypePtr getReturnTypeImpl(const DataTypes& arguments) const override
    {
        if (!isString(removeNullable(arguments[0])) && !WhichDataType(removeNullable(arguments[0])).isNothing()) {
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        if (!isString(removeNullable(arguments[1])) && !WhichDataType(removeNullable(arguments[1])).isNothing()) {
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of second argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        if (OutputDataType_->canBeInsideNullable() && (arguments[0]->isNullable() || arguments[1]->isNullable())) {
            return makeNullable(OutputDataType_);
        } else {
            return OutputDataType_;
        }
    }

    virtual bool useDefaultImplementationForNulls() const override
    {
        return false;
    }

    virtual bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    void executeImpl(Block& block, const ColumnNumbers& arguments, size_t result, size_t inputRowCount) override
    {
        const IColumn* columnYsonOrNull = block.getByPosition(arguments[0]).column.get();
        const IColumn* columnYson = columnYsonOrNull;
        if (auto* nullableColumnYson = checkAndGetColumn<ColumnNullable>(columnYson)) {
            columnYson = &nullableColumnYson->getNestedColumn();
        }

        const IColumn* columnPathOrNull = block.getByPosition(arguments[1]).column.get();
        const IColumn* columnPath = columnPathOrNull;
        if (auto* nullableColumnPath = checkAndGetColumn<ColumnNullable>(columnPath)) {
            columnPath = &nullableColumnPath->getNestedColumn();
        }

        MutableColumnPtr columnTo;
        if (OutputDataType_->canBeInsideNullable() && (columnYsonOrNull->isNullable() || columnPathOrNull->isNullable())) {
            columnTo = makeNullable(OutputDataType_)->createColumn();
        } else {
            columnTo = OutputDataType_->createColumn();
        }
        columnTo->reserve(inputRowCount);

        for (size_t i = 0; i < inputRowCount; ++i) {
            if (columnYsonOrNull->isNullAt(i) || columnPathOrNull->isNullAt(i)) {
                // Default is Null if columnTo is nullable, default type value otherwise.
                columnTo->insertDefault();
                continue;
            }

            const auto& yson = columnYson->getDataAt(i);
            const auto& path = columnPath->getDataAt(i);
            auto node = ConvertToNode(TYsonString(yson.data, yson.size));

            INodePtr subNode = nullptr;
            if constexpr (Strict) {
                subNode = GetNodeByYPath(node, TString(path.data, path.size));
            } else {
                subNode = FindNodeByYPath(node, TString(path.data, path.size));
            }

            if constexpr (std::is_fundamental_v<TYTOutputType> || std::is_same_v<TYTOutputType, TString>) {
                // For primitive types we simply call GetValue<TYTOutputType>().
                TYTOutputType value = TYTOutputType();
                if constexpr (Strict) {
                    value = subNode->GetValue<TYTOutputType>();
                } else {
                    if (subNode &&
                        NYTree::NDetail::TScalarTypeTraits<TYTOutputType>::GetValueSupportedTypes
                            .contains(subNode->GetType()))
                    {
                        try {
                            value = subNode->GetValue<TYTOutputType>();
                        } catch (const std::exception& /* ex */) {
                            // Just ignore the exception.
                            value = TYTOutputType();
                        }
                    }
                }
                columnTo->insert(toField(value));
            } else {
                // For array types we use ConvertTo<std::vector<TOutputValue>>(...).
                TYTOutputType value;
                if constexpr (Strict) {
                    value = ConvertTo<TYTOutputType>(subNode);
                } else {
                    try {
                        if (subNode) {
                            value = ConvertTo<TYTOutputType>(subNode);
                        }
                    } catch (const std::exception& /* ex */) {
                        // Just ignore the exception.
                        value = TYTOutputType();
                    }
                }
                columnTo->insertData(reinterpret_cast<char*>(value.data()), value.size() * sizeof(value[0]));
            }
        }

        block.getByPosition(result).column = std::move(columnTo);
    }

protected:
    DataTypePtr OutputDataType_;
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

    static FunctionPtr create(const Context& /* context */)
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

    static FunctionPtr create(const Context& /* context */)
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

class TFunctionConvertYson : public IFunction
{
public:
    static constexpr auto name = "ConvertYson";
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<TFunctionConvertYson>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    virtual bool useDefaultImplementationForNulls() const override
    {
        return false;
    }

    virtual bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(removeNullable(arguments[0])) && !WhichDataType(removeNullable(arguments[0])).isNothing()) {
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        if (!isString(removeNullable(arguments[1]))) {
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of second argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        if (arguments[0]->isNullable()) {
            return makeNullable(std::make_shared<DataTypeString>());
        } else {
            return std::make_shared<DataTypeString>();
        }
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t inputRowCount) override
    {
        const IColumn* columnYsonOrNull = block.getByPosition(arguments[0]).column.get();
        const IColumn* columnYson = columnYsonOrNull;
        if (auto* nullableColumnYson = checkAndGetColumn<ColumnNullable>(columnYson)) {
            columnYson = &nullableColumnYson->getNestedColumn();
        }

        const IColumn* columnFormatOrNull = block.getByPosition(arguments[1]).column.get();
        const IColumn* columnFormat = columnFormatOrNull;
        if (auto* nullableColumnFormat = checkAndGetColumn<ColumnNullable>(columnFormat)) {
            columnFormat = &nullableColumnFormat->getNestedColumn();
        }
        
        MutableColumnPtr columnTo;
        if (columnYsonOrNull->isNullable()) {
            columnTo = makeNullable(std::make_shared<DataTypeString>())->createColumn();
        } else {
            columnTo = DataTypeString().createColumn();   
        }
        columnTo->reserve(inputRowCount);

        for (size_t i = 0; i < inputRowCount; ++i) {
            if (columnYsonOrNull->isNullAt(i)) {
                // Default is Null.
                columnTo->insertDefault();
                continue;
            }
            if (columnFormatOrNull->isNullAt(i)) {
                THROW_ERROR_EXCEPTION("Yson format should be not null");
            }
            const auto& yson = columnYson->getDataAt(i);
            const auto& format = columnFormat->getDataAt(i);

            NYson::EYsonFormat ysonFormat = ConvertTo<NYson::EYsonFormat>(TString(format.data, format.size));
            auto ysonString = TYsonString(yson.data, yson.size);

            columnTo->insert(toField(ConvertToYsonString(ysonString, ysonFormat).GetData()));
        }

        block.getByPosition(result).column = std::move(columnTo);
    }
};

////////////////////////////////////////////////////////////////////////////////

void RegisterFunctions()
{
    auto& factory = FunctionFactory::instance();

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

    factory.registerFunction<TFunctionConvertYson>();
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
