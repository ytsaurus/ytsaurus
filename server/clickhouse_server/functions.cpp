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
        if (!isString(removeNullable(arguments[0]))) {
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        if (!isString(removeNullable(arguments[1]))) {
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of second argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return OutputDataType_;
    }

    virtual bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    void executeImpl(Block& block, const ColumnNumbers& arguments, size_t result, size_t inputRowCount) override
    {
        const IColumn* columnYson = block.getByPosition(arguments[0]).column.get();
        const IColumn* columnPath = block.getByPosition(arguments[1]).column.get();
        const ColumnUInt8* nullMap = nullptr;
        if (checkColumn<ColumnString>(columnYson) || checkColumnConst<ColumnString>(columnYson)) {
            // Everything is just fine.
        } else if (auto* nullableColumnYson = checkAndGetColumn<ColumnNullable>(columnYson)) {
            nullMap = &nullableColumnYson->getNullMapColumn();
            columnYson = &nullableColumnYson->getNestedColumn();
        } else {
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
        }

        auto columnTo = OutputDataType_->createColumn();
        columnTo->reserve(inputRowCount);

        for (size_t i = 0; i < inputRowCount; ++i) {
            const auto& yson = columnYson->getDataAt(i);
            const auto& path = columnPath->getDataAt(i);
            auto node = ConvertToNode(TYsonString(yson.data, yson.size));

            INodePtr subNode = nullptr;
            if constexpr (Strict) {
                if (!nullMap || nullMap->getUInt(i) == 0) {
                    subNode = GetNodeByYPath(node, TString(path.data, path.size));
                } else {
                    THROW_ERROR_EXCEPTION("Cannot apply ypath function to null value");
                }
            } else {
                if (!nullMap || nullMap->getUInt(i) == 0) {
                    subNode = FindNodeByYPath(node, TString(path.data, path.size));
                }
                if (!subNode) {
                    columnTo->insertDefault();
                    continue;
                }
            }

            Field field;
            if constexpr (std::is_fundamental_v<TYTOutputType> || std::is_same_v<TYTOutputType, TString>) {
                // For primitive types we simply call GetValue<TYTOutputType>().
                TYTOutputType value;
                if constexpr (Strict) {
                    value = subNode->GetValue<TYTOutputType>();
                } else {
                    if (!NYTree::NDetail::TScalarTypeTraits<TYTOutputType>::GetValueSupportedTypes.contains(
                        subNode->GetType()))
                    {
                        columnTo->insertDefault();
                        continue;
                    }

                    try {
                        value = subNode->GetValue<TYTOutputType>();
                    } catch (const std::exception& /* ex */) {
                        // Just ignore the exception.
                        columnTo->insertDefault();
                        continue;
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
                        value = ConvertTo<TYTOutputType>(subNode);
                    } catch (const std::exception& /* ex */) {
                        // Just ignore the exception.
                        columnTo->insertDefault();
                        continue;
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

    virtual bool useDefaultImplementationForNulls() const override
    {
        return true;
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

    virtual bool useDefaultImplementationForNulls() const override
    {
        return false;
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

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(removeNullable(arguments[0]))) {
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        if (!isString(removeNullable(arguments[1]))) {
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of second argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t inputRowCount) override
    {
        const IColumn* columnYson = block.getByPosition(arguments[0]).column.get();
        const IColumn* columnFormat = block.getByPosition(arguments[1]).column.get();
        const ColumnUInt8* nullMap = nullptr;
        if (checkColumn<ColumnString>(columnYson) || checkColumnConst<ColumnString>(columnYson)) {
            // Everything is just fine.
        } else if (auto* nullableColumnYson = checkAndGetColumn<ColumnNullable>(columnYson)) {
            nullMap = &nullableColumnYson->getNullMapColumn();
            columnYson = &nullableColumnYson->getNestedColumn();
        } else {
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
        }

        auto columnTo = DataTypeString().createColumn();
        columnTo->reserve(inputRowCount);

        for (size_t i = 0; i < inputRowCount; ++i) {
            const auto& yson = columnYson->getDataAt(i);
            const auto& format = columnFormat->getDataAt(i);

            NYson::EYsonFormat ysonFormat = ConvertTo<NYson::EYsonFormat>(TString(format.data, format.size));
            auto ysonString = TYsonString(yson.data, yson.size);

            if (!nullMap || nullMap->getUInt(i) == 0) {
                columnTo->insert(toField(ConvertToYsonString(ysonString, ysonFormat).GetData()));
            } else {
                columnTo->insertDefault();
            }
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
