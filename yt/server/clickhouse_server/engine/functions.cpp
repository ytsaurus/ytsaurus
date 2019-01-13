#include "functions.h"

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

namespace DB::ErrorCodes
{

////////////////////////////////////////////////////////////////////////////////

extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;

////////////////////////////////////////////////////////////////////////////////

} // namespace DB::ErrorCodes

namespace DB
{

////////////////////////////////////////////////////////////////////////////////

template <>
struct NearestFieldType<TString>
{
    using Type = std::string;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace DB

namespace NYT::NClickHouseServer::NEngine {

using namespace NYTree;
using namespace NYson;

using namespace DB;

////////////////////////////////////////////////////////////////////////////////

template <class TYTOutputType, bool Strict>
class TYPathFunctionBase : public IFunction
{
public:
    virtual std::string getName() const override
    {
        return "TYPathFunctionBase";
    }

    virtual size_t getNumberOfArguments() const override
    {
        return 2;
    }

    virtual DataTypePtr getReturnTypeImpl(const DataTypes& arguments) const override
    {
        if (!isString(arguments[0])) {
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        if (!isString(arguments[1])) {
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of second argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return OutputDataType_;
    }

    virtual bool useDefaultImplementationForNulls() const override
    {
        return false;
    }

    virtual bool useDefaultImplementationForConstants() const override
    {
        return false;
    }

    void executeImpl(Block& block, const ColumnNumbers& arguments, size_t result, size_t inputRowCount) override
    {
        const IColumn* columnYson = block.getByPosition(arguments[0]).column.get();
        const IColumn* columnPath = block.getByPosition(arguments[1]).column.get();
        if (!checkColumn<ColumnString>(columnYson) && !checkColumnConst<ColumnString>(columnYson)) {
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[0]).column->getName() + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
        }
        if (!checkColumn<ColumnString>(columnPath) && !checkColumnConst<ColumnString>(columnPath)) {
            throw Exception(
                "Illegal column " + block.getByPosition(arguments[1]).column->getName() + " of second argument of function " + getName(),
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
                subNode = GetNodeByYPath(node, TString(path.data, path.size));
            } else {
                subNode = FindNodeByYPath(node, TString(path.data, path.size));
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

template <class TCHOutputDataType, class TYTOutputType, bool Strict>
class TScalarYPathFunction
    : public TYPathFunctionBase<TYTOutputType, Strict>
{
public:
    TScalarYPathFunction()
    {
        this->OutputDataType_ = std::make_shared<TCHOutputDataType>();
        if constexpr (!Strict) {
            this->OutputDataType_ = makeNullable(this->OutputDataType_);
        }
    }

    static FunctionPtr create(const Context& /* context */)
    {
        return std::make_shared<TScalarYPathFunction>();
    }
};

template <class TCHOutputElementDataType, class TYTOutputType, bool Strict>
class TArrayYPathFunction
    : public TYPathFunctionBase<TYTOutputType, Strict>
{
public:
    TArrayYPathFunction()
    {
        this->OutputDataType_ = std::make_shared<DataTypeArray>(std::make_shared<TCHOutputElementDataType>());
        if constexpr (!Strict) {
            this->OutputDataType_ = makeNullable(this->OutputDataType_);
        }
    }

    static FunctionPtr create(const Context& /* context */)
    {
        return std::make_shared<TArrayYPathFunction>();
    }
};

////////////////////////////////////////////////////////////////////////////////

void RegisterFunctions()
{
    auto& factory = FunctionFactory::instance();

    factory.registerFunction<TScalarYPathFunction<DataTypeInt64, i64, true>>("YPathInt64Strict");
    factory.registerFunction<TScalarYPathFunction<DataTypeUInt64, ui64, true>>("YPathUInt64Strict");
    factory.registerFunction<TScalarYPathFunction<DataTypeUInt8, bool, true>>("YPathBooleanStrict");
    factory.registerFunction<TScalarYPathFunction<DataTypeFloat64, double, true>>("YPathDoubleStrict");
    factory.registerFunction<TScalarYPathFunction<DataTypeString, TString, true>>("YPathStringStrict");

    factory.registerFunction<TArrayYPathFunction<DataTypeInt64, std::vector<i64>, true>>("YPathArrayInt64Strict");
    factory.registerFunction<TArrayYPathFunction<DataTypeUInt64, std::vector<ui64>, true>>("YPathArrayUInt64Strict");
    factory.registerFunction<TArrayYPathFunction<DataTypeUInt8, std::vector<bool>, true>>("YPathArrayBooleanStrict");
    factory.registerFunction<TArrayYPathFunction<DataTypeFloat64, std::vector<double>, true>>("YPathArrayDoubleStrict");

    factory.registerFunction<TScalarYPathFunction<DataTypeInt64, i64, false>>("YPathInt64");
    factory.registerFunction<TScalarYPathFunction<DataTypeUInt64, ui64, false>>("YPathUInt64");
    factory.registerFunction<TScalarYPathFunction<DataTypeUInt8, bool, false>>("YPathBoolean");
    factory.registerFunction<TScalarYPathFunction<DataTypeFloat64, double, false>>("YPathDouble");
    factory.registerFunction<TScalarYPathFunction<DataTypeString, TString, false>>("YPathString");

    factory.registerFunction<TArrayYPathFunction<DataTypeInt64, std::vector<i64>, false>>("YPathArrayInt64");
    factory.registerFunction<TArrayYPathFunction<DataTypeUInt64, std::vector<ui64>, false>>("YPathArrayUInt64");
    factory.registerFunction<TArrayYPathFunction<DataTypeUInt8, std::vector<bool>, false>>("YPathArrayBoolean64");
    factory.registerFunction<TArrayYPathFunction<DataTypeFloat64, std::vector<double>, false>>("YPathArrayDouble64");
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NEngine
