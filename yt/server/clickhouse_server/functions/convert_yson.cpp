#include "ypath.h"

#include <yt/core/yson/string.h>

#include <yt/core/ytree/convert.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

namespace DB {

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

void RegisterConvertYsonFunctions()
{
    auto& factory = FunctionFactory::instance();

    factory.registerFunction<TFunctionConvertYson>();
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
