#include "unescaped_yson.h"

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

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
    static FunctionPtr create(ContextPtr)
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

    bool useDefaultImplementationForNulls() const override
    {
        return false;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
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

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t inputRowCount) const override
    {
        const IColumn* columnYsonOrNull = arguments[0].column.get();
        const IColumn* columnYson = columnYsonOrNull;
        if (auto* nullableColumnYson = checkAndGetColumn<ColumnNullable>(columnYson)) {
            columnYson = &nullableColumnYson->getNestedColumn();
        }

        const IColumn* columnFormatOrNull = arguments[1].column.get();
        const IColumn* columnFormat = columnFormatOrNull;
        if (auto* nullableColumnFormat = checkAndGetColumn<ColumnNullable>(columnFormat)) {
            columnFormat = &nullableColumnFormat->getNestedColumn();
        }

        bool isConstFormat = DB::isColumnConst(*columnFormat);
        EExtendedYsonFormat ysonFormat;

        // Deserializing format string can be done once if the format column is const.
        if (isConstFormat && inputRowCount > 0) {
            const auto& format = columnFormat->getDataAt(0);
            ysonFormat = ConvertTo<EExtendedYsonFormat>(TStringBuf(format.data, format.size));
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
                THROW_ERROR_EXCEPTION("YSON format should be not null");
            }

            if (!isConstFormat) {
                const auto& format = columnFormat->getDataAt(i);
                ysonFormat = ConvertTo<EExtendedYsonFormat>(TStringBuf(format.data, format.size));
            }
            const auto& yson = columnYson->getDataAt(i);
            auto ysonString = TYsonStringBuf(TStringBuf(yson.data, yson.size));

            auto convertedYsonString = ConvertToYsonStringExtendedFormat(ysonString, ysonFormat);
            columnTo->insertData(convertedYsonString.AsStringBuf().Data(), convertedYsonString.AsStringBuf().Size());
        }

        return columnTo;
    }
};

////////////////////////////////////////////////////////////////////////////////

REGISTER_FUNCTION(CHYT_ConvertYson)
{
    factory.registerFunction<TFunctionConvertYson>();
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
