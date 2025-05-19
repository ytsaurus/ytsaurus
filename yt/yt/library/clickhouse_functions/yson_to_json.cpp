#include "unescaped_yson.h"

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/json/yson/json2yson.h>
#include <library/cpp/json/json_writer.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

namespace NYT::NClickHouseServer {

using namespace NYTree;
using namespace NYson;

using namespace DB;

////////////////////////////////////////////////////////////////////////////////

class TFunctionConvertYsonToJson : public IFunction
{
public:
    static constexpr auto name = "ConvertYsonToJson";
    static FunctionPtr create(ContextPtr /*context*/)
    {
        return std::make_shared<TFunctionConvertYsonToJson>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override
    {
        return false;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes& arguments) const override
    {
        if (!isString(removeNullable(arguments[0])) && !WhichDataType(removeNullable(arguments[0])).isNothing()) {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function ,{}",
                arguments[0]->getName(),
                getName());
        }

        if (arguments[0]->isNullable()) {
            return makeNullable(std::make_shared<DataTypeString>());
        } else {
            return std::make_shared<DataTypeString>();
        }
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName& arguments, const DataTypePtr &, size_t inputRowCount) const override
    {
        const IColumn* columnYson = arguments[0].column.get();

        MutableColumnPtr columnTo;
        if (columnYson->isNullable()) {
            columnTo = makeNullable(std::make_shared<DataTypeString>())->createColumn();
        } else {
            columnTo = DataTypeString().createColumn();
        }
        columnTo->reserve(inputRowCount);

        for (size_t i = 0; i < inputRowCount; ++i) {
            if (columnYson->isNullAt(i)) {
                // Default is Null.
                columnTo->insertDefault();
                continue;
            }
            const auto& yson = columnYson->getDataAt(i);
            auto ysonString = TStringBuf(yson.data, yson.size);

            NJson::TJsonValue convertedJson;
            NJson2Yson::DeserializeYsonAsJsonValue(ysonString, &convertedJson, true);
            auto jsonString = NJson::WriteJson(&convertedJson);
            columnTo->insertData(jsonString.data(), jsonString.size());
        }

        return columnTo;
    }
};

////////////////////////////////////////////////////////////////////////////////

REGISTER_FUNCTION(CHYT_ConvertYsonToJson)
{
    factory.registerFunction<TFunctionConvertYsonToJson>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
