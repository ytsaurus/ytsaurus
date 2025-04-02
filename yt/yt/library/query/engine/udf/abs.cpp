#include <yt/yt/library/query/misc/udf_cpp_abi.h>

using namespace NYT::NQueryClient::NUdf;

extern "C" void abs(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* data)
{
    result->Type = data->Type;

    switch (data->Type) {
        case NYT::NQueryClient::NUdf::EValueType::Int64: {
            int64_t valueAsInt = data->Data.Int64;
            result->Data.Int64 = valueAsInt < 0 ? -valueAsInt : valueAsInt;
            break;
        }
        case NYT::NQueryClient::NUdf::EValueType::Uint64: {
            result->Data.Uint64 = data->Data.Uint64;
            break;
        }
        case NYT::NQueryClient::NUdf::EValueType::Double: {
            double valueAsDouble = data->Data.Double;
            result->Data.Double = valueAsDouble < 0 ? -valueAsDouble : valueAsDouble;
            break;
        }
        case NYT::NQueryClient::NUdf::EValueType::Null:
            result->Type = NYT::NQueryClient::NUdf::EValueType::Null;
            break;
        default:
            break;
    }
}
