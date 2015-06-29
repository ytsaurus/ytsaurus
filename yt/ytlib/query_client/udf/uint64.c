#include <yt_udf.h>

void uint64(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* value)
{
    result->Type = Uint64;
    if (value->Type == Int64) {
        result->Data.Uint64 = (uint64_t)value->Data.Uint64;
    } else if (value->Type == Uint64) {
        result->Data.Uint64 = (uint64_t)value->Data.Uint64;
    } else if (value->Type == Double) {
        result->Data.Uint64 = (uint64_t)value->Data.Double;
    }
}
