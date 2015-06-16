#include <yt_udf.h>

void int64(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* value)
{
    result->Type = Int64;
    if (value->Type == Int64) {
        result->Data.Int64 = value->Data.Int64;
    } else if (value->Type == Uint64) {
        result->Data.Int64 = (int64_t)value->Data.Uint64;
    } else if (value->Type == Double) {
        result->Data.Int64 = (int64_t)value->Data.Double;
    }
}
