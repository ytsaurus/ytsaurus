#include <yt_udf.h>

void double_cast(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* value)
{
    result->Type = Double;
    if (value->Type == Int64) {
        result->Data.Double = (double)value->Data.Int64;
    } else if (value->Type == Uint64) {
        result->Data.Double = (double)value->Data.Uint64;
    } else if (value->Type == Double) {
        result->Data.Double = value->Data.Double;
    }
}
