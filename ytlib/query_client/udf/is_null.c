#include <yt_udf.h>

void is_null(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* value)
{
    result->Type = Boolean;
    int8_t isnull = value->Type == Null;
    result->Data.Boolean = isnull;
}

