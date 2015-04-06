#include <udf_helpers.h>

void is_null_udf(TExecutionContext* context, TUnversionedValue* result, TUnversionedValue* value)
{
    bool isnull = value->Type == Null;
    result->Type = Boolean;
    result->Data.Boolean = isnull;
}
