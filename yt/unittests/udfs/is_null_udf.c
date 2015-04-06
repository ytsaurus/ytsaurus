#include <unversioned_value.h>

void is_null_udf(TUnversionedValue* result, TUnversionedValue* value)
{
    bool isnull = value->Type == Null;
    result->Type = Boolean;
    result->Data.Boolean = isnull;
}
