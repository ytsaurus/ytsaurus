#include "yt_udf_cpp.h"

extern "C" void ToAny(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* value);

extern "C" void to_any(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* value)
{
    ToAny(context, result, value);
}
