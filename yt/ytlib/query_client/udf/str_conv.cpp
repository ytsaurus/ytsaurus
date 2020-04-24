#include "yt_udf_cpp.h"

extern "C" void NumericToString(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* value);

extern "C" void numeric_to_string(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* value)
{
    NumericToString(context, result, value);
}

extern "C" void StringToDouble(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* value);

extern "C" void parse_double(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* value)
{
    StringToDouble(context, result, value);
}

extern "C" void StringToInt64(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* value);

extern "C" void parse_int64(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* value)
{
    StringToInt64(context, result, value);
}

extern "C" void StringToUint64(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* value);

extern "C" void parse_uint64(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* value)
{
    StringToUint64(context, result, value);
}

