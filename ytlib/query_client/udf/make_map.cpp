#include "yt_udf_cpp.h"

extern "C" void MakeMap(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* args,
    int argCount);

extern "C" void make_map(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* args,
    int argCount)
{
    MakeMap(context, result, args, argCount);
}
