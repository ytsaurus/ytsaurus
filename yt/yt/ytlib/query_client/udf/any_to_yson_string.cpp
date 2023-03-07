#include "yt_udf_cpp.h"

#include <string.h>

extern "C" void AnyToYsonString(
    TExpressionContext* context,
    char** result,
    int* resultLength,
    char* any,
    int anyLength);

extern "C" void any_to_yson_string(
    TExpressionContext* context,
    char** result,
    int* resultLength,
    char* any,
    int anyLength)
{
    AnyToYsonString(context, result, resultLength, any, anyLength);
}
