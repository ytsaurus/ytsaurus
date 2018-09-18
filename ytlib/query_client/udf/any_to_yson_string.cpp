#include "yt_udf_cpp.h"

#include <string.h>

extern "C" void any_to_yson_string(
    TExpressionContext* context,
    char** result,
    int* resultLength,
    char* any,
    int anyLength)
{
    *resultLength = anyLength;
    *result = AllocateBytes(context, *resultLength);
    memcpy(*result, any, anyLength);
}
