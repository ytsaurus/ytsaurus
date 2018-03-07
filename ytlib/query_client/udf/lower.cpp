#include "yt_udf_cpp.h"

extern "C" void ToLowerUTF8(
    TExpressionContext* context,
    char** result,
    int* result_len,
    char* s,
    int s_len);

extern "C" void lower(
    TExpressionContext* context,
    char** result,
    int* result_len,
    char* s,
    int s_len)
{
    ToLowerUTF8(context, result, result_len, s, s_len);
}
