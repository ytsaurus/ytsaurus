#include "yt_udf.h"

#include <string.h>

void concat(
    TExpressionContext* context,
    char** result,
    int* result_len,
    char* s1,
    int s1_len,
    char* s2,
    int s2_len)
{
    *result_len = s1_len + s2_len;
    *result = AllocateBytes(context, *result_len);

    memcpy(*result, s1, s1_len);
    memcpy(*result + s1_len, s2, s2_len);
}
