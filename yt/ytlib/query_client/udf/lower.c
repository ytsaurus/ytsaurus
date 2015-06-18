#include <yt_udf.h>

#include <ctype.h>

void lower(
    TExecutionContext* context,
    char** result,
    int* result_len,
    char* s,
    int s_len)
{
    *result = AllocateBytes(context, s_len);
    for (int i = 0; i < s_len; i++) {
        (*result)[i] = tolower(s[i]);
    }
    *result_len = s_len;
}
