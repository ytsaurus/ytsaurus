#include <yt_udf.h>

void get_suffix(
    TExecutionContext* context,
    char** result,
    int* result_len,
    char* string,
    int string_len,
    uint64_t suffix_len)
{
    *result = string + (string_len - suffix_len);
    *result_len = suffix_len;
}
