#include <yt_udf.h>
#include <string.h>

int64_t strtoint(
    TExecutionContext* context,
    char* string,
    int string_len)
{
    char* null_term_string = AllocateBytes(context, string_len + 1);
    memcpy(null_term_string, string, string_len);
    null_term_string[string_len] = 0;
    return atoll(null_term_string);
}
