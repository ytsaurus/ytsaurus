#include <yt_udf.h>

int64_t strtoint(
    TExecutionContext* context,
    char* string,
    int string_len)
{
    return atol(string);
}
