#include <yt_udf.h>

char IsSubstr(
    const char* patternData,
    uint32_t patternLength,
    const char* stringData,
    uint32_t stringLength);


int8_t is_substr(
    TExecutionContext* context,
    char* s1,
    int s1_len,
    char* s2,
    int s2_len)
{
    return IsSubstr(s1, s1_len, s2, s2_len);
}
