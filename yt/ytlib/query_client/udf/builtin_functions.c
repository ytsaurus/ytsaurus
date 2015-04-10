#include <yt_udf.h>
#include <ctype.h>

char IsSubstr(
    const char* patternData,
    uint32_t patternLength,
    const char* stringData,
    uint32_t stringLength);

char* ToLower(
    TExecutionContext* executionContext,
    const char* data,
    uint32_t length);

////////////////////////////////////////////////////////////////////////////////

int8_t is_substr(
    TExecutionContext* context,
    char* s1,
    int s1_len,
    char* s2,
    int s2_len)
{
    return IsSubstr(s1, s1_len, s2, s2_len);
}

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
