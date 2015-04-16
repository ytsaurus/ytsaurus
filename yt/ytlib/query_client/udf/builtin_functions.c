#include <yt_udf.h>
#include <ctype.h>

char IsSubstr(
    const char* patternData,
    uint32_t patternLength,
    const char* stringData,
    uint32_t stringLength);

uint64_t SimpleHash(
    const TUnversionedValue* begin,
    const TUnversionedValue* end);

uint64_t FarmHash(
    const TUnversionedValue* begin,
    const TUnversionedValue* end);

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

void is_null(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* value)
{
    result->Type = Boolean;
    int8_t isnull = value->Type == Null;
    result->Data.Boolean = isnull;
}

void simple_hash(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* args,
    int args_len)
{
    result->Data.Uint64 = SimpleHash(args, args + args_len);;
    result->Type = Uint64;
}

void farm_hash(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* args,
    int args_len)
{
    result->Data.Uint64 = FarmHash(args, args + args_len);
    result->Type = Uint64;
}
