#include <yt_udf.h>

#include <ctype.h>
#include <time.h>

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

int64_t sleep(
    TExecutionContext* context,
    int64_t value)
{
    if (value < 1) {
        value = 1;
    }
    if (value > 1000) {
        value = 1000;
    }
    struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = value * 1000000;
    nanosleep(&ts, NULL);
    return 0;
}

void uint64(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* value)
{
    result->Type = Uint64;
    if (value->Type == Int64) {
        result->Data.Uint64 = (uint64_t)value->Data.Uint64;
    } else if (value->Type == Uint64) {
        result->Data.Uint64 = (uint64_t)value->Data.Uint64;
    } else if (value->Type == Double) {
        result->Data.Uint64 = (uint64_t)value->Data.Double;
    }
}

void int64(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* value)
{
    result->Type = Int64;
    if (value->Type == Int64) {
        result->Data.Int64 = value->Data.Int64;
    } else if (value->Type == Uint64) {
        result->Data.Int64 = (int64_t)value->Data.Uint64;
    } else if (value->Type == Double) {
        result->Data.Int64 = (int64_t)value->Data.Double;
    }
}

void double_cast(
    TExecutionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* value)
{
    result->Type = Double;
    if (value->Type == Int64) {
        result->Data.Double = (double)value->Data.Int64;
    } else if (value->Type == Uint64) {
        result->Data.Double = (double)value->Data.Uint64;
    } else if (value->Type == Double) {
        result->Data.Double = value->Data.Double;
    }
}
