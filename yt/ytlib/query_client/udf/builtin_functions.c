#include <yt_udf.h>
#include <ctype.h>

char IsSubstr(
    const char* patternData,
    uint32_t patternLength,
    const char* stringData,
    uint32_t stringLength);


typedef struct TUnversionedRowHeader
{
    uint32_t Count;
    uint32_t Padding;
} TUnversionedRowHeader;

typedef TUnversionedRowHeader* TRow;

uint64_t SimpleHash(TRow row);

void AllocateRow1(TExecutionContext* context, int valueCount, TRow* row);

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
    TUnversionedValue* arg1,
    TUnversionedValue* args,
    int args_len)
{
    TRow row;
    AllocateRow1(context, args_len + 1, &row);
    TUnversionedValue* row_value = (TUnversionedValue*)(row + 1);

    *row_value = *arg1;
    row_value++;
    for (int i = 0; i < args_len; i++) {
        row_value[i] = args[i];
    }

    uint64_t hash = SimpleHash(row);

    result->Type = Uint64;
    result->Data.Uint64 = hash;
}
