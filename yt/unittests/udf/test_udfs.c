#include <yt_udf.h>

unsigned long strtol_udf(TExecutionContext* context, const char* string, int length)
{
    unsigned long result = 0;
    for (int i = 0; i < length; i++) {
        result *= 10;
        int digit = string[i] - 48;
        result += digit;
    }
    return result;
}

long exp_udf(TExecutionContext* context, long n, long m)
{
    long result = 1;
    for (long i = 0; i < m; i++) {
        result *= n;
    }
    return result;
}

void tolower_udf(
    TExecutionContext* context,
    char** result,
    int* result_length,
    char* string,
    int length)
{
    char* lower_string = AllocateBytes(
        context,
        length * sizeof(char));
    for (int i = 0; i < length; i++) {
        if (65 <= string[i] && string[i] <= 90) {
            lower_string[i] = string[i] + 32;
        } else {
            lower_string[i] = string[i];
        }
    }
    *result = lower_string;
    *result_length = length;
}

void is_null_udf(TExecutionContext* context, TUnversionedValue* result, TUnversionedValue* value)
{
    bool isnull = value->Type == Null;
    result->Type = Boolean;
    result->Data.Boolean = isnull;
}

long abs_udf(TExecutionContext* context, long n)
{
    return labs(n);
}

void sum_udf(
    TExecutionContext* context,
    TUnversionedValue* result_value,
    TUnversionedValue* n1,
    TUnversionedValue* ns,
    int ns_len)
{
    int64_t result = n1->Data.Int64;
    for (int i = 0; i < ns_len; i++) {
        result += n1[i].Data.Int64;
    }
    result_value->Type = Int64;
    result_value->Data.Int64 = result;
}
