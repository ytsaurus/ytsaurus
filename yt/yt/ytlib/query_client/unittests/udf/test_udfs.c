#include <yt/ytlib/query_client/udf/yt_udf.h>

uint64_t seventyfive(TExpressionContext* context)
{
    (void)context;
    return 75;
}

uint64_t strtol_udf(TExpressionContext* context, const char* string, int length)
{
    (void)context;
    uint64_t result = 0;
    for (int i = 0; i < length; i++) {
        result *= 10;
        int digit = string[i] - 48;
        result += digit;
    }
    return result;
}

int64_t exp_udf(TExpressionContext* context, int64_t n, int64_t m)
{
    (void)context;
    int64_t result = 1;
    for (int64_t i = 0; i < m; i++) {
        result *= n;
    }
    return result;
}

void tolower_udf(
    TExpressionContext* context,
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

void is_null_udf(TExpressionContext* context, TUnversionedValue* result, TUnversionedValue* value)
{
    (void)context;
    int8_t isnull = value->Type == Null;
    result->Type = Boolean;
    result->Data.Boolean = isnull;
}

int64_t abs_udf(TExpressionContext* context, int64_t n)
{
    (void)context;
    return llabs(n);
}

void sum_udf(
    TExpressionContext* context,
    TUnversionedValue* result_value,
    TUnversionedValue* n1,
    TUnversionedValue* ns,
    int ns_len)
{
    (void)context;
    int64_t result = n1->Data.Int64;
    for (int i = 0; i < ns_len; i++) {
        result += ns[i].Data.Int64;
    }
    result_value->Type = Int64;
    result_value->Data.Int64 = result;
}

int64_t throw_if_negative_udf(
    TExpressionContext* context,
    int64_t argument)
{
    (void)context;
    if (argument < 0) {
        ThrowException("Argument was negative");
    }

    return argument;
}

void avg_udaf_init(
    TExpressionContext* context,
    TUnversionedValue* result)
{
    int stateSize = 2 * sizeof(int64_t);
    char* statePtr = AllocateBytes(context, stateSize);
    int64_t* intStatePtr = (int64_t*)statePtr;
    intStatePtr[0] = 0;
    intStatePtr[1] = 0;

    result->Length = stateSize;
    result->Type = String;
    result->Data.String = statePtr;
}

void avg_udaf_update(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValue)
{
    (void)context;
    int64_t* intStatePtr = (int64_t*)state->Data.String;
    if (newValue->Type != Null) {
        intStatePtr[0] += 1;
        intStatePtr[1] += newValue->Data.Int64;
    }

    result->Length = 2 * sizeof(int64_t);
    result->Type = String;
    result->Data.String = (char*)intStatePtr;
}

void avg_udaf_merge(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* dstState,
    TUnversionedValue* state)
{
    (void)context;
    int64_t* dstStatePtr = (int64_t*)dstState->Data.String;
    int64_t* intStatePtr = (int64_t*)state->Data.String;

    dstStatePtr[0] += intStatePtr[0];
    dstStatePtr[1] += intStatePtr[1];

    result->Length = 2 * sizeof(int64_t);
    result->Type = String;
    result->Data.String = (char*)dstStatePtr;
}

void avg_udaf_finalize(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    (void)context;
    int64_t* intStatePtr = (int64_t*)state->Data.String;
    if (intStatePtr[0] == 0) {
        result->Type = Null;
    } else {
        double resultData = (double)intStatePtr[1] / (double)intStatePtr[0];
        result->Type = Double;
        result->Data.Double = resultData;
    }
}
