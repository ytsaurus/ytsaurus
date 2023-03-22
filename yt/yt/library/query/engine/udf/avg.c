#include <yt/yt/library/query/misc/udf_c_abi.h>

void avg_init(
    TExpressionContext* context,
    TUnversionedValue* result)
{
    int stateSize = 2 * sizeof(int64_t);
    char* statePtr = AllocateBytes(context, stateSize);
    int64_t* intStatePtr = (int64_t*)statePtr;
    intStatePtr[0] = 0;
    intStatePtr[1] = 0;

    result->Type = VT_String;
    result->Length = stateSize;
    result->Data.String = statePtr;
}

void avg_update(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValue)
{
    (void)context;

    int64_t* intStatePtr = (int64_t*)state->Data.String;
    if (newValue->Type != VT_Null) {
        intStatePtr[0] += 1;
        intStatePtr[1] += newValue->Data.Int64;
    }

    result->Type = VT_String;
    result->Length = 2 * sizeof(int64_t);
    result->Data.String = (char*)intStatePtr;
}

void avg_merge(
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

    result->Type = VT_String;
    result->Length = 2 * sizeof(int64_t);
    result->Data.String = (char*)dstStatePtr;
}

void avg_finalize(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    (void)context;

    int64_t* intStatePtr = (int64_t*)state->Data.String;
    if (intStatePtr[0] == 0) {
        result->Type = VT_Null;
    } else {
        double resultData = (double)intStatePtr[1] / (double)intStatePtr[0];
        result->Type = VT_Double;
        result->Data.Double = resultData;
    }
}
