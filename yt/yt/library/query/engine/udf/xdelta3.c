#include <yt/yt/library/query/misc/udf_c_abi.h>

int XdeltaMerge(
    void* context,
    const uint8_t* lhsData,
    size_t lhsSize,
    const uint8_t* rhsData,
    size_t rhsSize,
    const uint8_t** resultData,
    size_t* resultOffset,
    size_t* resultSize);

void xdelta_init(
    TExpressionContext* context,
    TUnversionedValue* result)
{
    (void)context;

    result->Type = VT_Null;
}

void xdelta_update(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValue)
{
    (void)context;
    (void)result;
    (void)state;
    (void)newValue;
    // not applicable
}

void xdelta_merge(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* dstState,
    TUnversionedValue* state)
{
    if (state->Type == VT_Null) {
        result->Type = dstState->Type;
        result->Data = dstState->Data;
        result->Length = dstState->Length;
        result->Flags = dstState->Flags;
        return;
    }

    if (dstState->Type == VT_String) {
        const uint8_t* result_data;
        size_t result_offset;
        size_t result_size;
        int ret = XdeltaMerge(
            context,
            (uint8_t*)dstState->Data.String,
            dstState->Length,
            (uint8_t*)state->Data.String,
            state->Length,
            &result_data,
            &result_offset,
            &result_size);
        if (ret != 0) {
            result->Type = VT_String;
            result->Flags = VF_Aggregate;
            result->Length = result_size;
            result->Data.String = (char*)result_data + result_offset;
        } else {
            // Error
            result->Type = VT_Null;
        }

    } else {
        result->Type = VT_String;
        result->Flags = VF_Aggregate;
        result->Data.String = state->Data.String;
        result->Length = state->Length;
    }
}

void xdelta_finalize(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    (void)context;

    result->Type = state->Type;
    result->Data = state->Data;
    result->Length = state->Length;
}
