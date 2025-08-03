#include <yt/yt/library/query/misc/udf_cpp_abi.h>

#include <util/system/types.h>

#include <string.h>

using namespace NYT::NQueryClient::NUdf;

extern "C" void ArrayAggFinalize(TExpressionContext* context, TUnversionedValue* result, TUnversionedValue* state);

i64& GetUsedSpace(TUnversionedValue state)
{
    return *reinterpret_cast<i64*>(state.Data.String);
}

ui64 RoundUpPowerOfTwo(ui64 value)
{
    return value == 0 ? 0 : ui64(1) << (64 - __builtin_clzll(value - 1));
}

bool IsStringLike(EValueType type)
{
    return type == EValueType::String || type == EValueType::Any;
}

i64 AppendValue(TUnversionedValue value, char* destination)
{
    auto dryRun = destination == nullptr;
    auto* start = destination;

    if (!dryRun) {
        memcpy(destination, &value.Type, sizeof(EValueType));
    }
    destination += sizeof(EValueType);

    if (value.Type == EValueType::Null) {
        return destination - start;
    }

    if (!dryRun) {
        ui64 dataOrLength;
        if (IsStringLike(value.Type)) {
            dataOrLength = value.Length;
        } else {
            dataOrLength = value.Data.Uint64;
        }
        memcpy(destination, &dataOrLength, sizeof(ui64));
    }
    destination += sizeof(ui64);

    if (!IsStringLike(value.Type)) {
        return destination - start;
    }

    if (!dryRun) {
        memcpy(destination, value.Data.String, value.Length);
    }
    destination += value.Length;

    return destination - start;
}

extern "C" void array_agg_init(
    TExpressionContext* /*context*/,
    TUnversionedValue* result)
{
    result->Type = EValueType::String;
    result->Data.String = nullptr;
    result->Length = 0;
}

extern "C" void array_agg_update(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValue,
    TUnversionedValue* ignoreNulls)
{
    *result = *state;

    auto shouldIgnoreNulls = (ignoreNulls->Type == EValueType::Boolean) && ignoreNulls->Data.Boolean;
    if (shouldIgnoreNulls && newValue->Type == EValueType::Null) {
        return;
    }

    i64 neededSpace;
    i64 usedSpace;
    if (state->Length == 0) {
        usedSpace = 0;
        neededSpace = sizeof(i64) + AppendValue(*newValue, nullptr);
    } else {
        usedSpace = GetUsedSpace(*state);
        neededSpace = usedSpace + AppendValue(*newValue, nullptr);
    }

    if (state->Length < neededSpace) {
        auto newLength = RoundUpPowerOfTwo(neededSpace);
        result->Data.String = AllocateBytes(context, newLength);
        result->Length = newLength;
        if (usedSpace != 0) {
            memcpy(result->Data.String, state->Data.String, usedSpace);
        } else {
            usedSpace = sizeof(i64);
            GetUsedSpace(*result) = usedSpace;
        }
    }

    auto* freeZone = result->Data.String + usedSpace;
    auto written = AppendValue(*newValue, freeZone);
    GetUsedSpace(*result) += written;
}

extern "C" void array_agg_merge(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* leftState,
    TUnversionedValue* rightState)
{
    if (rightState->Length == 0) {
        *result = *leftState;
        return;
    }

    if (leftState->Length == 0) {
        *result = *rightState;
        if (rightState->Type == EValueType::String && rightState->Length != 0) {
            result->Data.String = AllocateBytes(context, rightState->Length);
            memcpy(result->Data.String, rightState->Data.String, GetUsedSpace(*rightState));
        }
        return;
    }

    *result = *leftState;

    auto rightLengthNoHeader = GetUsedSpace(*rightState) - sizeof(i64);
    auto newLength = RoundUpPowerOfTwo(GetUsedSpace(*leftState) + rightLengthNoHeader);

    if (newLength != leftState->Length) {
        result->Length = newLength;
        result->Data.String = AllocateBytes(context, newLength);
        memcpy(result->Data.String, leftState->Data.String, GetUsedSpace(*leftState));
    }

    memcpy(
        result->Data.String + GetUsedSpace(*result),
        rightState->Data.String + sizeof(i64),
        rightLengthNoHeader);
    GetUsedSpace(*result) += rightLengthNoHeader;
}

extern "C" void array_agg_finalize(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    ArrayAggFinalize(context, result, state);
}
