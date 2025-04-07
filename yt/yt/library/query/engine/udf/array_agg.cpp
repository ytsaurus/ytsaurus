#include <yt/yt/library/query/misc/udf_cpp_abi.h>

#include <string.h>

using namespace NYT::NQueryClient::NUdf;

extern "C" void ArrayAggFinalize(TExpressionContext* context, TUnversionedValue* result, TUnversionedValue* state);

int64_t& GetUsedSpace(TUnversionedValue state)
{
    return *reinterpret_cast<int64_t*>(state.Data.String);
}

int64_t RoundUpPowerOfTwo(int64_t value)
{
    int64_t power = 1;
    while (power < value) {
        power *= 2;
    }
    return power;
}

bool IsStringlike(EValueType type)
{
    return type == EValueType::String || type == EValueType::Any;
}

int64_t AppendValue(TUnversionedValue value, char* destination)
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
        uint64_t dataOrLength;
        if (IsStringlike(value.Type)) {
            dataOrLength = value.Length;
        } else {
            dataOrLength = value.Data.Uint64;
        }
        memcpy(destination, &dataOrLength, sizeof(uint64_t));
    }
    destination += sizeof(uint64_t);

    if (!IsStringlike(value.Type)) {
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

    int64_t neededSpace;
    int64_t usedSpace;
    if (state->Length == 0) {
        usedSpace = 0;
        neededSpace = sizeof(int64_t) + AppendValue(*newValue, nullptr);
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
            usedSpace = sizeof(int64_t);
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

    auto rightLengthNoHeader = GetUsedSpace(*rightState) - sizeof(int64_t);
    auto newLength = RoundUpPowerOfTwo(GetUsedSpace(*leftState) + rightLengthNoHeader);

    if (newLength != leftState->Length) {
        result->Length = newLength;
        result->Data.String = AllocateBytes(context, newLength);
        memcpy(result->Data.String, leftState->Data.String, GetUsedSpace(*leftState));
    }

    memcpy(
        result->Data.String + GetUsedSpace(*result),
        rightState->Data.String + sizeof(int64_t),
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
