#include <yt/yt/library/query/misc/udf_cpp_abi.h>
#include <yt/yt/library/query/misc/function_context.h>

#include "uniq_state.h"

using namespace NYT::NQueryClient::NUdf;

extern "C" ui64 UniqGetFingerprint(TUnversionedValue* valueBegin, int valueCount);

namespace {

struct TRunningSumState
{
    EValueType Type = EValueType::Null;
    TUnversionedValueData Data{};
};

struct TRunningUniqState
{
    char* Storage = nullptr;
    size_t StorageSize = 0;
    TUniqState* UniqState = nullptr;

    ~TRunningUniqState()
    {
        delete[] Storage;
    }

    size_t GetStateSize(i8 bufferSizePower) const
    {
        return sizeof(TUniqState) + sizeof(ui32) * (static_cast<size_t>(1) << bufferSizePower);
    }

    void AllocateOwnedState(i8 bufferSizePower)
    {
        StorageSize = GetStateSize(bufferSizePower);
        Storage = new char[StorageSize];
        UniqState = new (Storage) TUniqState();
        UniqState->BufferSizePower = bufferSizePower;
        std::fill(
            UniqState->Buffer,
            UniqState->Buffer + UniqState->GetBufferSize(),
            TUniqState::SentinelHash);
    }

    void OwnState(TExpressionContext* context)
    {
        TUnversionedValue stateValue;
        stateValue.Type = EValueType::String;
        stateValue.Length = static_cast<i64>(GetStateSize(UniqState->BufferSizePower));
        stateValue.Data.String = reinterpret_cast<char*>(UniqState);

        auto* adapted = TUniqState::Adapt(context, &stateValue);

        if (adapted == UniqState) {
            return;
        }

        auto newSize = GetStateSize(adapted->BufferSizePower);
        auto* newStorage = new char[newSize];
        memcpy(newStorage, adapted, newSize);

        delete[] Storage;
        Storage = newStorage;
        StorageSize = newSize;
        UniqState = reinterpret_cast<TUniqState*>(newStorage);
    }
};

struct TRunningDifferenceState
{
    bool HasPrevious = false;
    TUnversionedValueData Previous{};
};

} // namespace

extern "C" void running_sum(
    TExpressionContext* /*context*/,
    NYT::NQueryClient::TFunctionContext* functionContext,
    TUnversionedValue* result,
    TUnversionedValue* newValue)
{
    auto* state = static_cast<TRunningSumState*>(functionContext->GetPrivateData());
    if (!state) {
        state = functionContext->CreateObject<TRunningSumState>();
        if (!state) {
            ThrowException("Failed to allocate running_sum state");
        }
        functionContext->SetPrivateData(state);
    }

    if (newValue->Type == EValueType::Null) {
        result->Type = EValueType::Null;
        return;
    }

    if (state->Type == EValueType::Null) {
        state->Type = newValue->Type;
        state->Data = newValue->Data;
    } else if (newValue->Type == EValueType::Int64) {
        state->Data.Int64 += newValue->Data.Int64;
    } else if (newValue->Type == EValueType::Uint64) {
        state->Data.Uint64 += newValue->Data.Uint64;
    } else if (newValue->Type == EValueType::Double) {
        state->Data.Double += newValue->Data.Double;
    }

    result->Type = state->Type;
    result->Data = state->Data;
}

extern "C" void running_uniq(
    TExpressionContext* context,
    NYT::NQueryClient::TFunctionContext* functionContext,
    TUnversionedValue* result,
    TUnversionedValue* newValue)
{
    auto* state = static_cast<TRunningUniqState*>(functionContext->GetPrivateData());
    if (!state) {
        state = functionContext->CreateObject<TRunningUniqState>();
        if (!state) {
            ThrowException("Failed to allocate running_uniq state");
        }
        functionContext->SetPrivateData(state);
    }

    if (!state->UniqState) {
        state->AllocateOwnedState(TUniqState::DefaultBufferSizePower);
    }

    auto hash = static_cast<ui32>(UniqGetFingerprint(newValue, 1));

    if (state->UniqState->IsGood(hash)) {
        state->UniqState->InsertImpl(hash);
        state->OwnState(context);
    }

    result->Type = EValueType::Uint64;
    result->Data.Uint64 = state->UniqState->Estimate();
}

extern "C" void running_difference(
    TExpressionContext* /*context*/,
    NYT::NQueryClient::TFunctionContext* functionContext,
    TUnversionedValue* result,
    TUnversionedValue* newValue)
{
    auto* state = static_cast<TRunningDifferenceState*>(functionContext->GetPrivateData());
    if (!state) {
        state = functionContext->CreateObject<TRunningDifferenceState>();
        if (!state) {
            ThrowException("Failed to allocate running_difference state");
        }
        functionContext->SetPrivateData(state);
    }

    if (newValue->Type == EValueType::Null) {
        result->Type = EValueType::Null;
        return;
    }

    if (!state->HasPrevious) {
        state->HasPrevious = true;
        state->Previous = newValue->Data;
    }

    result->Type = newValue->Type;
    switch (newValue->Type) {
        case EValueType::Int64:
            result->Data.Int64 = newValue->Data.Int64 - state->Previous.Int64;
            break;
        case EValueType::Uint64:
            result->Data.Uint64 = newValue->Data.Uint64 - state->Previous.Uint64;
            break;
        case EValueType::Double:
            result->Data.Double = newValue->Data.Double - state->Previous.Double;
            break;
        default:
            result->Type = EValueType::Null;
            break;
    }

    state->Previous = newValue->Data;
}
