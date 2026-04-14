#include <yt/yt/library/query/misc/udf_cpp_abi.h>

#include <library/cpp/yt/farmhash/farm_hash.h>

#include <util/system/types.h>

#include <algorithm>
#include <cstring>
#include <cmath>
#include <new>

using namespace NYT::NQueryClient::NUdf;

extern "C" uint64_t UniqGetFingerprint(TUnversionedValue* valueBegin, int valueCount);

namespace {

struct TUniqState
{
    static constexpr uint32_t SentinelHash = 0;
    static constexpr i8 DefaultBufferSizePower = 4;
    static constexpr i8 MaxBufferSizePower = 17;
    static constexpr uint32_t MaxSize = 1 << (MaxBufferSizePower - 1);

    uint32_t Size = 0;
    i8 SamplingDegree = 0;
    i8 BufferSizePower = DefaultBufferSizePower;
    bool HasSentinel = false;
    uint32_t Buffer[];

    uint32_t BufferSize() const
    {
        return 1u << BufferSizePower;
    }

    uint32_t Mask() const
    {
        return (static_cast<uint32_t>(1) << BufferSizePower) - 1;
    }

    uint32_t Place(uint32_t hash) const
    {
        return (hash >> (32 - TUniqState::MaxBufferSizePower)) & Mask();
    }

    bool Good(uint32_t hash) const {
        return hash == ((hash >> SamplingDegree) << SamplingDegree);
    }

    void Rehash()
    {
        for (uint32_t index = 0; index < BufferSize(); ++index) {
            if (Buffer[index] != SentinelHash) {
                if (!Good(Buffer[index])) {
                    Buffer[index] = SentinelHash;
                    --Size;
                } else if (index != Place(Buffer[index])) {
                    auto x = Buffer[index];
                    Buffer[index] = SentinelHash;
                    ReinsertImpl(x);
                }
            }
        }

        for (uint32_t index = 0; index < BufferSize() && Buffer[index] != SentinelHash; ++index) {
            if (index != Place(Buffer[index])) {
                auto x = Buffer[index];
                Buffer[index] = SentinelHash;
                ReinsertImpl(x);
            }
        }
    }

    std::pair<TUnversionedValue, TUniqState*> Resize(TExpressionContext* context) const
    {
        TUnversionedValue newState;
        auto* newUniqState = AllocateState(context, &newState, BufferSizePower + 1);
        newUniqState->HasSentinel = HasSentinel;
        newUniqState->Size = Size;
        newUniqState->SamplingDegree = SamplingDegree;
        memcpy(newUniqState->Buffer, Buffer, sizeof(uint32_t) * BufferSize());

        newUniqState->ResizeImpl();

        return {newState, newUniqState};
    }

    void ResizeImpl()
    {
        auto oldBufferSize = (1u << (BufferSizePower - 1));
        for (uint32_t index = 0; index < oldBufferSize || Buffer[index] != SentinelHash; ++index) {
            auto hash = Buffer[index];
            if (hash == SentinelHash) {
                continue;
            }

            auto pos = Place(hash);
            if (index == pos) {
                continue;
            }

            while (Buffer[pos] != SentinelHash && Buffer[pos] != hash) {
                ++pos;
                pos &= Mask();
            }

            if (Buffer[pos] == hash) {
                continue;
            }

            Buffer[pos] = hash;
            Buffer[index] = SentinelHash;
        }
    }

    void ReinsertImpl(uint32_t hash)
    {
        auto pos = Place(hash);

        while (Buffer[pos] != SentinelHash) {
            ++pos;
            pos &= Mask();
        }

        Buffer[pos] = hash;
    }

    void InsertImpl(uint32_t hash)
    {
        if (hash == SentinelHash) {
            if (!HasSentinel) {
                HasSentinel = true;
                Size++;
            }
            return;
        }

        auto pos = Place(hash);
        while (Buffer[pos] != SentinelHash && Buffer[pos] != hash) {
            ++pos;
            pos &= Mask();
        }

        if (Buffer[pos] == hash) {
            return;
        }

        Buffer[pos] = hash;
        Size++;
    }

    static TUniqState* AllocateState(
        TExpressionContext* context,
        TUnversionedValue* result,
        i8 sizePower)
    {
        result->Type = EValueType::String;
        result->Length = sizeof(TUniqState) + sizeof(uint32_t) * (1 << sizePower);
        result->Data.String = AllocateBytes(context, result->Length);
        auto* uniqState = new (result->Data.String) TUniqState();
        uniqState->BufferSizePower = sizePower;
        std::fill(uniqState->Buffer, uniqState->Buffer + uniqState->BufferSize(), TUniqState::SentinelHash);
        return uniqState;
    }
};

static_assert(std::is_trivially_destructible_v<TUniqState>);

inline TUniqState* Adapt(
    TExpressionContext* context,
    TUnversionedValue* state)
{
    auto* uniqState = reinterpret_cast<TUniqState*>(state->Data.String);

    if (uniqState->Size > uniqState->BufferSize() / 2) {
        if (uniqState->Size > TUniqState::MaxSize) {
            while (uniqState->Size > TUniqState::MaxSize) {
                uniqState->SamplingDegree++;

                uniqState->Rehash();
            }
        } else {
            std::tie(*state, uniqState) = uniqState->Resize(context);
        }
    }

    return uniqState;
}

} // namespace

// uniq

extern "C" void uniq_init(
    TExpressionContext* context,
    TUnversionedValue* result)
{
    TUniqState::AllocateState(context, result, TUniqState::DefaultBufferSizePower);
}

extern "C" void uniq_update(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValues,
    int valueCount)
{
    auto* uniqState = reinterpret_cast<TUniqState*>(state->Data.String);

    auto hash = static_cast<uint32_t>(UniqGetFingerprint(newValues, valueCount));

    if (!uniqState->Good(hash)) {
        *result = *state;
        return;
    }

    uniqState->InsertImpl(hash);

    Adapt(context, state);

    *result = *state;
}

extern "C" void uniq_merge(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* leftState,
    TUnversionedValue* rightState)
{
    auto* leftUniqState = reinterpret_cast<TUniqState*>(leftState->Data.String);
    auto* rightUniqState = reinterpret_cast<TUniqState*>(rightState->Data.String);

    if (rightUniqState->SamplingDegree > leftUniqState->SamplingDegree) {
        leftUniqState->SamplingDegree = rightUniqState->SamplingDegree;
        leftUniqState->Rehash();
    }

    if (rightUniqState->HasSentinel && !leftUniqState->HasSentinel) {
        leftUniqState->HasSentinel = true;
        leftUniqState->Size++;

        leftUniqState = Adapt(context, leftState);
    }

    auto rightBufferSize = rightUniqState->BufferSize();
    for (uint32_t index = 0; index < rightBufferSize; ++index) {
        auto hash = rightUniqState->Buffer[index];
        if (hash != TUniqState::SentinelHash && leftUniqState->Good(hash)) {
            leftUniqState->InsertImpl(hash);
            leftUniqState = Adapt(context, leftState);
        }
    }

    *result = *leftState;
}

extern "C" void uniq_finalize(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    result->Type = EValueType::Uint64;
    auto* uniqState = reinterpret_cast<TUniqState*>(state->Data.String);

    if (uniqState->SamplingDegree == 0) {
        result->Data.Uint64 = uniqState->Size;
        return;
    }

    uint64_t extrapolated = static_cast<uint64_t>(uniqState->Size) << uniqState->SamplingDegree;

    uint64_t fuzzed = extrapolated + (NYT::FarmFingerprint(uniqState->Size) & ((1ULL << uniqState->SamplingDegree) - 1));

    // 32-bit hashes help save space, but introduce collisions which would not be present otherwise.
    auto p32 = static_cast<uint64_t>(1) << 32;
    auto adjusted = static_cast<uint64_t>(round(p32 * (std::log(p32) - std::log(p32 - fuzzed))));

    result->Data.Uint64 = adjusted;
}

// uniq_state

extern "C" void uniq_state_init(
    TExpressionContext* context,
    TUnversionedValue* result)
{
    uniq_init(context, result);
}

extern "C" void uniq_state_update(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* newValues,
    int valueCount)
{
    uniq_update(context, result, state, newValues, valueCount);
}

extern "C" void uniq_state_merge(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* leftState,
    TUnversionedValue* rightState)
{
    uniq_merge(context, result, leftState, rightState);
}

extern "C" void uniq_state_finalize(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    *result = *state;
}

// uniq_merge

extern "C" void uniq_merge_init(
    TExpressionContext* context,
    TUnversionedValue* result)
{
    uniq_init(context, result);
}

extern "C" void uniq_merge_update(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* leftState,
    TUnversionedValue* rightState)
{
    uniq_merge(context, result, leftState, rightState);
}

extern "C" void uniq_merge_merge(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* leftState,
    TUnversionedValue* rightState)
{
    uniq_merge(context, result, leftState, rightState);
}

extern "C" void uniq_merge_finalize(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    uniq_finalize(context, result, state);
}

// uniq_merge_state

extern "C" void uniq_merge_state_init(
    TExpressionContext* context,
    TUnversionedValue* result)
{
    uniq_init(context, result);
}

extern "C" void uniq_merge_state_update(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* leftState,
    TUnversionedValue* rightState)
{
    uniq_merge(context, result, leftState, rightState);
}

extern "C" void uniq_merge_state_merge(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* leftState,
    TUnversionedValue* rightState)
{
    uniq_merge(context, result, leftState, rightState);
}

extern "C" void uniq_merge_state_finalize(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    *result = *state;
}
