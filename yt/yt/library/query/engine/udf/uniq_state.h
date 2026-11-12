#pragma once

#include <yt/yt/library/query/misc/udf_cpp_abi.h>

#include <library/cpp/yt/farmhash/farm_hash.h>

#include <util/system/types.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <new>
#include <type_traits>

using namespace NYT::NQueryClient::NUdf;

extern "C" ui64 UniqGetFingerprint(TUnversionedValue* valueBegin, int valueCount);

class TUniqState
{
public:
    static constexpr ui32 SentinelHash = 0;
    static constexpr i8 DefaultBufferSizePower = 4;
    static constexpr i8 MaxBufferSizePower = 17;
    static constexpr ui32 MaxSize = 1 << (MaxBufferSizePower - 1);

    ui32 Size = 0;
    i8 SamplingDegree = 0;
    i8 BufferSizePower = DefaultBufferSizePower;
    bool HasSentinel = false;
    ui32 Buffer[];

    inline ui32 GetBufferSize() const
    {
        return 1u << BufferSizePower;
    }

    inline ui32 GetMask() const
    {
        return (static_cast<ui32>(1) << BufferSizePower) - 1;
    }

    inline ui32 Place(ui32 hash) const
    {
        return (hash >> (32 - TUniqState::MaxBufferSizePower)) & GetMask();
    }

    inline bool IsGood(ui32 hash) const
    {
        return hash == ((hash >> SamplingDegree) << SamplingDegree);
    }

    void Rehash()
    {
        for (ui32 index = 0; index < GetBufferSize(); ++index) {
            if (Buffer[index] != SentinelHash) {
                if (!IsGood(Buffer[index])) {
                    Buffer[index] = SentinelHash;
                    --Size;
                } else if (index != Place(Buffer[index])) {
                    auto x = Buffer[index];
                    Buffer[index] = SentinelHash;
                    ReinsertImpl(x);
                }
            }
        }

        for (ui32 index = 0; index < GetBufferSize() && Buffer[index] != SentinelHash; ++index) {
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
        memcpy(newUniqState->Buffer, Buffer, sizeof(ui32) * GetBufferSize());

        newUniqState->ResizeImpl();

        return {newState, newUniqState};
    }

    void InsertImpl(ui32 hash)
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
            pos &= GetMask();
        }

        if (Buffer[pos] == hash) {
            return;
        }

        Buffer[pos] = hash;
        Size++;
    }

    ui64 Estimate() const
    {
        if (SamplingDegree == 0) {
            return Size;
        }

        ui64 extrapolated = static_cast<ui64>(Size) << SamplingDegree;

        ui64 fuzzed = extrapolated + (NYT::FarmFingerprint(Size) & ((1ULL << SamplingDegree) - 1));

        // 32-bit hashes help save space, but introduce collisions which would not be present otherwise.
        auto p32 = static_cast<ui64>(1) << 32;
        return static_cast<ui64>(round(p32 * (std::log(p32) - std::log(p32 - fuzzed))));
    }

    static TUniqState* AllocateState(
        TExpressionContext* context,
        TUnversionedValue* result,
        i8 sizePower)
    {
        result->Type = EValueType::String;
        result->Length = sizeof(TUniqState) + sizeof(ui32) * (1 << sizePower);
        result->Data.String = AllocateBytes(context, result->Length);
        auto* uniqState = new (result->Data.String) TUniqState();
        uniqState->BufferSizePower = sizePower;
        std::fill(uniqState->Buffer, uniqState->Buffer + uniqState->GetBufferSize(), TUniqState::SentinelHash);
        return uniqState;
    }

    static TUniqState* Adapt(
        TExpressionContext* context,
        TUnversionedValue* state)
    {
        auto* uniqState = reinterpret_cast<TUniqState*>(state->Data.String);

        if (uniqState->Size > uniqState->GetBufferSize() / 2) {
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

private:
    void ResizeImpl()
    {
        auto oldBufferSize = (1u << (BufferSizePower - 1));
        for (ui32 index = 0; index < oldBufferSize || Buffer[index] != SentinelHash; ++index) {
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
                pos &= GetMask();
            }

            if (Buffer[pos] == hash) {
                continue;
            }

            Buffer[pos] = hash;
            Buffer[index] = SentinelHash;
        }
    }

    void ReinsertImpl(ui32 hash)
    {
        auto pos = Place(hash);

        while (Buffer[pos] != SentinelHash) {
            ++pos;
            pos &= GetMask();
        }

        Buffer[pos] = hash;
    }
};

static_assert(std::is_trivially_destructible_v<TUniqState>);
