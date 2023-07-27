#pragma once

#include "coder.h"
#include "key_value.h"

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

template <typename V>
class TShardedValue: private TKV<uint64_t, V>
{
private:
    using TBase = TKV<uint64_t, V>;

public:
    using TValue = typename TBase::TValue;

public:
    TShardedValue() = default;

    TShardedValue(uint64_t shard, const TValue& value)
        : TBase(shard, value)
    { }

    uint64_t Shard() const noexcept
    {
        return TBase::Key();
    }

    uint64_t& Shard() noexcept
    {
        return TBase::Key();
    }

    const TValue& Value() const noexcept
    {
        return TBase::Value();
    }

    TValue& Value() noexcept
    {
        return TBase::Value();
    }
};

template <typename T>
class TCoder<TShardedValue<T>>
{
public:
    inline void Encode(IOutputStream* out, const TShardedValue<T>& shardedValue)
    {
        ShardCoder_.Encode(out, shardedValue.Shard());
        ValueCoder_.Encode(out, shardedValue.Value());
    }

    inline void Decode(IInputStream* in, TShardedValue<T>& shardedValue)
    {
        ShardCoder_.Decode(in, shardedValue.Shard());
        ValueCoder_.Decode(in, shardedValue.Value());
    }

private:
    TCoder<uint64_t> ShardCoder_;
    TCoder<T> ValueCoder_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
