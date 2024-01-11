#pragma once

#include <yt/yt/server/lib/lsm/store.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NLsm::NTesting {

////////////////////////////////////////////////////////////////////////////////

using TKey = i64;
using TValue = i64;

struct TVersionedValue
{
    TValue Value;
    TTimestamp Timestamp;
    i64 DataSize = 1;

    void Persist(const TStreamPersistenceContext& context);
};

struct TRow
{
    TKey Key;
    std::vector<TVersionedValue> Values;
    std::vector<TTimestamp> DeleteTimestamps;

    i64 DataSize = 0;

    bool operator==(const TRow& other) const
    {
        return Key == other.Key;
    }

    bool operator<(const TRow& other) const
    {
        return Key < other.Key;
    }

    void Persist(const TStreamPersistenceContext& context);
};

inline TRow Row(i64 key, i64 dataSize = 1)
{
    return TRow{
        .Key = key,
        .Values = {{1, 1, dataSize}},
        .DataSize = dataSize,
    };
}

inline TRow Tombstone(i64 key, TTimestamp timestamp, i64 dataSize = 0)
{
    return TRow{
        .Key = key,
        .DeleteTimestamps = {timestamp},
        .DataSize = dataSize,
    };
}

inline TRow Row(i64 key, std::vector<TVersionedValue> values)
{
    i64 dataSize = 0;
    for (const auto& value : values) {
        dataSize += value.DataSize;
    }
    return TRow{
        .Key = key,
        .Values = std::move(values),
        .DataSize = dataSize,
    };
}

inline TRow Row(
    i64 key,
    std::initializer_list<TVersionedValue> values,
    std::vector<TTimestamp> deleteTimestamps)
{
    i64 dataSize = 0;
    for (const auto& value : values) {
        dataSize += value.DataSize;
    }
    return TRow{
        .Key = key,
        .Values = std::move(values),
        .DeleteTimestamps = std::move(deleteTimestamps),
        .DataSize = dataSize,
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
