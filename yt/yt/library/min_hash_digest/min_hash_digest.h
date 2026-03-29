#pragma once

#include "public.h"

#include <yt/yt/core/misc/public.h>

#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/yt/farmhash/farm_hash.h>

#include <library/cpp/yt/misc/property.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TTimestampComparator>
struct TTimestampedHash
{
    TFingerprint Hash;
    ui32 PrimaryTimestamp;
    // Being used for digest merge, is not serialized in blocks.
    ui32 AuxiliaryTimestamp = 0;

    bool operator<(const TTimestampedHash& other) const;

    static constexpr size_t SerializedSize = sizeof(TFingerprint) + sizeof(ui32);
};

////////////////////////////////////////////////////////////////////////////////

template <class TTimestampComparator>
using TMinHashItems = std::vector<TTimestampedHash<TTimestampComparator>>;

template <class TTimestampComparator>
struct TMinHash
{
public:
    DEFINE_BYVAL_RO_PROPERTY(int, Capacity);
    DEFINE_BYREF_RO_PROPERTY(TMinHashItems<TTimestampComparator>, Items);

public:
    TMinHash() = default;
    TMinHash(int capacity, TMinHashItems<TTimestampComparator> items);

    i64 GetWeight() const;
    i64 GetSerializedSize() const;

    void Write(char*& ptr) const;
    void Read(const char*& ptr);

    static TMinHash Merge(const TMinHash& lhs, const TMinHash& rhs);
};

using TWriteMinHash = TMinHash<std::less<ui32>>;
using TDeleteMinHash = TMinHash<std::greater<ui32>>;

////////////////////////////////////////////////////////////////////////////////

template <class TTimestampComparator>
class TMinHashAccumulator
{
public:
    explicit TMinHashAccumulator(int capacity);

    void Add(TFingerprint hash, ui32 timestamp);

    TMinHash<TTimestampComparator> Finish();

private:
    int Capacity_;
    TMinHashItems<TTimestampComparator> ItemsBuffer_;

private:
    void EnsureCompacted();
};

using TWriteMinHashAccumulator = TMinHashAccumulator<std::less<ui32>>;
using TDeleteMinHashAccumulator = TMinHashAccumulator<std::greater<ui32>>;

////////////////////////////////////////////////////////////////////////////////

// Defines which timestamp is used for calculating result timestamp.
// LSM uses |Auxiliary| only if |MinDataVersions| is equal to 1.
DEFINE_ENUM(EMinHashWriteSimilarityMode,
    (Primary)
    (Auxiliary)
);

class TMinHashDigest
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RO_BOOLEAN_PROPERTY(Initialized);

public:
    explicit TMinHashDigest(IMemoryUsageTrackerPtr memoryTracker);

    ~TMinHashDigest();

    void Initialize(TSharedRef data);

    void Initialize(
        TWriteMinHash&& writeMinHash,
        TDeleteMinHash&& deleleMinHash);

    TSharedRef BuildSerialized() const;

    ui32 CalculateWriteDeleteSimilarityTimestamp(const TMinHashSimilarityConfigPtr& similarityConfig) const;

    ui32 CalculateWritesSimilarityTimestamp(
        const TMinHashSimilarityConfigPtr& similarityConfig,
        EMinHashWriteSimilarityMode similarityMode) const;

    static TMinHashDigestPtr Merge(const TMinHashDigestPtr& lhs, const TMinHashDigestPtr& rhs);

private:
    static constexpr ui32 FormatVersion = 1;

    const IMemoryUsageTrackerPtr MemoryTracker_;

    TWriteMinHash WriteMinHash_;
    TDeleteMinHash DeleteMinHash_;

private:
    ui32 CalculateSufficientTimestamp(
        const TMinHashSimilarityConfigPtr& similarityConfig,
        std::vector<ui32>&& intersectionTimestamps,
        int comparisonSize) const;

    i64 GetWeight() const;

    void TrackMemory();
};

DEFINE_REFCOUNTED_TYPE(TMinHashDigest)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
