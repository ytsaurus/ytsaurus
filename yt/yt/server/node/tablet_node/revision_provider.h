#pragma once

#include "public.h"

#include <yt/yt/core/misc/chunked_vector.h>
#include <yt/yt/core/misc/three_level_stable_vector.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! Hard limits are hard.
//! Moreover, it is quite expensive to be graceful in preventing it from being exceeded.
//! The corresponding soft limits, thus, is significantly smaller.

constexpr i64 TwoLevelHardRevisionsPerDynamicStoreLimit = 1ULL << 26;
constexpr i64 TwoLevelSoftRevisionsPerDynamicStoreLimit = 1ULL << 25;
static_assert(TwoLevelSoftRevisionsPerDynamicStoreLimit < TwoLevelHardRevisionsPerDynamicStoreLimit);
static_assert(TwoLevelSoftRevisionsPerDynamicStoreLimit <= SoftRevisionsPerDynamicStoreLimit);

// NB: 2^31 instead of 2^32 is chosen to avoid clushing with InvalidRevision and MaxRevision.
constexpr i64 ThreeLevelHardRevisionsPerDynamicStoreLimit = 1ULL << 31;
constexpr i64 ThreeLevelSoftRevisionsPerDynamicStoreLimit = 1ULL << 30;
static_assert(ThreeLevelSoftRevisionsPerDynamicStoreLimit < ThreeLevelHardRevisionsPerDynamicStoreLimit);
static_assert(ThreeLevelSoftRevisionsPerDynamicStoreLimit <= SoftRevisionsPerDynamicStoreLimit);

////////////////////////////////////////////////////////////////////////////////

struct IRevisionProvider
    : public TRefCounted
{
    virtual TSortedDynamicStoreRevision GetLatestRevision() const = 0;
    virtual TSortedDynamicStoreRevision RegisterRevision(TTimestamp timestamp, std::optional<i64> mutationSequenceNumber) = 0;
    virtual TTimestamp TimestampFromRevision(TSortedDynamicStoreRevision revision) const = 0;
    virtual i64 GetTimestampCount() const = 0;
    virtual i64 GetSoftTimestampCountLimit() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IRevisionProvider);

class TTwoLevelRevisionProvider
    : public IRevisionProvider
{
public:
    TTwoLevelRevisionProvider();

    TSortedDynamicStoreRevision GetLatestRevision() const final;
    TSortedDynamicStoreRevision RegisterRevision(TTimestamp timestamp, std::optional<i64> mutationSequenceNumber) final;
    TTimestamp TimestampFromRevision(TSortedDynamicStoreRevision revision) const final;
    i64 GetTimestampCount() const final;
    i64 GetSoftTimestampCountLimit() const final;

private:
    static constexpr size_t RevisionsPerChunk = 1ULL << 13;
    static constexpr size_t MaxRevisionChunks = TwoLevelHardRevisionsPerDynamicStoreLimit / RevisionsPerChunk + 1;

    TChunkedVector<TTimestamp, RevisionsPerChunk> RevisionToTimestamp_;
    i64 LatestRevisionMutationSequenceNumber_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TTwoLevelRevisionProvider);

class TThreeLevelRevisionProvider
    : public IRevisionProvider
{
public:
    TThreeLevelRevisionProvider();

    TSortedDynamicStoreRevision GetLatestRevision() const final;
    TSortedDynamicStoreRevision RegisterRevision(TTimestamp timestamp, std::optional<i64> mutationSequenceNumber) final;
    TTimestamp TimestampFromRevision(TSortedDynamicStoreRevision revision) const final;
    i64 GetTimestampCount() const final;
    i64 GetSoftTimestampCountLimit() const final;

private:
    static constexpr size_t RevisionsPerChunk = 1ULL << 13;
    static_assert(ThreeLevelHardRevisionsPerDynamicStoreLimit == 1LL << 31);

    TThreeLevelStableVector<TTimestamp, RevisionsPerChunk, RevisionsPerChunk, ThreeLevelHardRevisionsPerDynamicStoreLimit> RevisionToTimestamp_;
    i64 LatestRevisionMutationSequenceNumber_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TThreeLevelRevisionProvider);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
