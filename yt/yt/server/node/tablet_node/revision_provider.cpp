#include "revision_provider.h"

#include "private.h"

#include <library/cpp/yt/containers/chunked_vector.h>
#include <library/cpp/yt/containers/three_level_stable_vector.h>

namespace NYT::NTabletNode {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TTwoLevelRevisionProvider
    : public IRevisionProvider
{
public:
    TTwoLevelRevisionProvider()
    {
        // Reserve the vector to prevent reallocations and thus enable accessing
        // it from arbitrary threads.
        RevisionToTimestamp_.ReserveChunks(MaxRevisionChunks);
        RevisionToTimestamp_.PushBack(NullTimestamp);
        RevisionToTimestamp_[NullRevision.Underlying()] = NullTimestamp;
    }

    TSortedDynamicStoreRevision GetLatestRevision() const final
    {
        YT_VERIFY(!RevisionToTimestamp_.Empty());
        return TSortedDynamicStoreRevision(RevisionToTimestamp_.Size() - 1);
    }

    TSortedDynamicStoreRevision RegisterRevision(TTimestamp timestamp, std::optional<i64> mutationSequenceNumber) final
    {
        YT_VERIFY(timestamp >= MinTimestamp && timestamp <= MaxTimestamp);

        i64 resolvedMutationSequenceNumber = mutationSequenceNumber.value_or(0);

        auto latestRevision = GetLatestRevision();
        if (resolvedMutationSequenceNumber == LatestRevisionMutationSequenceNumber_ &&
            TimestampFromRevision(latestRevision) == timestamp)
        {
            return latestRevision;
        }

        YT_VERIFY(RevisionToTimestamp_.Size() < TwoLevelHardRevisionsPerDynamicStoreLimit);
        RevisionToTimestamp_.PushBack(timestamp);
        LatestRevisionMutationSequenceNumber_ = resolvedMutationSequenceNumber;

        return GetLatestRevision();
    }

    TTimestamp TimestampFromRevision(TSortedDynamicStoreRevision revision) const final
    {
        return RevisionToTimestamp_[revision.Underlying()];
    }

    i64 GetTimestampCount() const final
    {
        return RevisionToTimestamp_.Size();
    }

    i64 GetSoftTimestampCountLimit() const final
    {
        return TwoLevelSoftRevisionsPerDynamicStoreLimit;
    }

private:
    static constexpr size_t RevisionsPerChunk = 1ULL << 13;
    static constexpr size_t MaxRevisionChunks = TwoLevelHardRevisionsPerDynamicStoreLimit / RevisionsPerChunk + 1;

    TChunkedVector<TTimestamp, RevisionsPerChunk> RevisionToTimestamp_;
    i64 LatestRevisionMutationSequenceNumber_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TThreeLevelRevisionProvider
    : public IRevisionProvider
{
public:
    TThreeLevelRevisionProvider()
    {
        RevisionToTimestamp_.PushBack(NullTimestamp);
        YT_VERIFY(TimestampFromRevision(NullRevision) == NullTimestamp);
    }

    TSortedDynamicStoreRevision GetLatestRevision() const final
    {
        YT_VERIFY(!RevisionToTimestamp_.Empty());
        return TSortedDynamicStoreRevision(RevisionToTimestamp_.Size() - 1);
    }

    TSortedDynamicStoreRevision RegisterRevision(TTimestamp timestamp, std::optional<i64> mutationSequenceNumber) final
    {
        YT_VERIFY(timestamp >= MinTimestamp && timestamp <= MaxTimestamp);

        i64 resolvedMutationSequenceNumber = mutationSequenceNumber.value_or(0);

        auto latestRevision = GetLatestRevision();
        if (resolvedMutationSequenceNumber == LatestRevisionMutationSequenceNumber_ &&
            TimestampFromRevision(latestRevision) == timestamp)
        {
            return latestRevision;
        }

        YT_VERIFY(RevisionToTimestamp_.Size() < ThreeLevelHardRevisionsPerDynamicStoreLimit);
        RevisionToTimestamp_.PushBack(timestamp);
        LatestRevisionMutationSequenceNumber_ = resolvedMutationSequenceNumber;

        return GetLatestRevision();
    }

    TTimestamp TimestampFromRevision(TSortedDynamicStoreRevision revision) const final
    {
        return RevisionToTimestamp_[revision.Underlying()];
    }

    i64 GetTimestampCount() const final
    {
        return RevisionToTimestamp_.Size();
    }

    i64 GetSoftTimestampCountLimit() const final
    {
        return ThreeLevelSoftRevisionsPerDynamicStoreLimit;
    }

private:
    static constexpr size_t RevisionsPerChunk = 1ULL << 13;
    static_assert(ThreeLevelHardRevisionsPerDynamicStoreLimit == 1LL << 31);

    TThreeLevelStableVector<TTimestamp, RevisionsPerChunk, RevisionsPerChunk, ThreeLevelHardRevisionsPerDynamicStoreLimit> RevisionToTimestamp_;
    i64 LatestRevisionMutationSequenceNumber_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

IRevisionProviderPtr CreateTwoLevelRevisionProvider()
{
    return New<TTwoLevelRevisionProvider>();
}

IRevisionProviderPtr CreateThreeLevelRevisionProvider()
{
    return New<TThreeLevelRevisionProvider>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
