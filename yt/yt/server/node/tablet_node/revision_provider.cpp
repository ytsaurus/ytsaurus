#include "revision_provider.h"

#include "private.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

TTwoLevelRevisionProvider::TTwoLevelRevisionProvider()
{
    // Reserve the vector to prevent reallocations and thus enable accessing
    // it from arbitrary threads.
    RevisionToTimestamp_.ReserveChunks(MaxRevisionChunks);
    RevisionToTimestamp_.PushBack(NullTimestamp);
    RevisionToTimestamp_[NullRevision.Underlying()] = NullTimestamp;
}

TSortedDynamicStoreRevision TTwoLevelRevisionProvider::GetLatestRevision() const
{
    YT_VERIFY(!RevisionToTimestamp_.Empty());
    return TSortedDynamicStoreRevision(RevisionToTimestamp_.Size() - 1);
}

TSortedDynamicStoreRevision TTwoLevelRevisionProvider::RegisterRevision(TTimestamp timestamp, std::optional<i64> mutationSequenceNumber)
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

TTimestamp TTwoLevelRevisionProvider::TimestampFromRevision(TSortedDynamicStoreRevision revision) const
{
    return RevisionToTimestamp_[revision.Underlying()];
}

i64 TTwoLevelRevisionProvider::GetTimestampCount() const
{
    return RevisionToTimestamp_.Size();
}

i64 TTwoLevelRevisionProvider::GetSoftTimestampCountLimit() const
{
    return TwoLevelSoftRevisionsPerDynamicStoreLimit;
}

////////////////////////////////////////////////////////////////////////////////

TThreeLevelRevisionProvider::TThreeLevelRevisionProvider()
{
    RevisionToTimestamp_.PushBack(NullTimestamp);
    YT_VERIFY(TimestampFromRevision(NullRevision) == NullTimestamp);
}

TSortedDynamicStoreRevision TThreeLevelRevisionProvider::GetLatestRevision() const
{
    YT_VERIFY(!RevisionToTimestamp_.Empty());
    return TSortedDynamicStoreRevision(RevisionToTimestamp_.Size() - 1);
}

TSortedDynamicStoreRevision TThreeLevelRevisionProvider::RegisterRevision(TTimestamp timestamp, std::optional<i64> mutationSequenceNumber)
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

TTimestamp TThreeLevelRevisionProvider::TimestampFromRevision(TSortedDynamicStoreRevision revision) const
{
    return RevisionToTimestamp_[revision.Underlying()];
}

i64 TThreeLevelRevisionProvider::GetTimestampCount() const
{
    return RevisionToTimestamp_.Size();
}

i64 TThreeLevelRevisionProvider::GetSoftTimestampCountLimit() const
{
    return ThreeLevelSoftRevisionsPerDynamicStoreLimit;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
