#pragma once

#include "chunk_stripe_key.h"
#include "private.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/table_client/chunk_stripe_statistics.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

class TChunkStripe
    : public TRefCounted
{
public:
    using TDataSlices = TCompactVector<NChunkClient::TLegacyDataSlicePtr, 1>;

    explicit TChunkStripe(bool foreign = false);
    explicit TChunkStripe(NChunkClient::TLegacyDataSlicePtr dataSlice);

    DEFINE_BYREF_RW_PROPERTY(TDataSlices, DataSlices);

    NTableClient::TChunkStripeStatistics GetStatistics() const;
    int GetChunkCount() const;

    //! Input table index. May be -1 if chunk stripe is intermediate.
    int GetTableIndex() const;

    int GetInputStreamIndex() const;

    // TODO(apollo1321): This field is only used in TInputManager and should be moved there.
    DEFINE_BYREF_RW_PROPERTY(int, WaitingChunkCount);

    DEFINE_BYVAL_RW_BOOLEAN_PROPERTY(Foreign);

    DEFINE_BYVAL_RW_PROPERTY(NChunkClient::TChunkListId, ChunkListId);

    DEFINE_BYVAL_RW_PROPERTY(TBoundaryKeys, BoundaryKeys);

    //! This field represents correspondence of chunk stripe to chunk pool in multi chunk pool.
    //! For example, it may represent partition index in intermediate sort or output table index in sink.
    //! It is not used for filtering chunk blocks.
    DEFINE_BYVAL_RW_PROPERTY(std::optional<int>, PartitionTag);

private:
    PHOENIX_DECLARE_TYPE(TChunkStripe, 0x20bf907f);
};

DEFINE_REFCOUNTED_TYPE(TChunkStripe)

////////////////////////////////////////////////////////////////////////////////

struct TPersistentChunkStripeStatistics
    : public NTableClient::TChunkStripeStatistics
{
    void Persist(const TPersistenceContext& context);
};

////////////////////////////////////////////////////////////////////////////////

class TChunkStripeList
    : public TRefCounted
{
public:
    DEFINE_BYREF_RO_PROPERTY(std::vector<TChunkStripePtr>, Stripes);
    DEFINE_BYVAL_RO_PROPERTY(i64, SliceCount);

    //! If True then DataWeight and RowCount are approximate (and are hopefully upper bounds).
    DEFINE_BYVAL_RW_BOOLEAN_PROPERTY(Approximate);

    void Reserve(i64 size);

    void SetPartitionTag(int partitionTag);

    // Set partition tag and override data size.
    void SetPartitionTag(int partitionTag, i64 dataWeight, i64 rowCount);

    void AddStripe(TChunkStripePtr stripe);

    std::optional<int> GetPartitionTag() const;

    NTableClient::TChunkStripeStatisticsVector GetPerStripeStatistics() const;
    NTableClient::TChunkStripeStatistics GetAggregateStatistics() const;

private:
    std::optional<int> PartitionTag_;
    std::optional<i64> OverriddenDataWeight_;
    std::optional<i64> OverriddenRowCount_;

    TPersistentChunkStripeStatistics Statistics_;

    PHOENIX_DECLARE_TYPE(TChunkStripeList, 0x85f55d0b);
};

DEFINE_REFCOUNTED_TYPE(TChunkStripeList)

extern const TChunkStripeListPtr NullStripeList;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
