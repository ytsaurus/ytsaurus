#pragma once

#include "private.h"
#include "chunk_stripe_key.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/table_client/chunk_stripe_statistics.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

struct TChunkStripe
    : public TRefCounted
{
    TChunkStripe(bool foreign = false, bool solid = false);
    explicit TChunkStripe(NChunkClient::TLegacyDataSlicePtr dataSlice, bool foreign = false, bool solid = false);
    explicit TChunkStripe(const std::vector<NChunkClient::TLegacyDataSlicePtr>& dataSlices);
    explicit TChunkStripe(NChunkClient::TChunkListId, TBoundaryKeys boundaryKeys = TBoundaryKeys());

    NTableClient::TChunkStripeStatistics GetStatistics() const;
    int GetChunkCount() const;

    int GetTableIndex() const;

    int GetInputStreamIndex() const;

    TCompactVector<NChunkClient::TLegacyDataSlicePtr, 1> DataSlices;
    int WaitingChunkCount = 0;
    bool Foreign = false;
    bool Solid = false;

    NChunkClient::TChunkListId ChunkListId;
    TBoundaryKeys BoundaryKeys;

    //! This field represents correspondence of chunk stripe to chunk pool in multi chunk pool.
    //! For example, it may represent partition index in intermediate sort or output table index in sink.
    std::optional<int> PartitionTag = std::nullopt;

    PHOENIX_DECLARE_TYPE(TChunkStripe, 0x20bf907f);
};

DEFINE_REFCOUNTED_TYPE(TChunkStripe)

////////////////////////////////////////////////////////////////////////////////

struct TChunkStripeList
    : public TRefCounted
{
    TChunkStripeList() = default;
    TChunkStripeList(int stripeCount);

    NTableClient::TChunkStripeStatisticsVector GetStatistics() const;
    NTableClient::TChunkStripeStatistics GetAggregateStatistics() const;

    void AddStripe(TChunkStripePtr stripe);

    std::vector<TChunkStripePtr> Stripes;

    std::optional<int> PartitionTag;

    //! If True then TotalDataWeight and TotalRowCount are approximate (and are hopefully upper bounds).
    bool IsApproximate = false;

    i64 TotalDataWeight = 0;
    i64 LocalDataWeight = 0;

    i64 TotalRowCount = 0;
    i64 TotalValueCount = 0;

    i64 TotalCompressedDataSize = 0;

    int TotalChunkCount = 0;
    int LocalChunkCount = 0;

    PHOENIX_DECLARE_TYPE(TChunkStripeList, 0x85f55d0b);
};

DEFINE_REFCOUNTED_TYPE(TChunkStripeList)

extern const TChunkStripeListPtr NullStripeList;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
