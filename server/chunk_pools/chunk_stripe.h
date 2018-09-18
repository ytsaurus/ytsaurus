#pragma once

#include "private.h"
#include "chunk_stripe_key.h"

#include <yt/server/controller_agent/serialize.h>

#include <yt/ytlib/chunk_client/public.h>

namespace NYT {
namespace NChunkPools {

////////////////////////////////////////////////////////////////////////////////

struct TChunkStripeStatistics
{
    int ChunkCount = 0;
    i64 DataWeight = 0;
    i64 RowCount = 0;
    i64 MaxBlockSize = 0;

    void Persist(const TPersistenceContext& context);
};

TChunkStripeStatistics operator + (
    const TChunkStripeStatistics& lhs,
    const TChunkStripeStatistics& rhs);

TChunkStripeStatistics& operator += (
    TChunkStripeStatistics& lhs,
    const TChunkStripeStatistics& rhs);

typedef SmallVector<TChunkStripeStatistics, 1> TChunkStripeStatisticsVector;

//! Adds up input statistics and returns a single-item vector with the sum.
TChunkStripeStatisticsVector AggregateStatistics(
    const TChunkStripeStatisticsVector& statistics);

////////////////////////////////////////////////////////////////////////////////

struct TChunkStripe
    : public TIntrinsicRefCounted
{
    TChunkStripe(bool foreign = false, bool solid = false);
    explicit TChunkStripe(NChunkClient::TInputDataSlicePtr dataSlice, bool foreign = false, bool solid = false);
    explicit TChunkStripe(NChunkClient::TChunkListId, TBoundaryKeys boundaryKeys = TBoundaryKeys());

    TChunkStripeStatistics GetStatistics() const;
    int GetChunkCount() const;

    int GetTableIndex() const;

    int GetInputStreamIndex() const;

    void Persist(const TPersistenceContext& context);

    SmallVector<NChunkClient::TInputDataSlicePtr, 1> DataSlices;
    int WaitingChunkCount = 0;
    bool Foreign = false;
    bool Solid = false;

    NChunkClient::TChunkListId ChunkListId;
    TBoundaryKeys BoundaryKeys;
};

DEFINE_REFCOUNTED_TYPE(TChunkStripe)

////////////////////////////////////////////////////////////////////////////////

struct TChunkStripeList
    : public TIntrinsicRefCounted
{
    TChunkStripeList() = default;
    TChunkStripeList(int stripeCount);

    TChunkStripeStatisticsVector GetStatistics() const;
    TChunkStripeStatistics GetAggregateStatistics() const;

    void Persist(const TPersistenceContext& context);

    //! Setter that may be used in chain-like manner.
    TChunkStripeList* SetSplittable(bool splittable);

    std::vector<TChunkStripePtr> Stripes;

    TNullable<int> PartitionTag;

    //! If True then TotalDataWeight and TotalRowCount are approximate (and are hopefully upper bounds).
    bool IsApproximate = false;

    i64 TotalDataWeight = 0;
    i64 LocalDataWeight = 0;

    i64 TotalRowCount = 0;

    int TotalChunkCount = 0;
    int LocalChunkCount = 0;

    bool IsSplittable = true;
};

DEFINE_REFCOUNTED_TYPE(TChunkStripeList)

extern const TChunkStripeListPtr NullStripeList;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
