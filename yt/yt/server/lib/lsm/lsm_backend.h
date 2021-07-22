#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_node/public.h>

namespace NYT::NLsm {

////////////////////////////////////////////////////////////////////////////////

struct TCompactionRequest
{
    TTabletPtr Tablet = nullptr;
    TPartitionId PartitionId;
    std::vector<TStoreId> Stores;
    int Slack = 0;
    int Effect = 0;
    bool DiscardStores = false;
};

struct TSamplePartitionRequest
{
    TTabletPtr Tablet = nullptr;
    TPartitionId PartitionId;
    int PartitionIndex = -1;
};

struct TSplitPartitionRequest
{
    TTabletPtr Tablet = nullptr;
    TPartitionId PartitionId;
    int PartitionIndex = -1;

    bool Immediate = false;
    int SplitFactor = -1;
};

struct TMergePartitionsRequest
{
    TTabletPtr Tablet = nullptr;
    int FirstPartitionIndex = -1;
    std::vector<TPartitionId> PartitionIds;
};

struct TLsmActionBatch
{
    std::vector<TCompactionRequest> Compactions;
    std::vector<TCompactionRequest> Partitionings;

    std::vector<TSamplePartitionRequest> Samplings;
    std::vector<TSplitPartitionRequest> Splits;
    std::vector<TMergePartitionsRequest> Merges;

    void MergeWith(TLsmActionBatch&& other);

    TString GetStatsLoggingString() const;

private:
    template <class TRequest>
    void DoMerge(TLsmActionBatch& other, std::vector<TRequest> TLsmActionBatch::* member);
};

struct TLsmBackendState
{
    TTimestamp CurrentTimestamp;
    NTabletNode::TTabletNodeConfigPtr TabletNodeConfig;
    NTabletNode::TTabletNodeDynamicConfigPtr TabletNodeDynamicConfig;
};

////////////////////////////////////////////////////////////////////////////////

/*!
 *    Entry point for all LSM stuff: compaction, partition balancing etc.
 *
 *    Each run consists of two parts. First, |SetLsmBackendState| could (but not
 *  necessarily must)  be called. Its purpose is to set global variables used by
 *  the algorithm, e.g. current timestamp. Then |BuildLsmActions| should be called.
 *  There may be several concurrent calls to the latter, each of them working on
 *  an independent set of tablets.
 *
 *    Particular parts of the algorithm (e.g. store compactor) also implement this
 *  interface and follow the aforementioned conventions.
 */
struct ILsmBackend
    : public virtual TRefCounted
{
    //! Sets global state.
    //!
    //! NB: Should not be called concurrently with |BuildLsmActions|.
    virtual void SetLsmBackendState(const TLsmBackendState& state) = 0;

    //! Returns a set of actions (e.g. compaction requests) for a set of tablets.
    //!
    //! NB: Context switches are possible.
    virtual TLsmActionBatch BuildLsmActions(const std::vector<TTabletPtr>& tablets) = 0;
};

DEFINE_REFCOUNTED_TYPE(ILsmBackend)

////////////////////////////////////////////////////////////////////////////////

ILsmBackendPtr CreateLsmBackend();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm

#define LSM_BACKEND_INL_H_
#include "lsm_backend-inl.h"
#undef LSM_BACKEND_INL_H_
