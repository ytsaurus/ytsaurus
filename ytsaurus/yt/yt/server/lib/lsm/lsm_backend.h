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
    EStoreCompactionReason Reason = EStoreCompactionReason::None;
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

struct TRotateStoreRequest
{
    TTabletPtr Tablet = nullptr;
    EStoreRotationReason Reason = EStoreRotationReason::None;
    TInstant ExpectedLastPeriodicRotationTime = TInstant::Zero();
    TInstant NewLastPeriodicRotationTime = TInstant::Zero();
};

struct TLsmActionBatch
{
    std::vector<TCompactionRequest> Compactions;
    std::vector<TCompactionRequest> Partitionings;

    std::vector<TSamplePartitionRequest> Samplings;
    std::vector<TSplitPartitionRequest> Splits;
    std::vector<TMergePartitionsRequest> Merges;

    std::vector<TRotateStoreRequest> Rotations;

    void MergeWith(TLsmActionBatch&& other);

    TString GetStatsLoggingString() const;

private:
    template <class TRequest>
    void DoMerge(TLsmActionBatch& other, std::vector<TRequest> TLsmActionBatch::* member);
};

struct TTabletCellBundleState
{
    double ForcedRotationMemoryRatio = 0;
    bool EnablePerBundleMemoryLimit = true;
    i64 DynamicMemoryLimit = 0;
    i64 DynamicMemoryUsage = 0;
};

struct TLsmBackendState
{
    TTimestamp CurrentTimestamp;
    NTabletNode::TTabletNodeConfigPtr TabletNodeConfig;
    NTabletNode::TTabletNodeDynamicConfigPtr TabletNodeDynamicConfig;

    i64 DynamicMemoryLimit = 0;
    i64 DynamicMemoryUsage = 0;

    THashMap<TString, TTabletCellBundleState> Bundles;

    TInstant CurrentTime;
};

////////////////////////////////////////////////////////////////////////////////

/*!
 *    Entry point for all LSM stuff: compaction, partition balancing etc.
 *
 *    Each round consists of three parts.
 *    First, |StartNewRound| should be called. Its purpose is to reset the state and
 *  set global variables used by the algorithm, e.g. current timestamp.
 *    Second, |BuildLsmActions| should be called. There may be several concurrent
 *  calls to the latter, each of them working on an independent set of tablets.
 *  Each of these calls may return some actions.
 *    Finally, |BuildOverallLsmActions| should be called. This call returns
 *  a set of actions that require knowing all tablets at the node to be built.
 *
 *    Particular parts of the algorithm (e.g. store compactor) also implement this
 *  interface and follow the aforementioned conventions.
 */
struct ILsmBackend
    : public virtual TRefCounted
{
    //! Sets global state.
    //!
    //! NB: Should not be called concurrently with |Build(Overall)LsmActions|.
    virtual void StartNewRound(const TLsmBackendState& state) = 0;

    //! Returns a set of actions (e.g. compaction requests) for a set of tablets.
    //! May be called concurrently.
    //!
    //! NB: Context switches are possible.
    virtual TLsmActionBatch BuildLsmActions(
        const std::vector<TTabletPtr>& tablets,
        const TString& tabletCellBundle) = 0;

    //! Returns a set of actions built after all tablets were seen.
    //! Should be called once per round.
    //!
    //! NB: Context switches are possible.
    virtual TLsmActionBatch BuildOverallLsmActions() = 0;
};

DEFINE_REFCOUNTED_TYPE(ILsmBackend)

////////////////////////////////////////////////////////////////////////////////

ILsmBackendPtr CreateLsmBackend();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm

#define LSM_BACKEND_INL_H_
#include "lsm_backend-inl.h"
#undef LSM_BACKEND_INL_H_
