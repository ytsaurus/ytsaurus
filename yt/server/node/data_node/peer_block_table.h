#pragma once

#include "public.h"

#include <yt/server/node/cluster_node/public.h>

#include <yt/ytlib/chunk_client/block_id.h>

#include <yt/client/node_tracker_client/public.h>

#include <yt/core/misc/small_vector.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/concurrency/rw_spinlock.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Keeps information about all peers possibly holding a block.
/*!
 *  Thread affinity: any
 */
class TBlockPeerData
    : public TIntrinsicRefCounted
{
public:
    explicit TBlockPeerData(int entryCountLimit);

    static constexpr int TypicalPeerCount = 64;
    using TNodeIdList = SmallVector<TNodeId, TypicalPeerCount>;
    //! Returns all node ids (sweeping out expired ones).
    TNodeIdList GetPeers();

    //! Inserts a new peer.
    void AddPeer(TNodeId nodeId, TInstant expirationTime);

    //! Returns true if the entry list is still non-empty.
    bool Sweep();

private:
    template <class F>
    void DoSweep(F&& func);

    struct TBlockPeerEntry
    {
        TNodeId NodeId = NNodeTrackerClient::InvalidNodeId;
        TInstant ExpirationTime;
    };

    const int EntryCountLimit_;

    TSpinLock Lock_;
    SmallVector<TBlockPeerEntry, TypicalPeerCount * 2> Entries_;
};

DEFINE_REFCOUNTED_TYPE(TBlockPeerData)

////////////////////////////////////////////////////////////////////////////////

//! Manages peer blocks information.
/*!
 *
 *  When Data Node sends a block to a certain client
 *  ts address is remembered to facilitate peer-to-peer transfers.
 *  This class maintains an auto-expiring map for this purpose.
 *
 *  Thread affinity: any
 */
class TPeerBlockTable
    : public TRefCounted
{
public:
    TPeerBlockTable(
        TPeerBlockTableConfigPtr config,
        NClusterNode::TBootstrap* bootstrap);

    //! Retrieves peer data for a given #blockId.
    /*
     *  If #insert is true then always ensures an entry forr #blockId is created
     *  (if not already exists); otherwise may return null.
     */
    TBlockPeerDataPtr FindOrCreatePeerData(const TBlockId& blockId, bool insert);

private:
    const TPeerBlockTableConfigPtr Config_;

    const NConcurrency::TPeriodicExecutorPtr SweepExecutor_;

    NConcurrency::TReaderWriterSpinLock Lock_;
    THashMap<TBlockId, TBlockPeerDataPtr> BlockIdToData_;

    void OnSweep();
};

DEFINE_REFCOUNTED_TYPE(TPeerBlockTable)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
