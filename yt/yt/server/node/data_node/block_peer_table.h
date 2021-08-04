#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/ytlib/chunk_client/block_id.h>

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/core/misc/small_vector.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/concurrency/spinlock.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Keeps information about peers possibly holding a block (or a whole chunk).
/*!
 *  \note
 *  Thread affinity: any
 */
class TCachedPeerList
    : public TRefCounted
{
public:
    explicit TCachedPeerList(int entryCountLimit);

    static constexpr int TypicalPeerCount = 64;
    using TNodeIdList = SmallVector<TNodeId, TypicalPeerCount>;
    //! Returns all node ids (sweeping out expired ones).
    TNodeIdList GetPeers();

    //! Inserts a new peer.
    void AddPeer(TNodeId nodeId, TInstant expirationDeadline);

    //! Returns true if the entry list is still non-empty.
    bool Sweep();

private:
    template <class F>
    void DoSweep(F&& func);

    struct TBlockPeerEntry
    {
        TNodeId NodeId = NNodeTrackerClient::InvalidNodeId;
        TInstant ExpirationDeadline;
    };

    const int EntryCountLimit_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, Lock_);
    SmallVector<TBlockPeerEntry, TypicalPeerCount * 2> Entries_;
};

DEFINE_REFCOUNTED_TYPE(TCachedPeerList)

////////////////////////////////////////////////////////////////////////////////

//! Manages known block peers.
/*!
 *
 *  When Data Node sends a block to a certain client
 *  its address is remembered to facilitate peer-to-peer transfers.
 *
 *  Also, when a chunk removal job is issued, the master also
 *  provides the list of (other) known replicas to the node.
 *  These replicas are remembered and are suggested to requesters.
 *
 *  This class maintains an auto-expiring map maintaining the above data.
 *
 *  \note
 *  Thread affinity: any
 */
class TBlockPeerTable
    : public TRefCounted
{
public:
    explicit TBlockPeerTable(IBootstrap* bootstrap);

    //! Retrieves peer list for a given #blockId; returns null if none is known.
    TCachedPeerListPtr FindPeerList(const TBlockId& blockId);

    //! Retrieves peer list for a given #blockId.
    //! Always ensures an entry for #blockId is created (if not already exists).
    TCachedPeerListPtr GetOrCreatePeerList(const TBlockId& blockId);

private:
    const TBlockPeerTableConfigPtr Config_;

    const NConcurrency::TPeriodicExecutorPtr SweepExecutor_;

    YT_DECLARE_SPINLOCK(NConcurrency::TReaderWriterSpinLock, Lock_);
    THashMap<TBlockId, TCachedPeerListPtr> BlockIdToPeerList_;

    void OnSweep();
};

DEFINE_REFCOUNTED_TYPE(TBlockPeerTable)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
