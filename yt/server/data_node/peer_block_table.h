#pragma once

#include "public.h"

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/server/cell_node/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Keeps information about a peer possibly holding a block.
struct TPeerInfo
{
    NNodeTrackerClient::TNodeDescriptor Descriptor;
    TInstant ExpirationTime;

    TPeerInfo()
    { }

    TPeerInfo(
        const NNodeTrackerClient::TNodeDescriptor& descriptor,
        TInstant expirationTime)
        : Descriptor(descriptor)
        , ExpirationTime(expirationTime)
    { }
};

////////////////////////////////////////////////////////////////////////////////

//! When Data Node sends a block to a certain client
//! its address is remembered to facilitate peer-to-peer transfers.
//! This class maintains an auto-expiring map for this purpose.
class TPeerBlockTable
    : public TRefCounted
{
public:
    explicit TPeerBlockTable(TPeerBlockTableConfigPtr config, NCellNode::TBootstrap* bootstrap);
    
    //! Gets peers where a particular block was sent to.
    /*!
     *  Also sweeps expired peers.
     *
     *  \note Invoker affinity: Control invoker
     */
    const std::vector<TPeerInfo>& GetPeers(const TBlockId& blockId);

    //! For a given block, registers a new peer or updates the existing one.
    /*!
     *  Also sweeps expired peers.
     *
     *  \note Invoker affinity: Control invoker
     */
    void UpdatePeer(const TBlockId& blockId, const TPeerInfo& peer);

private:
    const TPeerBlockTableConfigPtr Config_;
    NCellNode::TBootstrap* const Bootstrap_;

    //! Each vector is sorted by decreasing expiration time.
    yhash<TBlockId, std::vector<TPeerInfo>> Table_;

    TInstant LastSwept_;


    static void SweepExpiredPeers(std::vector<TPeerInfo>& peers);
    void SweepAllExpiredPeers();

    std::vector<TPeerInfo>& GetMutablePeers(const TBlockId& blockId);

};

DEFINE_REFCOUNTED_TYPE(TPeerBlockTable)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
