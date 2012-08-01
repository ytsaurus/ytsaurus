#pragma once

#include "public.h"

#include <ytlib/misc/lease_manager.h>
#include <ytlib/cell_master/public.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

//! Controls node server-side leases.
/*!
 *  Upon receiving a registration request from a node,
 *  TChunkManager registers its by calling #TNodeLeaseTracker::OnNodeRegistered.
 *  
 *  It also extends the leases by calling #TNodeLeaseTracker::OnNodeHeartbeat.
 *  
 *  When a lease expires #TNodeLeaseTracker triggers node deregistration
 *  by calling #TChunkManager::CreateUnregisterNodeMutation.
 *  The latter is a logged operation during which #TNodeLeaseTracker::OnNodeUnregistered
 *  gets called.
 *  
 *  Each registered node carries an additional "Confirmed" flag.
 *  The flag is used to distinguish between nodes that were registered during an earlier
 *  epoch (and whose actual liveness is not yet confirmed) and
 *  those nodes that have reported a heartbeat during the current epoch.
 *  
 *  This flag is raised automatically in #OnNodeHeartbeat.
 *  
 */
class TNodeLeaseTracker
    : public TRefCounted
{
public:
    //! Initializes an instance.
    /*!
     *  \param config A configuration.
     *  \param chunkManager A chunk manager.
     *  \param invoker An invoker used for lease expiration callbacks.
     */
    TNodeLeaseTracker(
        TChunkManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

    //! Registers the node and assigns it an initial lease.
    /*!
     *  Initial lease timeout for registered nodes is #TChunkManagerConfig::RegisteredNodeTimeout.
     *  For online nodes it is decreased to #TChunkManagerConfig::OnlineNodeTimeout.
     */
    void OnNodeRegistered(const THolder* node, bool recovery);

    //! Notifies that the node has become online and hence its lease timeout must be updated.
    void OnNodeOnline(const THolder* node, bool recovery);

    //! Renews the lease.
    void OnNodeHeartbeat(const THolder* node);

    //! Unregisters the node and stop tracking its lease.
    void OnNodeUnregistered(const THolder* node);

    //! Returns True iff the node is confirmed.
    bool IsNodeConfirmed(const THolder* node);

    //! Returns the number of nodes that are currently online (including unconfirmed).
    int GetOnlineNodeCount();

private:
    struct TNodeInfo
    {
        TNodeInfo()
            : Confirmed(false)
        { }

        TLeaseManager::TLease Lease;
        bool Confirmed;
    };

    typedef yhash_map<TNodeId, TNodeInfo> TNodeInfoMap;
     
    TChunkManagerConfigPtr Config;
    NCellMaster::TBootstrap* Bootstrap;

    int OnlineNodeCount;
    TNodeInfoMap NodeInfoMap;

    TNodeInfo* FindNodeInfo(TNodeId nodeId);
    TNodeInfo& GetNodeInfo(TNodeId nodeId);
    void RenewLease(const THolder* node, const TNodeInfo& nodeInfo);
    TDuration GetTimeout(const THolder* node, const TNodeInfo& nodeInfo);

    void OnExpired(TNodeId nodeId);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
