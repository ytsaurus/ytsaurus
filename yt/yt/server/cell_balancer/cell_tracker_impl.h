#pragma once

#include "bootstrap.h"

#include <yt/yt/server/master/cell_server/cell_balancer.h>
#include <yt/yt/server/master/cell_server/cell_base.h>

#include <yt/yt/ytlib/cell_balancer/proto/cell_tracker_service.pb.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

class TCellTrackerImpl
    : public TRefCounted
{
public:
    TCellTrackerImpl(
        IBootstrap* bootstrap,
        TInstant startTime,
        NTabletServer::TDynamicTabletManagerConfigPtr config);

    void ScanCells();

private:
    IBootstrap* const Bootstrap_;
    const TInstant StartTime_;
    NTabletServer::TDynamicTabletManagerConfigPtr Config_;

    TClusterStateProviderPtr ClusterStateProvider_;

    void ScanCellarCells(
        NCellarClient::ECellarType cellarType,
        NCellServer::ICellBalancerProviderPtr cellarProvider,
        NCellBalancerClient::NProto::TReqReassignPeers* request);
    bool ScheduleLeaderReassignment(
        NCellServer::TCellBase* cell,
        NCellBalancerClient::NProto::TReqReassignPeers* request);
    void SchedulePeerAssignment(NCellServer::TCellBase* cell, NCellServer::ICellBalancer* balancer);
    void SchedulePeerRevocation(NCellServer::TCellBase* cell, NCellServer::ICellBalancer* balancer);
    bool SchedulePeerCountChange(
        NCellServer::TCellBase* cell,
        NCellBalancerClient::NProto::TReqReassignPeers* request);

    TError IsFailed(
        const NCellServer::TCellBase::TPeer& peer,
        const NCellServer::TCellBase* cell,
        TDuration timeout);
    bool IsDecommissioned(
        const NNodeTrackerServer::TNode* node,
        const NCellServer::TCellBase* cell);

    int FindGoodPeer(const NCellServer::TCellBase* cell);
    int FindGoodFollower(const NCellServer::TCellBase* cell);

    void UpdateDynamicConfig();
};

DEFINE_REFCOUNTED_TYPE(TCellTrackerImpl)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
