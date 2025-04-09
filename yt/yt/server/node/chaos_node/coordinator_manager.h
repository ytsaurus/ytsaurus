#pragma once

#include "public.h"
#include "replication_card.h"

#include <yt/yt/ytlib/chaos_client/public.h>

#include <yt/yt/ytlib/chaos_client/proto/coordinator_service.pb.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

using TSuspendCoordinatorContextPtr = TIntrusivePtr<NYT::NRpc::TTypedServiceContext<
    NYT::NChaosClient::NProto::TReqSuspendCoordinator,
    NYT::NChaosClient::NProto::TRspSuspendCoordinator>>;
using TResumeCoordinatorContextPtr = TIntrusivePtr<NYT::NRpc::TTypedServiceContext<
    NYT::NChaosClient::NProto::TReqResumeCoordinator,
    NYT::NChaosClient::NProto::TRspResumeCoordinator>>;

////////////////////////////////////////////////////////////////////////////////

struct ICoordinatorManager
    : public virtual TRefCounted
{
public:
    virtual void Initialize() = 0;

    virtual NYTree::IYPathServicePtr GetOrchidService() = 0;

    virtual void SuspendCoordinator(TSuspendCoordinatorContextPtr context) = 0;
    virtual void ResumeCoordinator(TResumeCoordinatorContextPtr context) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICoordinatorManager)

ICoordinatorManagerPtr CreateCoordinatorManager(
    TCoordinatorManagerConfigPtr config,
    IChaosSlotPtr slot,
    IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
