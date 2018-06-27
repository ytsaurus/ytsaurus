#pragma once

#include "public.h"

#include <yp/server/objects/public.h>

#include <yp/client/nodes/public.h>

namespace NYP {
namespace NServer {
namespace NNodes {

////////////////////////////////////////////////////////////////////////////////

class TNodeTracker
    : public TRefCounted
{
public:
    TNodeTracker(NServer::NMaster::TBootstrap* bootstrap, TNodeTrackerConfigPtr config);

    NObjects::TNode* ProcessHandshake(
        const NObjects::TTransactionPtr& transaction,
        const NObjects::TObjectId& nodeId,
        const TString& address);

    void ProcessHeartbeat(
        NObjects::TNode* node,
        const TEpochId& epochId,
        ui64 sequenceNumber,
        const NClient::NNodes::NProto::TReqHeartbeat* request,
        NClient::NNodes::NProto::TRspHeartbeat* response);

    void NotifyAgent(NObjects::TNode* node);

private:
    class TImpl;
    const NYT::TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TNodeTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodes
} // namespace NServer
} // namespace NYP
