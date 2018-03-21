#include "node_tracker_service.h"
#include "node_tracker.h"
#include "config.h"
#include "private.h"

#include <yp/server/master/bootstrap.h>
#include <yp/server/master/service_detail.h>

#include <yp/server/objects/transaction_manager.h>
#include <yp/server/objects/transaction.h>
#include <yp/server/objects/node.h>

#include <yp/client/nodes/node_tracker_service_proxy.h>

#include <yt/core/rpc/grpc/proto/grpc.pb.h>

namespace NYP {
namespace NServer {
namespace NNodes {

using namespace NClient::NNodes;
using namespace NServer::NObjects;

using namespace NYT::NRpc;
using namespace NYT::NConcurrency;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TNodeTrackerService
    : public NMaster::TServiceBase
{
public:
    TNodeTrackerService(
        NMaster::TBootstrap* bootstrap,
        TNodeTrackerConfigPtr config)
        : TServiceBase(
            bootstrap,
            TNodeTrackerServiceProxy::GetDescriptor(),
            NNodes::Logger)
        , Config_(std::move(config))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Handshake));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Heartbeat));
    }

private:
    const TNodeTrackerConfigPtr Config_;

    DECLARE_RPC_SERVICE_METHOD(NClient::NNodes::NProto, Handshake)
    {
        const auto& nodeId = request->node_id();
        const auto& address = request->address();

        context->SetRequestInfo("NodeId: %v, Address: %v",
            nodeId,
            address);

        ValidateAgentIdentity(context, nodeId);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto transaction = WaitFor(transactionManager->StartReadWriteTransaction())
            .ValueOrThrow();

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->ProcessHandshake(transaction, nodeId, address);

        WaitFor(transaction->Commit())
            .ThrowOnError();

        auto epochId = node->Status().EpochId().Load();

        ToProto(response->mutable_node_id(), nodeId);
        ToProto(response->mutable_epoch_id(), epochId);

        context->SetResponseInfo("NodeId: %v, EpochId: %v",
            nodeId,
            epochId);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NClient::NNodes::NProto, Heartbeat)
    {
        auto nodeId = FromProto<TObjectId>(request->node_id());
        auto epochId = FromProto<TEpochId>(request->epoch_id());
        auto sequenceNumber = request->sequence_number();

        context->SetRequestInfo("NodeId: %v, EpochId: %v, SequenceNumber: %v",
            nodeId,
            epochId,
            sequenceNumber);

        ValidateAgentIdentity(context, nodeId);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto transaction = WaitFor(transactionManager->StartReadWriteTransaction())
            .ValueOrThrow();

        auto* node = transaction->GetNode(nodeId);

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        nodeTracker->ProcessHeartbeat(
            transaction,
            node,
            epochId,
            sequenceNumber,
            request,
            response);

        WaitFor(transaction->Commit())
            .ThrowOnError();

        context->Reply();
    }


    void ValidateAgentIdentity(const NRpc::IServiceContextPtr& context, const TObjectId& nodeId)
    {
        if (!Config_->ValidateAgentIdentity) {
            return;
        }

        const auto& ext = context->GetRequestHeader().GetExtension(NYT::NRpc::NGrpc::NProto::TSslCredentialsExt::ssl_credentials_ext);
        if (!ext.has_peer_identity()) {
            THROW_ERROR_EXCEPTION("Node %Qv did not provide an SSL identity",
                nodeId);
        }
        if (ext.peer_identity() != nodeId) {
            THROW_ERROR_EXCEPTION("Node %Qv has provided a wrong SSL identity %Qv",
                nodeId,
                ext.peer_identity());
        }
    }
};

IServicePtr CreateNodeTrackerService(
    NMaster::TBootstrap* bootstrap,
    TNodeTrackerConfigPtr config)
{
    return New<TNodeTrackerService>(bootstrap, std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodes
} // namespace NServer
} // namespace NYP

