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

#include <yt/client/tablet_client/public.h>

#include <yt/core/rpc/grpc/proto/grpc.pb.h>

namespace NYP::NServer::NNodes {

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
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetHostSpec));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SetHostStatus));
    }

private:
    const TNodeTrackerConfigPtr Config_;

    DECLARE_RPC_SERVICE_METHOD(NClient::NNodes::NProto, Handshake)
    {
        const auto& nodeId = request->node_id();
        const auto& address = request->address();
        // COMPAT(babenko): make required
        const auto& version = request->has_version() ? request->version() : TString("unknown");

        context->SetRequestInfo("NodeId: %v, Address: %v, Version: %v",
            nodeId,
            address,
            version);

        ValidateCallerCertificate(context, nodeId);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto transaction = WaitFor(transactionManager->StartReadWriteTransaction())
            .ValueOrThrow();

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->ProcessHandshake(
            transaction,
            nodeId,
            address,
            version);

        WaitFor(transaction->Commit())
            .ThrowOnError();

        auto epochId = node->Status().EpochId().Load();

        ToProto(response->mutable_epoch_id(), epochId);

        context->SetResponseInfo("NodeId: %v, EpochId: %v",
            nodeId,
            epochId);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NClient::NNodes::NProto, Heartbeat)
    {
        const auto& nodeId = request->node_id();
        auto epochId = FromProto<TEpochId>(request->epoch_id());
        auto sequenceNumber = request->sequence_number();

        context->SetRequestInfo("NodeId: %v, EpochId: %v, SequenceNumber: %v",
            nodeId,
            epochId,
            sequenceNumber);

        ValidateCallerCertificate(context, nodeId);

        auto processAttempt = [&] {
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
        };

        int attempt = 1;
        while (true) {
            try {
                processAttempt();
                break;
            } catch (const TErrorException& ex) {
                const auto& error = ex.Error();
                if (attempt < Config_->ProcessHeartbeatAttemptCount &&
                    error.FindMatching(NTabletClient::EErrorCode::TransactionLockConflict))
                {
                    YT_LOG_DEBUG(error, "Heartbeat processing failed, retrying (Attempt: %v)",
                        attempt);
                    ++attempt;
                    response->Clear();
                    continue;
                } else {
                    throw;
                }
            }
        }

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NClient::NNodes::NProto, GetHostSpec)
    {
        const auto& nodeId = request->node_id();

        context->SetRequestInfo("NodeId: %v",
            nodeId);

        ValidateCallerCertificate(context, nodeId);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto transaction = WaitFor(transactionManager->StartReadOnlyTransaction())
            .ValueOrThrow();

        auto* node = transaction->GetNode(nodeId);
        const auto& spec = node->Spec().Load().host_manager();
        *response->mutable_spec() = spec;

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NClient::NNodes::NProto, SetHostStatus)
    {
        const auto& nodeId = request->node_id();

        context->SetRequestInfo("NodeId: %v",
            nodeId);

        ValidateCallerCertificate(context, nodeId);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        auto transaction = WaitFor(transactionManager->StartReadWriteTransaction())
            .ValueOrThrow();

        auto* node = transaction->GetNode(nodeId);
        node->Status().HostManager() = request->status();

        WaitFor(transaction->Commit())
            .ThrowOnError();

        Y_UNUSED(response);
        context->Reply();
    }


    void ValidateCallerCertificate(const NRpc::IServiceContextPtr& context, const TObjectId& nodeId)
    {
        if (!Config_->ValidateCallerCertificate) {
            return;
        }

        const auto& ext = context->GetRequestHeader().GetExtension(NYT::NRpc::NGrpc::NProto::TSslCredentialsExt::ssl_credentials_ext);

        if (!ext.has_peer_identity()) {
            THROW_ERROR_EXCEPTION("Caller %Qv did not provide SSL credentials",
                nodeId);
        }
        if (ext.peer_identity() != nodeId) {
            THROW_ERROR_EXCEPTION("Caller %Qv has provided a wrong SSL identity %Qv",
                nodeId,
                ext.peer_identity());
        }

        if (Config_->CallerCertificateIssuer) {
            if (!ext.has_issuer()) {
                THROW_ERROR_EXCEPTION("Unable to determine an SSL certificate issuer for node %Qv",
                    nodeId);
            }
            if (ext.issuer() != *Config_->CallerCertificateIssuer) {
                THROW_ERROR_EXCEPTION("Caller %Qv has provided an SSL certificate with a wrong issuer: expected %Qv, found %Qv",
                    nodeId,
                    *Config_->CallerCertificateIssuer,
                    ext.issuer());
            }
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

} // namespace NYP::NServer::NNodes

