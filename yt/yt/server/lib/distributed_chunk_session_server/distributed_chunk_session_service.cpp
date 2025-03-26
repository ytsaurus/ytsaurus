#include "distributed_chunk_session_service.h"

#include "distributed_chunk_session_coordinator.h"
#include "distributed_chunk_session_manager.h"
#include "private.h"

#include <yt/yt/ytlib/distributed_chunk_session_client/distributed_chunk_session_service_proxy.h>

#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NDistributedChunkSessionServer {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NDistributedChunkSessionClient;
using namespace NNodeTrackerClient;
using namespace NRpc;

using NTableClient::NProto::TDataBlockMeta;
using NApi::NNative::IConnectionPtr;

////////////////////////////////////////////////////////////////////////////////

class TDistributedChunkSessionService
    : public TServiceBase
{
public:
    TDistributedChunkSessionService(
        TDistributedChunkSessionServiceConfigPtr config,
        IInvokerPtr invoker,
        IConnectionPtr connection)
        : TServiceBase(
            invoker,
            TDistributedChunkSessionServiceProxy::GetDescriptor(),
            DistributedChunkSessionServiceLogger())
        , DistributedChunkSessionManager_(
            CreateDistributedChunkSessionManager(
                std::move(config),
                std::move(invoker),
                std::move(connection)))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartSession));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingSession));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SendBlocks)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FinishSession));
    }

private:
    const IDistributedChunkSessionManagerPtr DistributedChunkSessionManager_;

    DECLARE_RPC_SERVICE_METHOD(NDistributedChunkSessionClient::NProto, StartSession)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());
        auto targets = FromProto<std::vector<TNodeDescriptor>>(request->chunk_replicas());

        context->SetRequestInfo(
            "SessionId: %v, Targets: %v",
            sessionId,
            targets);

        DistributedChunkSessionManager_->StartSession(sessionId, std::move(targets));

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NDistributedChunkSessionClient::NProto, PingSession)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());

        context->SetRequestInfo(
            "SessionId: %v, AcknowledgedBlockCount: %v",
            sessionId,
            request->acknowledged_block_count());

        DistributedChunkSessionManager_->RenewSessionLease(sessionId);
        auto coordinator = DistributedChunkSessionManager_->GetCoordinatorOrThrow(sessionId);

        auto status = WaitFor(coordinator->UpdateStatus(request->acknowledged_block_count()))
            .ValueOrThrow();

        ToProto(response, status);

        context->SetResponseInfo(
            "CloseDemanded: %v, "
            "WrittenBlockCount: %v",
            status.CloseDemanded,
            status.WrittenBlockCount);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NDistributedChunkSessionClient::NProto, SendBlocks)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());

        context->SetRequestInfo(
            "SessionId: %v",
            sessionId);

        std::vector<TBlock> blocks;
        blocks.reserve(request->Attachments().size());
        for (int index = 0; index < std::ssize(request->Attachments()); ++index) {
            blocks.emplace_back(request->Attachments()[index]);
        }

        auto session = DistributedChunkSessionManager_->GetCoordinatorOrThrow(sessionId);

        context->ReplyFrom(session->SendBlocks(
            std::move(blocks),
            FromProto<std::vector<TDataBlockMeta>>(request->data_block_metas()),
            request->blocks_misc_meta()));
    }

    DECLARE_RPC_SERVICE_METHOD(NDistributedChunkSessionClient::NProto, FinishSession)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());

        context->SetRequestInfo(
            "SessionId: %v",
            sessionId);

        auto session = DistributedChunkSessionManager_->GetCoordinatorOrThrow(sessionId);

        context->ReplyFrom(session->Close(/*force*/ true));
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateDistributedChunkSessionService(
    TDistributedChunkSessionServiceConfigPtr config,
    IInvokerPtr invoker,
    IConnectionPtr connection)
{
    return New<TDistributedChunkSessionService>(
        std::move(config),
        std::move(invoker),
        std::move(connection));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionServer
