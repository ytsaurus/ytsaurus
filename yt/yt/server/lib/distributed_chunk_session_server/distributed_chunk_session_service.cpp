#include "distributed_chunk_session_service.h"

#include "distributed_chunk_session_manager.h"
#include "distributed_chunk_session_sequencer.h"
#include "private.h"

#include <yt/yt/ytlib/distributed_chunk_session_client/distributed_chunk_session_service_proxy.h>

#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/client/api/config.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NDistributedChunkSessionServer {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NDistributedChunkSessionClient;
using namespace NNodeTrackerClient;
using namespace NRpc;
using namespace NYTree;
using namespace NYson;

using NTableClient::NProto::TDataBlockMeta;
using NApi::NNative::IConnectionPtr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDistributedChunkSessionService
    : public TServiceBase
{
public:
    TDistributedChunkSessionService(
        IInvokerPtr invoker,
        IConnectionPtr connection)
        : TServiceBase(
            invoker,
            TDistributedChunkSessionServiceProxy::GetDescriptor(),
            DistributedChunkSessionServiceLogger())
        , DistributedChunkSessionManager_(
            CreateDistributedChunkSessionManager(
                std::move(invoker),
                std::move(connection)))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartSession));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingSession));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(WriteRecord)
            .SetHeavy(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(FinishSession));
    }

private:
    const IDistributedChunkSessionManagerPtr DistributedChunkSessionManager_;

    DECLARE_RPC_SERVICE_METHOD(NDistributedChunkSessionClient::NProto, StartSession)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());
        auto targets = FromProto<TChunkReplicaWithMediumList>(request->chunk_replicas());

        auto options = ConvertTo<TJournalChunkWriterOptionsPtr>(TYsonStringBuf(request->journal_chunk_writer_options()));
        auto config = ConvertTo<TJournalChunkWriterConfigPtr>(TYsonStringBuf(request->journal_chunk_writer_config()));

        auto sessionTimeout = FromProto<TDuration>(request->session_timeout());

        context->SetRequestInfo(
            "SessionId: %v, Targets: %v, SessionTimeout: %v",
            sessionId,
            targets,
            sessionTimeout);

        auto asyncResult = DistributedChunkSessionManager_->StartSession(
            sessionId,
            sessionTimeout,
            std::move(targets),
            std::move(options),
            std::move(config));

        context->ReplyFrom(std::move(asyncResult));
    }

    DECLARE_RPC_SERVICE_METHOD(NDistributedChunkSessionClient::NProto, PingSession)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());

        context->SetRequestInfo("SessionId: %v", sessionId);

        DistributedChunkSessionManager_->RenewSessionLease(sessionId);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NDistributedChunkSessionClient::NProto, WriteRecord)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());

        THROW_ERROR_EXCEPTION_IF(
            request->Attachments().size() != 1,
            "Invalid attachments size: expected 1, got %v",
            request->Attachments().size());

        context->SetRequestInfo(
            "SessionId: %v",
            sessionId);

        auto sequencer = DistributedChunkSessionManager_->GetSequencerOrThrow(sessionId);

        context->ReplyFrom(sequencer->WriteRecord(request->Attachments()[0]));
    }

    DECLARE_RPC_SERVICE_METHOD(NDistributedChunkSessionClient::NProto, FinishSession)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());

        context->SetRequestInfo(
            "SessionId: %v",
            sessionId);

        auto sequencer = DistributedChunkSessionManager_->GetSequencerOrThrow(sessionId);

        context->ReplyFrom(sequencer->Close().ToUncancelable());
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

IServicePtr CreateDistributedChunkSessionService(
    IInvokerPtr invoker,
    IConnectionPtr connection)
{
    return New<TDistributedChunkSessionService>(
        std::move(invoker),
        std::move(connection));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionServer
