#include "data_node_nbd_service.h"

#include "bootstrap.h"
#include "config.h"
// TODO: remove me.
#include "session.h"
#include "nbd_session.h"
#include "session_manager.h"

#include <yt/yt/server/lib/nbd/chunk_block_device.h>

#include <yt/yt/ytlib/chunk_client/data_node_nbd_service_proxy.h>

#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/ytlib/chunk_client/proto/data_node_nbd_service.pb.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NDataNode {

using namespace NChunkClient;
using namespace NLogging;
using namespace NObjectClient;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TDataNodeNbdService
    : public TServiceBase
{
public:
    TDataNodeNbdService(
        IBootstrap* bootstrap,
        TLogger logger)
        : TServiceBase(
            bootstrap->GetStorageLightInvoker(),
            TDataNodeNbdServiceProxy::GetDescriptor(),
            logger,
            TServiceOptions{
                .Authenticator = bootstrap->GetNativeAuthenticator(),
            })
        , Bootstrap_(bootstrap)
        , Logger(std::move(logger))
    {
        YT_VERIFY(Bootstrap_);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(OpenSession)
            .SetQueueSizeLimit(50)
            .SetConcurrencyLimit(5));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CloseSession)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Read)
            .SetQueueSizeLimit(500)
            .SetConcurrencyLimit(50)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Write)
            .SetQueueSizeLimit(500)
            .SetConcurrencyLimit(50)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(KeepSessionAlive));
    }

private:
    IBootstrap* const Bootstrap_;
    TLogger Logger;

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NNbd::NProto, OpenSession)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());
        auto size = FromProto<i64>(request->size());
        auto fsType = FromProto<NYT::NNbd::EFilesystemType>(request->fs_type());

        context->SetRequestInfo("SessionId: %v, Size: %v, FsType: %v",
            sessionId,
            size,
            fsType);

        if (TypeFromId(sessionId.ChunkId) != EObjectType::NbdChunk) {
            THROW_ERROR_EXCEPTION("Invalid chunk type in session id")
                << TErrorAttribute("chunk_id", sessionId.ChunkId)
                << TErrorAttribute("expected_chunk_type", EObjectType::NbdChunk)
                << TErrorAttribute("actual_chunk_type", TypeFromId(sessionId.ChunkId));
        }

        const auto& sessionManager = Bootstrap_->GetSessionManager();

        TSessionOptions options;
        options.WorkloadDescriptor.Category = EWorkloadCategory::UserInteractive;
        options.NbdChunkSize = size;
        options.MinLocationAvailableSpace = size;
        options.NbdChunkFsType = fsType;
        auto session = sessionManager->StartSession(sessionId, options);
        context->ReplyFrom(session->Start());
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NNbd::NProto, CloseSession)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());

        context->SetRequestInfo("SessionId: %v",
            sessionId);

        auto session = GetSessionOrThrow(sessionId);

        // Destroy removes session from session manager.
        context->ReplyFrom(session->Destroy());
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NNbd::NProto, Read)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());
        auto offset = FromProto<i64>(request->offset());
        auto length = FromProto<i64>(request->length());

        context->SetRequestInfo("SessionId: %v, Offset: %v, Length: %v",
            sessionId,
            offset,
            length);

        auto session = GetSessionOrThrow(sessionId);
        auto future = session->Read(offset, length).Apply(BIND([response] (const TBlock& block) {
            SetRpcAttachedBlocks(response, {block});
        })
        .AsyncVia(Bootstrap_->GetStorageLightInvoker()));

        response->set_close_session(session->GetStoreLocation()->IsSick());
        context->ReplyFrom(future);
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NNbd::NProto, Write)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());
        auto offset = FromProto<i64>(request->offset());
        auto blocks = GetRpcAttachedBlocks(request, false);

        YT_VERIFY(blocks.size() == 1);

        context->SetRequestInfo("SessionId: %v, Offset: %v, Length: %v",
            sessionId,
            offset,
            blocks[0].Size());

        auto session = GetSessionOrThrow(sessionId);
        auto future = session->Write(offset, blocks[0]);

        response->set_close_session(session->GetStoreLocation()->IsSick());
        context->ReplyFrom(future.AsVoid());
    }

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NNbd::NProto, KeepSessionAlive)
    {
        auto sessionId = FromProto<TSessionId>(request->session_id());

        context->SetRequestInfo("SessionId: %v",
            sessionId);

        auto session = GetSessionOrThrow(sessionId);
        session->Ping();

        response->set_close_session(session->GetStoreLocation()->IsSick());
        context->Reply();
    }

    TNbdSessionPtr GetSessionOrThrow(const TSessionId& sessionId)
    {
        return DynamicPointerCast<TNbdSession>(
            Bootstrap_->GetSessionManager()->GetSessionOrThrow(sessionId.ChunkId));
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateDataNodeNbdService(
    IBootstrap* bootstrap,
    TLogger logger)
{
    return New<TDataNodeNbdService>(
        bootstrap,
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
