#include "push_based_shuffle_service.h"

#include "private.h"
#include "push_based_shuffle_registry.h"

#include <yt/yt/server/lib/controller_agent/push_based_shuffle_service_proxy.h>

#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/ytlib/distributed_chunk_session_client/session_pool.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NControllerAgent {

using namespace NDistributedChunkSessionClient;
using namespace NRpc;

using NChunkClient::TSessionId;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TPushBasedShuffleService
    : public TServiceBase
{
public:
    TPushBasedShuffleService(
        TPushBasedShuffleRegistryPtr registry,
        IInvokerPtr invoker,
        IAuthenticatorPtr authenticator)
        : TServiceBase(
            invoker,
            TPushBasedShuffleServiceProxy::GetDescriptor(),
            ControllerAgentLogger(),
            TServiceOptions{
                .Authenticator = std::move(authenticator),
            })
        , Registry_(std::move(registry))
        , Invoker_(std::move(invoker))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetShuffleWriteSession));
    }

private:
    const TPushBasedShuffleRegistryPtr Registry_;
    const IInvokerPtr Invoker_;

    DECLARE_RPC_SERVICE_METHOD(NProto, GetShuffleWriteSession)
    {
        auto incarnationId = FromProto<TIncarnationId>(request->controller_agent_incarnation_id());
        auto operationId = FromProto<TOperationId>(request->operation_id());
        auto jobId = FromProto<TJobId>(request->job_id());
        int partitionIndex = request->partition_index();
        auto excludedSessionId = YT_OPTIONAL_FROM_PROTO(*request, excluded_session_id, TSessionId);

        context->SetRequestInfo("IncarnationId: %v, OperationId: %v, JobId: %v, PartitionIndex: %v, ExcludedSessionId: %v",
            incarnationId,
            operationId,
            jobId,
            partitionIndex,
            excludedSessionId);

        auto pool = Registry_->GetShufflePoolOrThrow(incarnationId, operationId);

        context->ReplyFrom(pool->GetSession(partitionIndex, excludedSessionId)
            .Apply(BIND([context] (const TErrorOr<TSessionDescriptor>& sessionOrError) {
                const auto& session = sessionOrError.ValueOrThrow();
                ToProto(context->Response().mutable_session_id(), session.SessionId);
                ToProto(context->Response().mutable_sequencer_node(), session.SequencerNode);
                context->SetResponseInfo("SessionId: %v, SequencerNode: %v",
                    session.SessionId,
                    session.SequencerNode);
            })
                .AsyncVia(Invoker_)));
    }
};

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreatePushBasedShuffleService(
    TPushBasedShuffleRegistryPtr registry,
    IInvokerPtr invoker,
    IAuthenticatorPtr authenticator)
{
    return New<TPushBasedShuffleService>(
        std::move(registry),
        std::move(invoker),
        std::move(authenticator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
