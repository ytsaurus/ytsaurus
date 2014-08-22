#include "query_service.h"
#include "private.h"
#include "public.h"

#include <core/concurrency/scheduler.h>

#include <core/rpc/service_detail.h>

#include <core/compression/public.h>
#include <core/compression/helpers.h>

#include <ytlib/new_table_client/schemaful_writer.h>

#include <ytlib/query_client/plan_fragment.h>
#include <ytlib/query_client/executor.h>
#include <ytlib/query_client/query_service_proxy.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/tablet_client/wire_protocol.h>

#include <server/query_agent/config.h>

#include <server/data_node/public.h>

namespace NYT {
namespace NQueryAgent {

using namespace NConcurrency;
using namespace NRpc;
using namespace NQueryClient;
using namespace NVersionedTableClient;
using namespace NTabletClient;
using namespace NCellNode;

////////////////////////////////////////////////////////////////////////////////

class TQueryService
    : public TServiceBase
{
public:
    TQueryService(
        TQueryAgentConfigPtr config,
        IInvokerPtr invoker,
        IExecutorPtr executor)
        : TServiceBase(
            CreatePrioritizedInvoker(invoker),
            TQueryServiceProxy::GetServiceName(),
            QueryAgentLogger)
        , Config_(config)
        , Executor_(executor)
    {
        YCHECK(Executor_);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute)
            .SetEnableReorder(true));
    }

private:
    TQueryAgentConfigPtr Config_;
    IExecutorPtr Executor_;


    DECLARE_RPC_SERVICE_METHOD(NQueryClient::NProto, Execute)
    {
        auto planFragment = FromProto(request->plan_fragment());
        planFragment.GetContext()->GetNodeDirectory()->MergeFrom(request->node_directory());

        context->SetRequestInfo("FragmentId: %v", planFragment.Id());

        for (int retryIndex = 0; retryIndex < Config_->MaxQueryRetries; ++retryIndex) {
            TWireProtocolWriter protocolWriter;
            auto rowsetWriter = protocolWriter.CreateSchemafulRowsetWriter();

            auto result = WaitFor(Executor_->Execute(planFragment, rowsetWriter));
            if (!result.IsOK()) {
                if (IsRetriableError(result)) {
                    LOG_INFO(result, "Query execution failed, retrying");
                    continue;
                }
                THROW_ERROR result;
            }

            response->Attachments() = NCompression::CompressWithEnvelope(
                protocolWriter.Flush(),
                Config_->SelectResponseCodec);
            ToProto(response->mutable_query_statistics(), result.Value());

            context->Reply();
            break;
        }
    }

    static bool IsRetriableError(const TError& error)
    {
        if (error.FindMatching(NDataNode::EErrorCode::LocalChunkReaderFailed)) {
            return true;
        }
        return false;
    }

};

IServicePtr CreateQueryService(
    TQueryAgentConfigPtr config,
    IInvokerPtr invoker,
    IExecutorPtr executor)
{
    return New<TQueryService>(
        config,
        invoker,
        executor);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT

