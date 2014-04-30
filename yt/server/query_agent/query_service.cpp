#include "query_service.h"
#include "private.h"

#include <core/concurrency/scheduler.h>

#include <core/rpc/service_detail.h>

#include <ytlib/new_table_client/schemaful_writer.h>

#include <ytlib/query_client/plan_fragment.h>
#include <ytlib/query_client/executor.h>
#include <ytlib/query_client/query_service_proxy.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/tablet_client/wire_protocol.h>

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
    explicit TQueryService(
        IInvokerPtr invoker,
        IExecutorPtr executor)
        : TServiceBase(
            CreatePrioritizedInvoker(invoker),
            TQueryServiceProxy::GetServiceName(),
            QueryAgentLogger.GetCategory())
        , Executor_(executor)
    {
        YCHECK(Executor_);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute)
            .SetEnableReorder(true));
    }

private:
    IExecutorPtr Executor_;


    DECLARE_RPC_SERVICE_METHOD(NQueryClient::NProto, Execute)
    {
        context->SetRequestInfo("");
        
        auto planFragment = FromProto(request->plan_fragment());
        planFragment.GetContext()->GetNodeDirectory()->MergeFrom(request->node_directory());

        TWireProtocolWriter protocolWriter;
        auto rowsetWriter = protocolWriter.CreateSchemafulRowsetWriter();

        auto result = WaitFor(Executor_->Execute(planFragment, rowsetWriter));
        THROW_ERROR_EXCEPTION_IF_FAILED(result);

        response->set_encoded_response(protocolWriter.GetData());
        context->Reply();
    }

};

IServicePtr CreateQueryService(
    IInvokerPtr invoker,
    IExecutorPtr executor)
{
    return New<TQueryService>(
        invoker,
        executor);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT

