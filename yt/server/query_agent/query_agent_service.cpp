#include "query_agent_service.h"
#include "private.h"
#include "config.h"

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NQueryAgent {

using namespace NRpc;
using namespace NQueryClient;
using namespace NQueryClient::NProto;
using namespace NCellNode;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = QueryAgentLogger;
static auto& Profiler = QueryAgentProfiler;
static auto ProfilingPeriod = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

TQueryAgentService::TQueryAgentService(
    TQueryAgentConfigPtr config,
    TBootstrap* bootstrap)
    : NRpc::TServiceBase(
        CreatePrioritizedInvoker(bootstrap->GetControlInvoker()),
        TProxy::GetServiceName(),
        QueryAgentLogger.GetCategory())
    , Config(config)
    , Bootstrap(bootstrap)
    , WorkerPool(New<TThreadPool>(Config->WorkerPoolSize, "QueryAgentWorker"))
{
    YCHECK(config);
    YCHECK(bootstrap);

    RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute));
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TQueryAgentService, Execute)
{
    UNUSED(response);

    context->SetRequestInfo("UNIMPLEMETED");

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT

