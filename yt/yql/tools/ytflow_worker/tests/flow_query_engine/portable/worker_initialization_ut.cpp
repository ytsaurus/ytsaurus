#include <yt/yt/flow/library/cpp/runner/init.h>
#include <yt/yt/flow/library/cpp/runner/public.h>
#include <yt/yt/flow/library/cpp/runner/root_clients_cache.h>

#include <yt/yt/flow/library/cpp/worker/job_tracker.h>

#include <yt/yt/flow/library/cpp/common/column_evaluator_cache.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/job_directory.h>
#include <yt/yt/flow/library/cpp/common/payload_converter.h>

#include <yt/yt/flow/library/cpp/misc/node_info.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/flow/library/cpp/misc/testing/env_guard.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/options.h>

#include <yt/yt/client/cache/cache.h>
#include <yt/yt/client/cache/config.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/current_invoker.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <util/system/env.h>

namespace NYT::NFlow {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TEST(TRunnerLinkSmokeTest, InitializesWithoutStartingFlowNode)
{
    NTesting::TEnvGuard flowModeGuard(FlowModeEnvVarName);
    NTesting::TEnvGuard resolverGuard("GRPC_DNS_RESOLVER");
    const char* argv[] = {"flow_query_engine_test"};

    Initialize(/*argc*/ 1, argv);

    EXPECT_EQ("native", GetEnv("GRPC_DNS_RESOLVER"));
}

class TWorkerInitializationTest
    : public ::testing::Test
{ };

TEST_W(TWorkerInitializationTest, CreatesRpcClientAndIdleJobTrackerWithoutCluster)
{
    auto invoker = MakeStrong(GetCurrentInvoker());
    auto clientsCacheConfig = New<NClient::NCache::TClientsCacheConfig>();
    clientsCacheConfig->DefaultConnection->EnableProxyDiscovery = false;
    clientsCacheConfig->DefaultConnection->ProxyAddresses = std::vector<std::string>{"127.0.0.1:1"};

    NYPath::TRichYPath pipelinePath("//tmp/pipeline");
    pipelinePath.SetCluster("offline");

    auto clientsCache = CreateRootClientsCache({
        .PipelinePath = pipelinePath,
        .ClientsCacheConfig = clientsCacheConfig,
        .ClientOptions = NApi::TClientOptions::FromUser("test-user"),
    });
    auto client = clientsCache->GetClient("offline");
    ASSERT_TRUE(client);

    auto converterCache = CreatePayloadConverterCache(CreateFastColumnEvaluatorCache());
    auto context = New<NWorker::TJobTrackerContext>();
    context->WorkerNodeInfo = New<TNodeInfo>();
    context->WorkerNodeInfo->VcpuFactor = 1;
    context->WorkerNodeInfo->VcpuLimit = 1000;
    context->ClientsCache = clientsCache;
    context->PipelinePath = pipelinePath;
    context->ControlInvoker = invoker;
    context->JobDirectory = CreateJobDirectory(converterCache, NLogging::TLogger());
    context->ClockClusterTag = NObjectClient::InvalidCellTag;
    context->StatusProfiler = CreateSyncStatusProfiler();

    EXPECT_EQ(client, context->GetClient());

    auto tracker = NWorker::CreateJobTracker(context);
    auto statuses = WaitFor(tracker->GetStatuses())
        .ValueOrThrow();

    EXPECT_TRUE(statuses.empty());
    EXPECT_TRUE(tracker->GetResourceStatuses().empty());
    EXPECT_TRUE(tracker->GetPreloadedStates().empty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
