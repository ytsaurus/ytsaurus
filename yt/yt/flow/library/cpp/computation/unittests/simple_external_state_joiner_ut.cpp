#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/state.h>
#include <yt/yt/flow/library/cpp/common/state_cache.h>

#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/cache/cache.h>

#include <yt/yt/client/unittests/mock/client.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NFlow {
namespace {

using namespace NConcurrency;
using namespace NTableClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TStubClientsCache
    : public NClient::NCache::IClientsCache
{
public:
    NApi::IClientPtr GetClient(TStringBuf /*clusterUrl*/) override
    {
        return New<NApi::TMockClient>();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSimpleExternalStateJoinerTest
    : public ::testing::Test
{
protected:
    const TActionQueuePtr Queue_ = New<TActionQueue>("SimpleExternalStateJoinerTest");

    //! The joiner caches through TExpiringJobNamedStateCache, which must be built on a serialized
    //! invoker, so construction runs on the queue rather than on the test thread.
    TSimpleExternalStateJoinerPtr MakeJoiner()
    {
        return WaitFor(BIND(&TSimpleExternalStateJoinerTest::DoMakeJoiner, this)
                .AsyncVia(Queue_->GetInvoker())
                .Run())
            .ValueOrThrow();
    }

    TSimpleExternalStateJoinerPtr DoMakeJoiner()
    {
        auto context = New<TExternalStateJoinerContext>();
        context->KeySchema = New<TTableSchema>(std::vector<TColumnSchema>{
            TColumnSchema("hash", EValueType::Uint64, ESortOrder::Ascending),
            TColumnSchema("key", EValueType::String, ESortOrder::Ascending),
        });
        context->ClientsCache = New<TStubClientsCache>();
        context->SerializedInvoker = Queue_->GetInvoker();
        context->StatusProfiler = CreateSyncStatusProfiler();
        context->PipelinePath = NYPath::TRichYPath("//pipeline");
        context->PipelinePath.SetCluster("test");
        context->Logger = NLogging::TLogger("Test");
        context->StateCache = New<TStateCache>(New<TDynamicStateCacheSpec>(), NProfiling::TProfiler{})
            ->WithJob(TJobId(TGuid::Create()), NProfiling::TProfiler{})
            ->WithName("joiner");

        auto spec = New<TExternalStateJoinerSpec>();
        spec->ExternalStateJoinerClassName = "NYT::NFlow::TSimpleExternalStateJoiner";
        spec->JoinOn = New<TStateJoinSpec>();
        spec->Parameters = BuildYsonNodeFluently()
            .BeginMap()
            .Item("path")
            .Value("<cluster=test>//table")
            .EndMap()
            ->AsMap();
        context->ExternalStateJoinerSpec = std::move(spec);

        auto dynamicContext = New<TDynamicExternalStateJoinerContext>();
        dynamicContext->DynamicExternalStateJoinerSpec = New<TDynamicExternalStateJoinerSpec>();

        return New<TSimpleExternalStateJoiner>(std::move(context), std::move(dynamicContext));
    }
};

////////////////////////////////////////////////////////////////////////////////

// A key that was never preloaded used to abort the process, so a computation asking for the wrong
// key — a spec with a key schema override was enough — took down every job on the worker.
TEST_F(TSimpleExternalStateJoinerTest, GetStateThrowsForNonPreloadedKey)
{
    auto joiner = MakeJoiner();

    EXPECT_THROW_WITH_SUBSTRING(
        joiner->GetState(MakeKey(1ul, "absent")),
        "has no preloaded state for key");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
