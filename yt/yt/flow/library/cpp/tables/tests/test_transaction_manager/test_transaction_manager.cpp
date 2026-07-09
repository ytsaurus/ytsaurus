#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/tables/context.h>
#include <yt/yt/flow/library/cpp/tables/transaction_manager.h>

#include <yt/yt/flow/library/cpp/misc/load_throughput_throttler.h>
#include <yt/yt/flow/library/cpp/misc/retryable_transaction.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/misc/guid.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/cache/rpc.h>

namespace NYT::NFlow {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NLogging;
using namespace NObjectClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "Test");

////////////////////////////////////////////////////////////////////////////////

class TTestTransactionManager
    : public ::testing::Test
{
protected:
    NApi::IClientPtr Client_;

    void SetUp() override
    {
        // Connect through the RPC proxy using YT_PROXY (exported by the Flow recipe). Going
        // through the proxy means the pipeline node type handler that bootstraps a pipeline's
        // child tables runs from the package binaries, not from the (possibly older) code linked
        // into this test binary.
        Client_ = NClient::NCache::CreateClient();

        LeaseTransaction_ = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Master))
            .ValueOrThrow();

        WaitFor(Client_->CreateNode(GetPipelinePath(), EObjectType::Pipeline))
            .ThrowOnError();
        // Wait for any table to be mounted — partition_transactions is created by the pipeline.
        WaitUntilTableMounted(YPathJoin(GetPipelinePath(), "input_messages"));
    }

    void TearDown() override
    {
        auto tryRemove = [&] {
            return WaitFor(Client_->RemoveNode(GetPipelinePath())).IsOK();
        };
        WaitForPredicate(tryRemove, /*iterationCount*/ 100, /*period*/ TDuration::MilliSeconds(200));
        if (LeaseTransaction_) {
            WaitFor(LeaseTransaction_->Abort()).ThrowOnError();
            LeaseTransaction_.Reset();
        }
    }

    static TYPath GetPipelinePath()
    {
        return "//pipeline_transaction_manager";
    }

    void WaitUntilTableMounted(const TYPath& tablePath)
    {
        WaitForPredicate(
            [&] {
                auto state = WaitFor(Client_->GetNode(tablePath + "/@tablet_state")).ValueOrThrow();
                return NYTree::ConvertTo<std::string>(state) == "mounted";
            },
            /*iterationCount*/ 100,
            /*period*/ TDuration::MilliSeconds(500));
    }

    NTables::TTransactionManagerContextPtr PrepareTransactionManagerContext()
    {
        auto context = New<NTables::TTransactionManagerContext>();
        context->Client = Client_;
        context->PipelinePath = GetPipelinePath();
        context->LoadThroughputThrottler = New<TLoadThroughputThrottler>(
            CreateNamedUnlimitedThroughputThrottler("test", NProfiling::TProfiler()),
            Logger(),
            NProfiling::TProfiler());
        context->Logger = Logger();
        context->Profiler = NProfiling::TProfiler();
        context->LeaseId = LeaseTransaction_->GetId();
        context->PartitionId = TPartitionId(TGuid::Create());
        context->StatusProfiler = CreateStatusProfiler();
        return context;
    }

    ITransactionPtr LeaseTransaction_;
};

////////////////////////////////////////////////////////////////////////////////

// Verifies that CreateTransaction() returns a non-null retryable transaction
// and CommitTransaction() succeeds on an empty transaction.
TEST_W(TTestTransactionManager, CreateAndCommitTransaction)
{
    auto context = PrepareTransactionManagerContext();
    auto spec = New<TDynamicRetryableRequestSpec>();
    auto manager = New<NTables::TTransactionManager>(context, spec);

    auto tx = manager->CreateTransaction();
    ASSERT_TRUE(tx != nullptr);

    // Commit an empty transaction — should succeed.
    WaitFor(manager->CommitTransaction(tx)).ThrowOnError();
}

// Verifies that Cleanup() completes without error.
TEST_W(TTestTransactionManager, Cleanup)
{
    auto context = PrepareTransactionManagerContext();
    auto spec = New<TDynamicRetryableRequestSpec>();
    auto manager = New<NTables::TTransactionManager>(context, spec);

    WaitFor(manager->Cleanup()).ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
