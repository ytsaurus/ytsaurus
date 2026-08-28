#include <yt/yt/flow/library/cpp/native_client/pipeline_init.h>

#include <yt/yt/client/unittests/mock/client.h>
#include <yt/yt/client/unittests/mock/transaction.h>

#include <yt/yt/client/cypress_client/public.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

using namespace NApi;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NYPath;

using ::testing::_;
using ::testing::NiceMock;
using ::testing::Return;

////////////////////////////////////////////////////////////////////////////////

TEST(TPipelineInitTest, IgnoreExistingPropagatesToInnerTables)
{
    auto client = New<NiceMock<TMockClient>>();
    auto transaction = New<NiceMock<TMockTransaction>>();

    // Captures every CreateNode call made on the master transaction during
    // pipeline materialization, so the test can assert on the options passed
    // to inner table creates without spinning up a real master.
    struct TCreateNodeCall
    {
        TYPath Path;
        EObjectType Type;
        TCreateNodeOptions Options;
    };

    std::vector<TCreateNodeCall> createNodeCalls;

    ON_CALL(*client, StartTransaction(_, _))
        .WillByDefault(Return(MakeFuture<ITransactionPtr>(transaction)));

    ON_CALL(*transaction, CreateNode(_, _, _))
        .WillByDefault([&] (
            const TYPath& path,
            EObjectType type,
            const TCreateNodeOptions& options) {
            createNodeCalls.push_back({path, type, options});
            return MakeFuture<TNodeId>(TNodeId(TGuid::Create()));
        });

    ON_CALL(*transaction, Commit(_))
        .WillByDefault(Return(MakeFuture(TTransactionCommitResult{})));

    ON_CALL(*client, MountTable(_, _))
        .WillByDefault(Return(OKFuture));

    TCreateNodeOptions options;
    options.IgnoreExisting = true;
    options.Recursive = true;

    CreatePipelineNode(client, "//tmp/pipeline", options);

    // 1 call for the pipeline map-node itself + one per inner table.
    ASSERT_GT(createNodeCalls.size(), 1u);

    bool sawPipelineNode = false;
    bool sawInnerTable = false;
    for (const auto& call : createNodeCalls) {
        EXPECT_TRUE(call.Options.IgnoreExisting)
            << "IgnoreExisting must propagate to every CreateNode call "
            << "(path=" << call.Path << ", type=" << ToString(call.Type) << ")";
        if (call.Type == EObjectType::MapNode) {
            sawPipelineNode = true;
        } else if (call.Type == EObjectType::Table) {
            sawInnerTable = true;
        }
    }
    EXPECT_TRUE(sawPipelineNode);
    EXPECT_TRUE(sawInnerTable);
}

////////////////////////////////////////////////////////////////////////////////

// The dyntable lease backend cannot run without the "leases" table, and a pipeline created by the
// native client is provisioned by #GetTables alone: yt_sync is not involved there. The Python
// catalogue drift test cannot cover this — it creates a pipeline through the master, which does
// not create that table at all — so dropping the registration would otherwise go unnoticed.
TEST(TPipelineInitTest, CreatesTheLeasesTable)
{
    auto client = New<NiceMock<TMockClient>>();
    auto transaction = New<NiceMock<TMockTransaction>>();

    std::vector<TYPath> createdPaths;

    ON_CALL(*client, StartTransaction(_, _))
        .WillByDefault(Return(MakeFuture<ITransactionPtr>(transaction)));

    ON_CALL(*transaction, CreateNode(_, _, _))
        .WillByDefault([&] (
            const TYPath& path,
            EObjectType /*type*/,
            const TCreateNodeOptions& /*options*/) {
            createdPaths.push_back(path);
            return MakeFuture<TNodeId>(TNodeId(TGuid::Create()));
        });

    ON_CALL(*transaction, Commit(_))
        .WillByDefault(Return(MakeFuture(TTransactionCommitResult{})));

    ON_CALL(*client, MountTable(_, _))
        .WillByDefault(Return(OKFuture));

    CreatePipelineNode(client, "//tmp/pipeline", TCreateNodeOptions());

    EXPECT_THAT(createdPaths, ::testing::Contains("//tmp/pipeline/leases"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
