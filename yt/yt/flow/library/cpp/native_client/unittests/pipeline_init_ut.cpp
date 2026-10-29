#include <yt/yt/flow/library/cpp/native_client/pipeline_init.h>
#include <yt/yt/flow/library/cpp/pipeline_tables/public.h>

#include <yt/yt/client/unittests/mock/client.h>
#include <yt/yt/client/unittests/mock/transaction.h>

#include <yt/yt/client/cypress_client/public.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <yt/yt/server/lib/chaos_election/election_manager.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

using namespace NApi;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NYPath;
using namespace NYTree;

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

    // One call for the pipeline map-node, one for the logs directory, and one per inner table.
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

TEST(TPipelineInitTest, UsesCanonicalSchemas)
{
    auto client = New<NiceMock<TMockClient>>();
    auto transaction = New<NiceMock<TMockTransaction>>();

    THashMap<TYPath, TCreateNodeOptions> createNodeCalls;
    THashMap<TYPath, EObjectType> createdTypes;
    std::vector<TYPath> mountedPaths;

    ON_CALL(*client, StartTransaction(_, _))
        .WillByDefault(Return(MakeFuture<ITransactionPtr>(transaction)));

    ON_CALL(*transaction, CreateNode(_, _, _))
        .WillByDefault([&] (
            const TYPath& path,
            EObjectType type,
            const TCreateNodeOptions& options) {
            createNodeCalls[path] = options;
            createdTypes[path] = type;
            return MakeFuture<TNodeId>(TNodeId(TGuid::Create()));
        });

    ON_CALL(*transaction, Commit(_))
        .WillByDefault(Return(MakeFuture(TTransactionCommitResult{})));

    ON_CALL(*client, MountTable(_, _))
        .WillByDefault([&] (const TYPath& path, const TMountTableOptions&) {
            mountedPaths.push_back(path);
            return OKFuture;
        });

    CreatePipelineNode(client, "//tmp/pipeline", {});

    const auto& definitions = GetPipelineTableDefinitions();
    auto checkDefinitions = [&] (const auto& section) {
        for (const auto& [name, definition] : section) {
            auto path = Format("//tmp/pipeline/%v", name);
            auto options = createNodeCalls.find(path);
            ASSERT_NE(options, createNodeCalls.end());
            EXPECT_THAT(mountedPaths, ::testing::Contains(path));
            auto actualAttributes = options->second.Attributes->Clone();
            auto actualSchemaYson = actualAttributes->GetYsonAndRemove("schema");
            EXPECT_EQ(
                *ConvertTo<TTableSchemaPtr>(actualSchemaYson),
                *definition.Schema);
            EXPECT_TRUE(AreNodesEqual(
                ConvertTo<INodePtr>(actualSchemaYson),
                ConvertTo<INodePtr>(definition.SchemaYson)));
            EXPECT_TRUE(AreNodesEqual(
                actualAttributes->ToMap(),
                definition.Attributes->ToMap()));
        }
    };
    checkDefinitions(definitions.Tables);
    checkDefinitions(definitions.Queues);
    for (const auto& [name, definition] : definitions.Nodes) {
        auto path = BuildPipelineTablePath("//tmp/pipeline", name);
        auto type = createdTypes.find(path);
        ASSERT_NE(type, createdTypes.end());
        EXPECT_EQ(type->second, definition.Type);
        auto options = createNodeCalls.find(path);
        ASSERT_NE(options, createNodeCalls.end());
        EXPECT_TRUE(AreNodesEqual(
            options->second.Attributes->ToMap(),
            definition.Attributes->ToMap()));
    }
    auto logsOptions = createNodeCalls.find("//tmp/pipeline/logs");
    ASSERT_NE(logsOptions, createNodeCalls.end());
    EXPECT_EQ(
        logsOptions->second.Attributes->Get<int>("yt_logs_format_version"),
        1);
    auto pipelineOptions = createNodeCalls.find("//tmp/pipeline");
    ASSERT_NE(pipelineOptions, createNodeCalls.end());
    EXPECT_FALSE(pipelineOptions->second.Attributes->Contains("yt_logs_format_version"));

    const auto& definition = definitions.Tables.at("leader_election_lock");
    auto electionSchema = NChaosElection::GetChaosElectionLockTableSchema();
    EXPECT_EQ(
        *definition.Schema->ToSortedStrippedColumnAttributes(),
        *electionSchema);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TPipelineInitTest, InitializeTablesIsControlAttribute)
{
    auto client = New<NiceMock<TMockClient>>();
    auto transaction = New<NiceMock<TMockTransaction>>();

    int createNodeCallCount = 0;
    TCreateNodeOptions capturedOptions;

    ON_CALL(*client, StartTransaction(_, _))
        .WillByDefault(Return(MakeFuture<ITransactionPtr>(transaction)));

    ON_CALL(*transaction, CreateNode(_, _, _))
        .WillByDefault([&] (
            const TYPath& path,
            EObjectType type,
            const TCreateNodeOptions& options) {
            ++createNodeCallCount;
            EXPECT_EQ(path, "//tmp/pipeline");
            EXPECT_EQ(type, EObjectType::MapNode);
            capturedOptions = options;
            return MakeFuture<TNodeId>(TNodeId(TGuid::Create()));
        });

    ON_CALL(*transaction, Commit(_))
        .WillByDefault(Return(MakeFuture(TTransactionCommitResult{})));

    TCreateNodeOptions options;
    options.Attributes = NYTree::CreateEphemeralAttributes();
    options.Attributes->Set("initialize_tables", false);
    options.Attributes->Set("monitoring_cluster", "monitoring");
    options.Attributes->Set(PipelineFormatVersionAttribute, -1);

    CreatePipelineNode(client, "//tmp/pipeline", options);

    EXPECT_EQ(createNodeCallCount, 1);
    ASSERT_TRUE(capturedOptions.Attributes);
    EXPECT_FALSE(capturedOptions.Attributes->Contains("initialize_tables"));
    EXPECT_EQ(capturedOptions.Attributes->Get<std::string>("monitoring_cluster"), "monitoring");
    EXPECT_EQ(
        capturedOptions.Attributes->Get<int>(PipelineFormatVersionAttribute),
        CurrentPipelineFormatVersion);
}

////////////////////////////////////////////////////////////////////////////////

// The dyntable lease backend cannot run without the "leases" table. Keep direct coverage of the
// native path that provisions it from #GetPipelineTableDefinitions(), without yt_sync.
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

TEST(TPipelineInitTest, SkipsLogsDirectoryWhenTableInitializationIsDisabled)
{
    auto client = New<NiceMock<TMockClient>>();
    auto transaction = New<NiceMock<TMockTransaction>>();

    std::vector<TYPath> createdPaths;
    std::vector<TYPath> mountedPaths;

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
        .WillByDefault([&] (const TYPath& path, const TMountTableOptions& /*options*/) {
            mountedPaths.push_back(path);
            return OKFuture;
        });

    TCreateNodeOptions options;
    options.Attributes = CreateEphemeralAttributes();
    options.Attributes->Set("initialize_tables", false);
    CreatePipelineNode(client, "//tmp/pipeline", options);

    EXPECT_EQ(createdPaths, std::vector<TYPath>({"//tmp/pipeline"}));
    EXPECT_TRUE(mountedPaths.empty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
