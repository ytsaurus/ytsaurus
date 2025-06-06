#include <yt/cpp/mapreduce/tests/native/operations/jobs.h>

#include <yt/cpp/mapreduce/tests/native/proto_lib/row.pb.h>

#include <yt/cpp/mapreduce/interface/client.h>

#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/interface/skiff_row.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/yt/string/format.h>

#include <util/system/env.h>

using namespace NYT;
using namespace NYT::NTesting;

////////////////////////////////////////////////////////////////////////////////

REGISTER_MAPPER(TIdMapper)
REGISTER_REDUCER(TIdReducer)
REGISTER_MAPPER(TIdProtoMapper)
REGISTER_REDUCER(TIdProtoReducer)

////////////////////////////////////////////////////////////////////////////////

TString GetEnvChecked(const TString& name) {
    auto value = GetEnv(name);
    if (value.empty()) {
        ythrow yexception() << name << " is not specified" << Endl;
    }
    return value;
}

TString CreateTable(const IClientPtr& client, TYPath path, const TTableSchema& schema)
{
    client->Create(
        path,
        NT_TABLE,
        TCreateOptions()
            .Recursive(true).Force(true)
            .Attributes(TNode()("schema", schema.ToNode())));
    return path;
}

TNode GetClusterName(const IClientPtr& client)
{
    return client->Get("//sys/@cluster_name");
}

void AllowRemoteOperations(const IClientPtr& client, const TVector<TNode>& allowedClusters)
{
    static const TString ControllerAgentsConfigPath = "//sys/controller_agents/config";

    auto exists = client->Exists(ControllerAgentsConfigPath);
    if (!exists) {
        client->Create(ControllerAgentsConfigPath, ENodeType::NT_DOCUMENT, TCreateOptions().Recursive(true));
    }

    client->Set(NYT::Format("%v/remote_operations", ControllerAgentsConfigPath), TNode::CreateMap());

    for (const auto& cluster : allowedClusters) {
        client->Set(
            NYT::Format("%v/remote_operations/%v/allowed_for_everyone", ControllerAgentsConfigPath, cluster.AsString()),
            TNode(true),
            TSetOptions().Recursive(true));
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
::TIntrusivePtr<IMapperBase> GetMapper()
{
    if constexpr (std::is_same_v<T, TNode>) {
        return new TIdMapper;
    } else if constexpr (std::is_same_v<T, TNumberRecord>) {
        return new TIdProtoMapper;
    } else {
        static_assert(TDependentFalse<T>, "Unknown type");
    }
}

template <typename T>
::TIntrusivePtr<IReducerBase> GetReducer()
{
    if constexpr (std::is_same_v<T, TNode>) {
        return new TIdReducer;
    } else if constexpr (std::is_same_v<T, TNumberRecord>) {
        return new TIdProtoReducer;
    } else {
        static_assert(TDependentFalse<T>, "Unknown type");
    }
}

template <typename TRowFormat>
void TestOperations(ENodeReaderFormat format)
{
    TConfig::Get()->NodeReaderFormat = format;

    auto firstClient = CreateTestClient(GetEnvChecked("YT_PROXY_FIRST"));
    auto secondClient = CreateTestClient(GetEnvChecked("YT_PROXY_SECOND"));
    auto thirdClient = CreateTestClient(GetEnvChecked("YT_PROXY_THIRD"));

    auto firstTestingDir = CreateTestDirectory(firstClient);
    auto secondTestingDir = CreateTestDirectory(secondClient);
    auto thirdTestingDir = CreateTestDirectory(thirdClient);

    const auto tableSchema = TTableSchema().AddColumn("number", VT_INT64, SO_ASCENDING);

    CreateTable(firstClient, firstTestingDir + "/input1", tableSchema);
    WriteTable(firstClient, firstTestingDir + "/input1", {TNode()("number", 1), TNode()("number", 2)});

    CreateTable(firstClient, firstTestingDir + "/input2", tableSchema);
    WriteTable(firstClient, firstTestingDir + "/input2", {TNode()("number", 3), TNode()("number", 4)});

    CreateTable(secondClient, secondTestingDir + "/input1", tableSchema);
    WriteTable(secondClient, secondTestingDir + "/input1", {TNode()("number", 5), TNode()("number", 6)});

    CreateTable(secondClient, secondTestingDir + "/input2", tableSchema);
    WriteTable(secondClient, secondTestingDir + "/input2", {TNode()("number", 7), TNode()("number", 8)});

    AllowRemoteOperations(thirdClient, {GetClusterName(firstClient), GetClusterName(secondClient)});

    const std::vector<TNode> expectedRows{
        TNode()("number", 1), TNode()("number", 2), TNode()("number", 3), TNode()("number", 4),
        TNode()("number", 5), TNode()("number", 6), TNode()("number", 7), TNode()("number", 8)};

    CreateTable(thirdClient, thirdTestingDir + "/output", tableSchema);
    thirdClient->Map(
        TMapOperationSpec()
            .Ordered(true)
            .template AddInput<TRowFormat>(TRichYPath(firstTestingDir + "/input1").Cluster(GetClusterName(firstClient).AsString()))
            .template AddInput<TRowFormat>(TRichYPath(firstTestingDir + "/input2").Cluster(GetClusterName(firstClient).AsString()))
            .template AddInput<TRowFormat>(TRichYPath(secondTestingDir + "/input1").Cluster(GetClusterName(secondClient).AsString()))
            .template AddInput<TRowFormat>(TRichYPath(secondTestingDir + "/input2").Cluster(GetClusterName(secondClient).AsString()))
            .template AddOutput<TRowFormat>(thirdTestingDir + "/output"),
        GetMapper<TRowFormat>());

    {
        auto actualRows = ReadTable(thirdClient, thirdTestingDir + "/output");
        EXPECT_EQ(actualRows, expectedRows);
    }

    CreateTable(thirdClient, thirdTestingDir + "/output", tableSchema);
    thirdClient->Reduce(
        TReduceOperationSpec()
            .ReduceBy({"number"})
            .template AddInput<TRowFormat>(TRichYPath(firstTestingDir + "/input1").Cluster(GetClusterName(firstClient).AsString()))
            .template AddInput<TRowFormat>(TRichYPath(firstTestingDir + "/input2").Cluster(GetClusterName(firstClient).AsString()))
            .template AddInput<TRowFormat>(TRichYPath(secondTestingDir + "/input1").Cluster(GetClusterName(secondClient).AsString()))
            .template AddInput<TRowFormat>(TRichYPath(secondTestingDir + "/input2").Cluster(GetClusterName(secondClient).AsString()))
            .template AddOutput<TRowFormat>(thirdTestingDir + "/output"),
        GetReducer<TRowFormat>());

    {
        auto actualRows = ReadTable(thirdClient, thirdTestingDir + "/output");
        EXPECT_EQ(actualRows, expectedRows);
    }

    CreateTable(thirdClient, thirdTestingDir + "/output", tableSchema);
    thirdClient->MapReduce(
        TMapReduceOperationSpec()
            .ReduceBy({"number"})
            .template AddInput<TRowFormat>(TRichYPath(firstTestingDir + "/input1").Cluster(GetClusterName(firstClient).AsString()))
            .template AddInput<TRowFormat>(TRichYPath(firstTestingDir + "/input2").Cluster(GetClusterName(firstClient).AsString()))
            .template AddInput<TRowFormat>(TRichYPath(secondTestingDir + "/input1").Cluster(GetClusterName(secondClient).AsString()))
            .template AddInput<TRowFormat>(TRichYPath(secondTestingDir + "/input2").Cluster(GetClusterName(secondClient).AsString()))
            .template AddOutput<TRowFormat>(thirdTestingDir + "/output"),
        GetMapper<TRowFormat>(),
        GetReducer<TRowFormat>());

    {
        auto actualRows = ReadTable(thirdClient, thirdTestingDir + "/output");
        EXPECT_EQ(actualRows, expectedRows);
    }

    CreateTable(thirdClient, thirdTestingDir + "/output", tableSchema);
    thirdClient->Sort(
        TSortOperationSpec()
            .SortBy({"number"})
            .AddInput(TRichYPath(firstTestingDir + "/input2").Cluster(GetClusterName(firstClient).AsString()))
            .AddInput(TRichYPath(secondTestingDir + "/input1").Cluster(GetClusterName(secondClient).AsString()))
            .AddInput(TRichYPath(secondTestingDir + "/input2").Cluster(GetClusterName(secondClient).AsString()))
            .AddInput(TRichYPath(firstTestingDir + "/input1").Cluster(GetClusterName(firstClient).AsString()))
            .Output(thirdTestingDir + "/output"));

    {
        auto actualRows = ReadTable(thirdClient, thirdTestingDir + "/output");
        EXPECT_EQ(actualRows, expectedRows);
    }

    CreateTable(thirdClient, thirdTestingDir + "/output", tableSchema);
    thirdClient->Merge(
        TMergeOperationSpec()
            .MergeBy({"number"})
            .Mode(EMergeMode::MM_SORTED)
            .AddInput(TRichYPath(firstTestingDir + "/input2").Cluster(GetClusterName(firstClient).AsString()))
            .AddInput(TRichYPath(secondTestingDir + "/input1").Cluster(GetClusterName(secondClient).AsString()))
            .AddInput(TRichYPath(secondTestingDir + "/input2").Cluster(GetClusterName(secondClient).AsString()))
            .AddInput(TRichYPath(firstTestingDir + "/input1").Cluster(GetClusterName(firstClient).AsString()))
            .Output(thirdTestingDir + "/output"));

    {
        auto actualRows = ReadTable(thirdClient, thirdTestingDir + "/output");
        EXPECT_EQ(actualRows, expectedRows);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(RemoteClusters, TNodeOperations)
{
    TestOperations<TNode>(ENodeReaderFormat::Yson);
}

////////////////////////////////////////////////////////////////////////////////

TEST(RemoteClusters, ProtobufOperations)
{
    TestOperations<TNumberRecord>(ENodeReaderFormat::Yson);
}

////////////////////////////////////////////////////////////////////////////////

TEST(RemoteClusters, SkiffOperations)
{
    TestOperations<TNode>(ENodeReaderFormat::Skiff);
}

////////////////////////////////////////////////////////////////////////////////

TEST(RemoteClusters, OperationsWithTransaction)
{
    auto firstClient = CreateTestClient(GetEnvChecked("YT_PROXY_FIRST"));
    auto secondClient = CreateTestClient(GetEnvChecked("YT_PROXY_SECOND"));

    auto firstTestingDir = CreateTestDirectory(firstClient);
    auto secondTestingDir = CreateTestDirectory(secondClient);

    const auto tableSchema = TTableSchema().AddColumn("number", VT_INT64, SO_ASCENDING);

    CreateTable(firstClient, firstTestingDir + "/input1", tableSchema);
    WriteTable(firstClient, firstTestingDir + "/input1", {TNode()("number", 1), TNode()("number", 2)});

    AllowRemoteOperations(secondClient, {GetClusterName(firstClient)});

    const std::vector<TNode> expectedRows{TNode()("number", 1), TNode()("number", 2)};

    auto tx = secondClient->StartTransaction();
    tx->Map(
        TMapOperationSpec()
            .AddInput<TNode>(TRichYPath(firstTestingDir + "/input1").Cluster(GetClusterName(firstClient).AsString()))
            .AddOutput<TNode>(secondTestingDir + "/output"),
        new TIdMapper);
    tx->Commit();

    {
        auto actualRows = ReadTable(secondClient, secondTestingDir + "/output");
        EXPECT_EQ(actualRows, expectedRows);
    }

    tx = secondClient->StartTransaction();
    tx->Map(
        TMapOperationSpec()
            .AddInput<TNumberRecord>(TRichYPath(firstTestingDir + "/input1").Cluster(GetClusterName(firstClient).AsString()))
            .AddOutput<TNumberRecord>(secondTestingDir + "/output"),
        new TIdProtoMapper);
    tx->Commit();

    {
        auto actualRows = ReadTable(secondClient, secondTestingDir + "/output");
        EXPECT_EQ(actualRows, expectedRows);
    }
}

TEST(RemoteClusters, OperationsWithObjectId)
{
    auto firstClient = CreateTestClient(GetEnvChecked("YT_PROXY_FIRST"));
    auto secondClient = CreateTestClient(GetEnvChecked("YT_PROXY_SECOND"));

    auto firstTestingDir = CreateTestDirectory(firstClient);
    auto secondTestingDir = CreateTestDirectory(secondClient);

    const auto tableSchema = TTableSchema().AddColumn("number", VT_INT64, SO_ASCENDING);

    const auto inputTablePath = firstTestingDir + "/input1";

    CreateTable(firstClient, inputTablePath, tableSchema);
    WriteTable(firstClient, inputTablePath, {TNode()("number", 1), TNode()("number", 2)});

    AllowRemoteOperations(secondClient, {GetClusterName(firstClient)});

    const std::vector<TNode> expectedRows{TNode()("number", 1), TNode()("number", 2)};

    auto trunkTableId = firstClient->Get(inputTablePath + "/@id");
    auto trunkTableIdAsPath = "#" + trunkTableId.AsString();

    auto tx = secondClient->StartTransaction();
    tx->Map(
        TMapOperationSpec()
            .AddInput<TNode>(TRichYPath(trunkTableIdAsPath).Cluster(GetClusterName(firstClient).AsString()))
            .AddOutput<TNode>(secondTestingDir + "/output"),
        new TIdMapper);
    tx->Commit();

    {
        auto actualRows = ReadTable(secondClient, secondTestingDir + "/output");
        EXPECT_EQ(actualRows, expectedRows);
    }
}

////////////////////////////////////////////////////////////////////////////////
