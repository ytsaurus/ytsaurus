#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/api/table_client.h>
#include <yt/yt/client/api/table_writer.h>

#include <yt/yt/client/rpc/helpers.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace NCppTests {
namespace {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NRpc;
using namespace NSecurityClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;

using NTableClient::NProto::TColumnMetaExt;
using NApi::TTableWriterOptions;

using testing::Each;
using testing::Eq;

////////////////////////////////////////////////////////////////////////////////

std::string MakeRandomString(size_t stringSize)
{
    std::string randomString;
    randomString.reserve(stringSize);
    for (size_t i = 0; i < stringSize; i++) {
        randomString += ('a' + rand() % 30);
    }
    return randomString;
}

////////////////////////////////////////////////////////////////////////////////

class TReadSizeEstimationTest
    : public TApiTestBase
{
protected:
    NNative::IClientPtr NativeClient_;
    NNative::IConnectionPtr NativeConnection_;

    ITableWriterPtr Writer_;
    TChunkId ChunkId_;
    std::string ChunkReplica_;

    std::vector<std::vector<std::string>> Requests_;
    std::vector<i64> Responses_;

    static void CreateTable(const TTableSchemaPtr& schema, bool isColumnar)
    {
        TCreateNodeOptions options;
        options.Attributes = CreateEphemeralAttributes();
        options.Attributes->Set("schema", schema);
        if (isColumnar) {
            options.Attributes->Set("optimize_for", "scan");
        }
        WaitFor(Client_->CreateNode("//tmp/table", EObjectType::Table, options))
            .ThrowOnError();
    }

    static void CreateColumnarTable(const TTableSchemaPtr& schema)
    {
        CreateTable(schema, /*isColumnar*/ true);
    }

    static void CreateHorizontalTable(const TTableSchemaPtr& schema)
    {
        CreateTable(schema, /*isColumnar*/ false);
    }

    static TColumnSchema MakeColumnSchema(
        const std::string& name,
        EValueType type,
        const std::optional<std::string>& group = std::nullopt)
    {
        TColumnSchema column(name, type);
        if (group.has_value()) {
            column.SetGroup(*group);
        }
        return column;
    }

    void SetUp() override
    {
        NativeClient_ = DynamicPointerCast<NNative::IClient>(Client_);
        NativeConnection_ = NativeClient_->GetNativeConnection();
    }

    void TearDown() override
    {
        Responses_.clear();
        Requests_.clear();
        NativeClient_.Reset();
        NativeConnection_.Reset();
        WaitFor(Client_->RemoveNode("//tmp/table"))
            .ThrowOnError();
    }

    void InitTableWriter()
    {
        TTableWriterOptions options;
        options.Config = New<TTableWriterConfig>();
        options.Config->BlockSize = 1;
        Writer_ = WaitFor(Client_->CreateTableWriter("//tmp/table", options))
            .ValueOrThrow();
    }

    TUnversionedOwningRow BuildRow(
        int firstColumn,
        const std::string& secondColumn,
        const std::string& thirdColumn,
        const std::optional<std::string>& unknownColumn = std::nullopt)
    {
        auto nameTable = Writer_->GetNameTable();

        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedInt64Value(/*value*/ firstColumn, /*id*/ nameTable->GetIdOrRegisterName("small")));
        builder.AddValue(MakeUnversionedStringValue(/*value*/ secondColumn, /*id*/ nameTable->GetIdOrRegisterName("large1")));
        builder.AddValue(MakeUnversionedStringValue(/*value*/ thirdColumn, /*id*/ nameTable->GetIdOrRegisterName("large2")));

        if (unknownColumn.has_value()) {
            builder.AddValue(MakeUnversionedStringValue(/*value*/ *unknownColumn, /*id*/ nameTable->GetIdOrRegisterName("unknown")));
        }

        return builder.FinishRow();
    }

    void FindChunkIdAndReplica()
    {
        auto chunkIds = ConvertTo<std::vector<TChunkId>>(WaitFor(Client_->GetNode("//tmp/table/@chunk_ids"))
            .ValueOrThrow());
        ASSERT_EQ(std::ssize(chunkIds), 1);
        ChunkId_ = chunkIds[0];
        ChunkReplica_ = ConvertTo<std::string>(WaitFor(Client_->GetNode(Format("#%v/@stored_replicas/0", ChunkId_)))
            .ValueOrThrow());
    }

    void EnsureColumnMetaHasMultipleSegments(int expectedColumnCount)
    {
        TDataNodeServiceProxy proxy(NativeConnection_->CreateChannelByAddress(ChunkReplica_));

        auto req = proxy.GetChunkMeta();
        ToProto(req->mutable_chunk_id(), ChunkId_);
        req->add_extension_tags(TProtoExtensionTag<TColumnMetaExt>::Value);
        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();
        auto columnMeta = GetProtoExtension<TColumnMetaExt>(rsp->chunk_meta().extensions());
        ASSERT_EQ(columnMeta.columns_size(), expectedColumnCount);

        for (const auto& column : columnMeta.columns()) {
            EXPECT_GT(column.segments_size(), 1);
        }
    }

    void FetchReadSizeEstimations(std::vector<std::vector<std::string>> requests)
    {
        Requests_ = std::move(requests);

        TDataNodeServiceProxy proxy(NativeConnection_->CreateChannelByAddress(ChunkReplica_));

        auto nameTable = New<TNameTable>();

        auto req = proxy.GetColumnarStatistics();

        SetRequestWorkloadDescriptor(req, TWorkloadDescriptor(EWorkloadCategory::UserBatch));

        for (const auto& request : Requests_) {
            auto* subrequest = req->add_subrequests();
            ToProto(subrequest->mutable_chunk_id(), ChunkId_);
            for (const auto& column : request) {
                subrequest->add_column_ids(nameTable->GetIdOrRegisterName(column));
            }
            subrequest->set_enable_read_size_estimation(true);
        }

        ToProto(req->mutable_name_table(), nameTable);

        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();

        Responses_.clear();
        Responses_.reserve(requests.size());
        for (const auto& response : rsp->subresponses()) {
            Responses_.push_back(response.read_data_size_estimate());
        }

        ASSERT_EQ(Requests_.size(), Responses_.size());
    }

    i64 GetReadSizeEstimation(const std::vector<std::string>& columns)
    {
        auto* it = Find(Requests_, columns);
        YT_VERIFY(it != Requests_.end());
        return Responses_[it - Requests_.begin()];
    }
};

TEST_F(TReadSizeEstimationTest, ColumnarChunkStrictSchemaNoGroups)
{
    CreateColumnarTable(New<TTableSchema>(
        std::vector{
            TColumnSchema("small", EValueType::Int64),
            TColumnSchema("large1", EValueType::String),
            TColumnSchema("large2", EValueType::String)}));

    InitTableWriter();

    // Use random strings to prevent excessive compression.
    Writer_->Write({
        BuildRow(42, MakeRandomString(1000), MakeRandomString(100)),
        BuildRow(43, MakeRandomString(1000), MakeRandomString(100)),
    });

    WaitFor(Writer_->GetReadyEvent())
        .ThrowOnError();

    Writer_->Write({
        BuildRow(1, MakeRandomString(1000), MakeRandomString(100)),
        BuildRow(2, MakeRandomString(1000), MakeRandomString(100)),
    });

    WaitFor(Writer_->Close())
        .ThrowOnError();

    FindChunkIdAndReplica();
    EnsureColumnMetaHasMultipleSegments(/*expectedColumnCount*/ 3);

    FetchReadSizeEstimations({
        {"small"},
        {"large1"},
        {"large2"},
        {"unknown"},
        {"large1", "large2"},
        {"small", "large1"},
        {"small", "large2"},
        {"small", "large1", "large2"},
        {"large1", "unknown"}});

    EXPECT_NEAR(GetReadSizeEstimation({"small"}), 95, 10);
    EXPECT_NEAR(GetReadSizeEstimation({"large1"}), 4100, 50);
    EXPECT_NEAR(GetReadSizeEstimation({"large2"}), 500, 30);
    EXPECT_EQ(GetReadSizeEstimation({"unknown"}), 0);

    EXPECT_EQ(
        GetReadSizeEstimation({"large1"}) + GetReadSizeEstimation({"large2"}),
        GetReadSizeEstimation({"large1", "large2"}));

    EXPECT_EQ(
        GetReadSizeEstimation({"small"}) + GetReadSizeEstimation({"large1"}),
        GetReadSizeEstimation({"small", "large1"}));

    EXPECT_EQ(
        GetReadSizeEstimation({"small"}) + GetReadSizeEstimation({"large2"}),
        GetReadSizeEstimation({"small", "large2"}));

    EXPECT_EQ(
        GetReadSizeEstimation({"small"}) + GetReadSizeEstimation({"large1"}) + GetReadSizeEstimation({"large2"}),
        GetReadSizeEstimation({"small", "large1", "large2"}));

    EXPECT_EQ(GetReadSizeEstimation({"large1", "unknown"}), GetReadSizeEstimation({"large1"}));

    EXPECT_GT(GetReadSizeEstimation({"large1"}), GetReadSizeEstimation({"small"}));
    EXPECT_GT(GetReadSizeEstimation({"large2"}), GetReadSizeEstimation({"small"}));
    EXPECT_GT(GetReadSizeEstimation({"large1"}), GetReadSizeEstimation({"large2"}));
}

TEST_F(TReadSizeEstimationTest, ColumnarChunkNonStrictSchemaNoGroups)
{
    CreateColumnarTable(New<TTableSchema>(
        std::vector{
            TColumnSchema("small", EValueType::Int64),
            TColumnSchema("large1", EValueType::String),
            TColumnSchema("large2", EValueType::String),
        },
        /*strict*/ false));

    InitTableWriter();

    Writer_->Write({
        BuildRow(42, MakeRandomString(1000), MakeRandomString(100)),
        BuildRow(43, MakeRandomString(1000), MakeRandomString(100), MakeRandomString(300)),
    });

    WaitFor(Writer_->GetReadyEvent())
        .ThrowOnError();

    Writer_->Write({
        BuildRow(1, MakeRandomString(1000), MakeRandomString(100)),
        BuildRow(2, MakeRandomString(1000), MakeRandomString(100)),
    });

    WaitFor(Writer_->Close())
        .ThrowOnError();

    FindChunkIdAndReplica();
    EnsureColumnMetaHasMultipleSegments(/*expectedColumnCount*/ 4);

    FetchReadSizeEstimations({
        {"small"},
        {"large1"},
        {"large2"},
        {"unknown"},
        {"large1", "large2"},
        {"small", "large1"},
        {"small", "large2"},
        {"small", "large1", "large2"},
        {"large1", "unknown"},
        {"small", "large2", "unknow_empty", "unknown"},
    });

    EXPECT_NEAR(GetReadSizeEstimation({"small"}), 95, 10);
    EXPECT_NEAR(GetReadSizeEstimation({"large1"}), 4100, 50);
    EXPECT_NEAR(GetReadSizeEstimation({"large2"}), 500, 30);
    EXPECT_NEAR(GetReadSizeEstimation({"unknown"}), 400, 30);

    EXPECT_EQ(
        GetReadSizeEstimation({"large1"}) + GetReadSizeEstimation({"large2"}),
        GetReadSizeEstimation({"large1", "large2"}));

    EXPECT_EQ(
        GetReadSizeEstimation({"small"}) + GetReadSizeEstimation({"large1"}),
        GetReadSizeEstimation({"small", "large1"}));

    EXPECT_EQ(
        GetReadSizeEstimation({"small"}) + GetReadSizeEstimation({"large2"}),
        GetReadSizeEstimation({"small", "large2"}));

    EXPECT_EQ(
        GetReadSizeEstimation({"small"}) + GetReadSizeEstimation({"large1"}) + GetReadSizeEstimation({"large2"}),
        GetReadSizeEstimation({"small", "large1", "large2"}));

    EXPECT_EQ(
        GetReadSizeEstimation({"small"}) + GetReadSizeEstimation({"large2"}) + GetReadSizeEstimation({"unknown"}),
        GetReadSizeEstimation({"small", "large2", "unknow_empty", "unknown"}));

    EXPECT_GT(GetReadSizeEstimation({"large1", "unknown"}), GetReadSizeEstimation({"large1"}));

    EXPECT_GT(GetReadSizeEstimation({"large1"}), GetReadSizeEstimation({"small"}));
    EXPECT_GT(GetReadSizeEstimation({"large2"}), GetReadSizeEstimation({"small"}));
    EXPECT_GT(GetReadSizeEstimation({"large1"}), GetReadSizeEstimation({"large2"}));
}

TEST_F(TReadSizeEstimationTest, ColumnarChunkStrictSchemaWithGroups)
{
    CreateColumnarTable(New<TTableSchema>(
        std::vector{
            MakeColumnSchema("small", EValueType::Int64, "group1"),
            MakeColumnSchema("large1", EValueType::String, "group2"),
            MakeColumnSchema("large2", EValueType::String, "group1")}));

    InitTableWriter();

    Writer_->Write({
        BuildRow(42, MakeRandomString(1000), MakeRandomString(100)),
        BuildRow(43, MakeRandomString(1000), MakeRandomString(100)),
    });

    WaitFor(Writer_->GetReadyEvent())
        .ThrowOnError();

    Writer_->Write({
        BuildRow(1, MakeRandomString(1000), MakeRandomString(100)),
        BuildRow(2, MakeRandomString(1000), MakeRandomString(100)),
    });

    WaitFor(Writer_->Close())
        .ThrowOnError();

    FindChunkIdAndReplica();
    EnsureColumnMetaHasMultipleSegments(/*expectedColumnCount*/ 3);

    FetchReadSizeEstimations({
        {"small"},
        {"large1"},
        {"large2"},
        {"unknown"},
        {"large1", "large2"},
        {"small", "large1"},
        {"small", "large2"},
        {"small", "large1", "large2"}});

    EXPECT_NEAR(GetReadSizeEstimation({"small"}), 590, 20);
    EXPECT_NEAR(GetReadSizeEstimation({"large1"}), 4100, 50);

    EXPECT_EQ(
        GetReadSizeEstimation({"small"}) + GetReadSizeEstimation({"large1"}),
        GetReadSizeEstimation({"small", "large1", "large2"}));

    EXPECT_EQ(GetReadSizeEstimation({"unknown"}), 0);

    EXPECT_EQ(
        GetReadSizeEstimation({"small"}),
        GetReadSizeEstimation({"large2"}));

    EXPECT_EQ(
        GetReadSizeEstimation({"large1", "large2"}),
        GetReadSizeEstimation({"small", "large1", "large2"}));

    EXPECT_EQ(
        GetReadSizeEstimation({"small", "large1"}),
        GetReadSizeEstimation({"small", "large1", "large2"}));

    EXPECT_EQ(
        GetReadSizeEstimation({"small", "large2"}),
        GetReadSizeEstimation({"small"}));
}

TEST_F(TReadSizeEstimationTest, ColumnarChunkNonStrictSchemaWithGroups)
{
    CreateColumnarTable(New<TTableSchema>(
        std::vector{
            MakeColumnSchema("small", EValueType::Int64, "group1"),
            MakeColumnSchema("large1", EValueType::String, "group2"),
            MakeColumnSchema("large2", EValueType::String, "group1")},
        /*strict*/ false));

    InitTableWriter();

    Writer_->Write({
        BuildRow(42, MakeRandomString(1000), MakeRandomString(100)),
        BuildRow(43, MakeRandomString(1000), MakeRandomString(100)),
    });

    WaitFor(Writer_->GetReadyEvent())
        .ThrowOnError();

    Writer_->Write({
        BuildRow(1, MakeRandomString(1000), MakeRandomString(100), MakeRandomString(10000)),
        BuildRow(2, MakeRandomString(1000), MakeRandomString(100)),
    });

    WaitFor(Writer_->Close())
        .ThrowOnError();

    FindChunkIdAndReplica();
    EnsureColumnMetaHasMultipleSegments(/*expectedColumnCount*/ 4);

    FetchReadSizeEstimations({
        {"small"},
        {"large1"},
        {"large2"},
        {"unknown"},
        {"unknown_empty"},
        {"large1", "large2"},
        {"small", "large1"},
        {"small", "large2"},
        {"small", "large1", "large2"},
        {"small", "large1", "large2", "unknow"},
        {"small", "large1", "large2", "unknow_empty"},
        {"small", "large2", "unknow_empty", "unknown"},
    });

    EXPECT_NEAR(GetReadSizeEstimation({"small"}), 590, 20);
    EXPECT_NEAR(GetReadSizeEstimation({"large1"}), 4100, 50);
    EXPECT_NEAR(GetReadSizeEstimation({"unknown"}), 10150, 50);

    EXPECT_EQ(
        GetReadSizeEstimation({"small", "large2"}),
        GetReadSizeEstimation({"small"}));

    EXPECT_EQ(
        GetReadSizeEstimation({"small"}),
        GetReadSizeEstimation({"large2"}));

    EXPECT_EQ(
        GetReadSizeEstimation({"large1", "large2"}),
        GetReadSizeEstimation({"small", "large1", "large2"}));

    EXPECT_EQ(
        GetReadSizeEstimation({"unknown"}),
        GetReadSizeEstimation({"unknown_empty"}));

    EXPECT_EQ(
        GetReadSizeEstimation({"small", "large1", "large2", "unknow"}),
        GetReadSizeEstimation({"small", "large1", "large2", "unknow_empty"}));

    EXPECT_GT(
        GetReadSizeEstimation({"small", "large1", "large2", "unknow_empty"}),
        GetReadSizeEstimation({"small", "large1", "large2"}));

    EXPECT_GT(
        GetReadSizeEstimation({"small", "large1", "large2", "unknow_empty"}),
        GetReadSizeEstimation({"small", "large2", "unknow_empty", "unknown"}));
}

TEST_F(TReadSizeEstimationTest, HorizontalChunkNonStrict)
{
    CreateHorizontalTable(New<TTableSchema>(
        std::vector{
            TColumnSchema("small", EValueType::Int64),
            TColumnSchema("large1", EValueType::String),
            TColumnSchema("large2", EValueType::String),
        },
        /*strict*/ false));

    InitTableWriter();

    Writer_->Write({
        BuildRow(42, MakeRandomString(1000), MakeRandomString(100)),
        BuildRow(43, MakeRandomString(1000), MakeRandomString(100)),
    });

    WaitFor(Writer_->GetReadyEvent())
        .ThrowOnError();

    Writer_->Write({
        BuildRow(1, MakeRandomString(1000), MakeRandomString(100)),
        BuildRow(2, MakeRandomString(1000), MakeRandomString(100), MakeRandomString(10000)),
    });

    WaitFor(Writer_->Close())
        .ThrowOnError();

    FindChunkIdAndReplica();

    FetchReadSizeEstimations({
        {"small"},
        {"large1"},
        {"large2"},
        {"unknown"},
        {"large1", "large2"},
        {"small", "large1"},
        {"small", "large2"},
        {"small", "large1", "large2"},
        {"small", "unknown"},
        {"small", "large1", "large2", "unknown"},
        {"large1", "large2", "unknown"}});

    EXPECT_NEAR(GetReadSizeEstimation({"small"}), 14625, 50);

    EXPECT_THAT(Responses_, Each(Eq(Responses_[0])));
}

TEST_F(TReadSizeEstimationTest, HorizontalChunkStrict)
{
    CreateHorizontalTable(New<TTableSchema>(
        std::vector{
            TColumnSchema("small", EValueType::Int64),
            TColumnSchema("large1", EValueType::String),
            TColumnSchema("large2", EValueType::String),
        }));

    InitTableWriter();

    Writer_->Write({
        BuildRow(42, MakeRandomString(1000), MakeRandomString(100)),
        BuildRow(43, MakeRandomString(1000), MakeRandomString(100)),
    });

    WaitFor(Writer_->GetReadyEvent())
        .ThrowOnError();

    Writer_->Write({
        BuildRow(1, MakeRandomString(1000), MakeRandomString(100)),
        BuildRow(2, MakeRandomString(1000), MakeRandomString(100)),
    });

    WaitFor(Writer_->Close())
        .ThrowOnError();

    FindChunkIdAndReplica();

    FetchReadSizeEstimations({
        {"unknown"},
        {"large1"},
        {"small", "large1", "large2", "unknown"},
        {"small"}});

    EXPECT_NEAR(GetReadSizeEstimation({"small"}), 4580, 50);
    EXPECT_EQ(GetReadSizeEstimation({"unknown"}), GetReadSizeEstimation({"small"}));

    EXPECT_EQ(
        GetReadSizeEstimation({"small"}),
        GetReadSizeEstimation({"large1"}));
    EXPECT_EQ(
        GetReadSizeEstimation({"small", "large1", "large2", "unknown"}),
        GetReadSizeEstimation({"large1"}));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NCppTests
} // namespace NYT

