#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/connectors/static_table/source.h>

#include <yt/yt/flow/library/cpp/common/unittests/mock/state.h>
#include <yt/yt/flow/library/cpp/common/unittests/mock/time_provider.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/complex_types/yson_format_conversion.h>

#include <yt/yt/client/hedging/unittests/mock/cache.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/unittests/mock/client.h>
#include <yt/yt/client/unittests/mock/table_reader.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/library/re2/re2.h>

namespace NYT::NFlow::NStaticTableConnector {
namespace {

using namespace NYPath;
using namespace NTableClient;
using namespace NComplexTypes;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;

using ::testing::_;
using ::testing::Invoke;
using ::testing::NiceMock;
using ::testing::Return;

////////////////////////////////////////////////////////////////////////////////

NClient::NCache::IClientsCachePtr MakeFixedClientsCache(NApi::IClientPtr client)
{
    auto clientsCache = New<NiceMock<NClient::NHedging::TMockClientsCache>>();
    ON_CALL(*clientsCache, GetClient(_)).WillByDefault(Return(std::move(client)));
    return clientsCache;
}

const TTableSchemaPtr& GetValueTableSchema()
{
    static const auto schema = New<TTableSchema>(std::vector<TColumnSchema>{TColumnSchema("value", EValueType::Int64)});
    return schema;
}

IUnversionedRowBatchPtr MakeRowBatch(const std::vector<i64>& values, int valueId)
{
    auto holder = std::make_shared<std::vector<TUnversionedOwningRow>>();
    for (i64 value : values) {
        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedInt64Value(value, valueId));
        holder->push_back(builder.FinishRow());
    }
    std::vector<TUnversionedRow> rows;
    rows.reserve(holder->size());
    for (const auto& row : *holder) {
        rows.push_back(row);
    }
    return CreateBatchFromUnversionedRows(MakeSharedRange(std::move(rows), std::move(holder)));
}

// Reader that yields |values| and then reports |readyAfterRows| as its ready event.
NApi::ITableReaderPtr MakeTableReader(std::vector<i64> values, TFuture<void> readyAfterRows)
{
    auto reader = New<NApi::TMockTableReader>(GetValueTableSchema());
    const int valueId = reader->GetNameTable()->GetIdOrThrow("value");
    auto rows = std::make_shared<std::vector<i64>>(std::move(values));

    EXPECT_CALL(*reader, GetReadyEvent()).WillRepeatedly(Invoke([=] {
        return rows->empty() ? readyAfterRows : OKFuture;
    }));
    EXPECT_CALL(*reader, Read(_)).WillRepeatedly(Invoke([=] (const TRowBatchReadOptions& options) -> IUnversionedRowBatchPtr {
        if (rows->empty()) {
            return nullptr;
        }
        auto count = std::min<i64>(options.MaxRowsPerRead, std::ssize(*rows));
        auto batch = MakeRowBatch(std::vector<i64>(rows->begin(), rows->begin() + count), valueId);
        rows->erase(rows->begin(), rows->begin() + count);
        return batch;
    }));

    return reader;
}

class TStaticTableSourceReaderTest
    : public ::testing::Test
{
protected:
    const NConcurrency::TActionQueuePtr ActionQueue_ = New<NConcurrency::TActionQueue>("StaticTableSourceReaderTest");
    const TStateManagerMockPtr StateManager_ = New<TStateManagerMock>();
    const IStatusProfilerPtr StatusProfiler_ = CreateSyncStatusProfiler();
    const TMessageBatcherSettingsPtr BatcherSettings_ = New<TMessageBatcherSettings>();
    std::vector<TIntrusivePtr<TSource>> Sources_;
    std::vector<i64> ReaderStarts_;

    template <typename TFunctor>
    auto RunInInvoker(TFunctor&& functor)
    {
        auto errorOrValue = WaitFor(BIND(std::forward<TFunctor>(functor)).AsyncVia(ActionQueue_->GetInvoker()).Run());
        if constexpr (requires { errorOrValue.ValueOrThrow(); }) {
            return errorOrValue.ValueOrThrow();
        } else {
            errorOrValue.ThrowOnError();
        }
    }

    TDynamicSourceContextPtr MakeDynamicContext(TDuration readTimeout, i64 lowerRowIndex)
    {
        TRichYPath partitionTable("//table");
        partitionTable.SetCluster("test");
        SetRowIndexRange(partitionTable, lowerRowIndex, 3);

        // clang-format off
        auto dynamicSourceSpec = New<TDynamicSourceSpec>();
        dynamicSourceSpec->Parameters = BuildYsonNodeFluently()
            .BeginMap()
                .Item("read_timeout").Value(readTimeout)
            .EndMap()
            ->AsMap();

        auto dynamicPartitionSpec = BuildYsonNodeFluently()
            .BeginMap()
                .Item("table").Value(partitionTable)
                .Item("event_timestamp").Value(100)
                .Item("system_timestamp").Value(101)
                .Item("rows_per_second").Value(1e9)
            .EndMap()
            ->AsMap();
        // clang-format on

        auto dynamicContext = New<TDynamicSourceContext>();
        dynamicContext->DynamicSourceSpec = dynamicSourceSpec;
        dynamicContext->DynamicPartitionSpec = dynamicPartitionSpec;
        return dynamicContext;
    }

    // Client that returns |readers| from consecutive CreateTableReader calls, recording their start row indices.
    TIntrusivePtr<NApi::TMockClient> MakeClient(std::vector<TFuture<NApi::ITableReaderPtr>> readers)
    {
        auto client = New<NApi::TMockClient>();
        auto pendingReaders = std::make_shared<std::deque<TFuture<NApi::ITableReaderPtr>>>(readers.begin(), readers.end());
        EXPECT_CALL(*client, CreateTableReader(_, _))
            .Times(std::ssize(readers))
            .WillRepeatedly(Invoke([this, pendingReaders] (const TRichYPath& path, const NApi::TTableReaderOptions&) {
                ReaderStarts_.push_back(GetRowIndexRange(path).first);
                auto reader = pendingReaders->front();
                pendingReaders->pop_front();
                return reader;
            }));
        return client;
    }

    TIntrusivePtr<TSource> MakeSource(const NApi::IClientPtr& client, TDuration readTimeout)
    {
        TRichYPath table("//table");
        table.SetCluster("test");

        auto sourceSpec = New<TSourceSpec>();
        sourceSpec->SourceClassName = TypeName<TSource>();
        // clang-format off
        sourceSpec->Parameters = BuildYsonNodeFluently()
            .BeginMap()
                .Item("tables").BeginList()
                    .Item().Value(table)
                .EndList()
                .Item("finite").Value(true)
            .EndMap()
            ->AsMap();
        // clang-format on

        auto context = New<TSourceContext>();
        context->SourceStreamId = TStreamId("test_input");
        context->SourceSpec = sourceSpec;
        context->ClientsCache = MakeFixedClientsCache(client);
        context->SerializedInvoker = ActionQueue_->GetInvoker();
        context->Logger = NLogging::TLogger("Test");
        context->StatusProfiler = StatusProfiler_;
        context->TimeProvider = New<TFakeTimeProvider>();

        auto source = New<TSource>(std::move(context), MakeDynamicContext(readTimeout, 0));
        RunInInvoker([&] {
            source->Init(StateManager_->CreateContext(Format("source_%v", Sources_.size())));
        });
        Sources_.push_back(source);
        return source;
    }

    void TrimSource(const TIntrusivePtr<TSource>& source, TDuration readTimeout, i64 lowerRowIndex)
    {
        RunInInvoker([&] {
            source->Reconfigure(MakeDynamicContext(readTimeout, lowerRowIndex));
        });
    }

    void CommitSource(const TIntrusivePtr<TSource>& source)
    {
        RunInInvoker([&] {
            source->Commit();
        });
    }

    bool HasReadError() const
    {
        return StatusProfiler_->GetStatus().Errors.contains("/read");
    }

    static bool IsCanceled(const TFuture<void>& pendingRead)
    {
        return pendingRead.IsSet() && pendingRead.GetOrCrash().FindMatching(NYT::EErrorCode::Canceled).has_value();
    }

    // The read timeout is measured against the wall clock, so the first poll may still fit into it.
    void PollUntilCanceled(const TIntrusivePtr<TSource>& source, const TFuture<void>& pendingRead)
    {
        for (int attempt = 0; attempt < 2 && !IsCanceled(pendingRead); ++attempt) {
            EXPECT_TRUE(ReadRows(source).empty());
        }
    }

    std::vector<std::pair<i64, i64>> ReadRows(const TIntrusivePtr<TSource>& source)
    {
        return RunInInvoker([&] {
            auto batches = WaitFor(source->GetNextBatch(BatcherSettings_)).ValueOrThrow();
            std::vector<std::pair<i64, i64>> rows;
            for (const auto& batch : batches) {
                for (const auto& message : batch.Messages) {
                    rows.emplace_back(GetColumnValue<i64>(message, "value"), GetColumnValue<i64>(message, SequenceNumberColumnName));
                }
            }
            return rows;
        });
    }

    void SetUp() override
    {
        BatcherSettings_->MaxRowsPerBatch = TSize(10);
    }

    void TearDown() override
    {
        for (const auto& source : Sources_) {
            source->Terminate();
        }
        ActionQueue_->Shutdown();
        Sources_.clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

std::function<TRangeId()> MakeRangeIdGenerator(i64 startCounter)
{
    return [counter = startCounter] () mutable {
        return TRangeId(counter++);
    };
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStaticTableSourceTest, DoDistributingSimple)
{

    auto distributingTable = New<TSourceControllerTable>();
    distributingTable->Path = "primary://home/t1";
    distributingTable->EventTimestamp = TSystemTimestamp(101);
    distributingTable->SystemTimestamp = TSystemTimestamp(102);
    distributingTable->RowCount = 30000;
    distributingTable->DistributedRows = 10000;
    distributingTable->DistributingRanges = {
        {TRangeId(41000), {8000, 9000}},
        {TRangeId(41001), {9000, 10000}},
    };
    auto originalDistributingRanges = distributingTable->DistributingRanges;

    auto sourceParameters = New<TDynamicTableSourceParameters>();
    sourceParameters->DesiredTableProcessTime = TDuration::Seconds(10);
    sourceParameters->DesiredPartitionProcessTime = TDuration::Seconds(2);

    TSourceController::DoDistributing(
        sourceParameters,
        /*desiredRangeRowsPerSecond*/ 1000,
        /*committedOffsetsExclusive*/ {},
        distributingTable,
        MakeRangeIdGenerator(42000));

    EXPECT_EQ(distributingTable->DistributingRanges[TRangeId(41000)], originalDistributingRanges[TRangeId(41000)]);
    EXPECT_EQ(distributingTable->DistributingRanges[TRangeId(41001)], originalDistributingRanges[TRangeId(41001)]);

    ASSERT_EQ(distributingTable->DistributingRanges.size(), 3u);
    EXPECT_EQ(distributingTable->DistributingRanges.at(TRangeId(42000)), (std::pair<i64, i64>(10000, 12000)));
    EXPECT_EQ(distributingTable->DistributedRows, 12000);
}

TEST(TStaticTableSourceTest, DoDistributingMaxPartitionCount)
{
    auto distributingTable = New<TSourceControllerTable>();
    distributingTable->Path = "primary://home/t1";
    distributingTable->RowCount = 15;
    distributingTable->DistributedRows = 10;

    auto sourceParameters = New<TDynamicTableSourceParameters>();
    sourceParameters->DesiredTableProcessTime = TDuration::Seconds(1);
    sourceParameters->DesiredPartitionProcessTime = TDuration::Seconds(1);
    sourceParameters->MaxPartitionCount = TSize(1);

    TSourceController::DoDistributing(
        sourceParameters,
        /*desiredRangeRowsPerSecond*/ 0.01,
        /*committedOffsetsExclusive*/ {},
        distributingTable,
        MakeRangeIdGenerator(42000));

    ASSERT_EQ(distributingTable->DistributingRanges.size(), 1u);
    EXPECT_EQ(distributingTable->DistributingRanges.at(TRangeId(42000)), (std::pair<i64, i64>(10, 11)));
    EXPECT_EQ(distributingTable->DistributedRows, 11);

    // Check that max partition count has an effect.
    sourceParameters->MaxPartitionCount = TSize(2);
    TSourceController::DoDistributing(
        sourceParameters,
        /*desiredRangeRowsPerSecond*/ 0.01,
        /*committedOffsetsExclusive*/ {},
        distributingTable,
        MakeRangeIdGenerator(43000));

    ASSERT_EQ(distributingTable->DistributingRanges.size(), 2u);
    EXPECT_EQ(distributingTable->DistributingRanges.at(TRangeId(43000)), (std::pair<i64, i64>(11, 12)));
    EXPECT_EQ(distributingTable->DistributedRows, 12);
}

TEST(TStaticTableSourceTest, IgnoreSymlinks)
{
    auto lastProcessingTable = New<TSourceControllerTable>();
    lastProcessingTable->Path = "primary://home/t1";
    lastProcessingTable->RowCount = 30;
    lastProcessingTable->DistributedRows = 20;
    lastProcessingTable->DistributingRanges[TRangeId(40000)] = std::pair<i64, i64>(12, 20);
    // clang-format off
    auto linkNode = NYT::NYTree::BuildYsonNodeFluently()
        .BeginAttributes()
            .Item("type").Value(NYT::NObjectClient::EObjectType::Link)
        .EndAttributes()
        .Entity();

    auto tableNode = NYT::NYTree::BuildYsonNodeFluently()
        .BeginAttributes()
            .Item("type").Value(NYT::NObjectClient::EObjectType::Table)
            .Item("id").Value("2025-01-01T00:00:00Z")
            .Item("creation_time").Value("2025-01-01T00:00:00Z")
            .Item("key").Value("2025-01-01T00:00:00Z")
            .Item("row_count").Value(10)
            .Item("uncompressed_data_size").Value(100)
        .EndAttributes()
        .Entity();
    // clang-format on

    auto sourceDynParameters = New<TDynamicTableSourceParameters>();
    auto sourceParameters = New<TTableSourceParameters>();
    sourceParameters->IgnoreSymlinks = true;

    EXPECT_TRUE(TSourceController::MakeTables({{"primary://home/t1", linkNode}}, sourceParameters, sourceDynParameters, lastProcessingTable, 10).empty());
    EXPECT_TRUE(TSourceController::MakeTables({{"primary://home/t1", tableNode}}, sourceParameters, sourceDynParameters, lastProcessingTable, 10).size() == 1);

    sourceParameters->IgnoreSymlinks = false;

    EXPECT_TRUE(TSourceController::MakeTables({{"primary://home/t1", tableNode}}, sourceParameters, sourceDynParameters, lastProcessingTable, 10).size() == 1);
}

TEST(TStaticTableSourceTest, SkipNonTableNodes)
{
    auto lastProcessingTable = New<TSourceControllerTable>();
    lastProcessingTable->Path = "primary://home/t1";
    lastProcessingTable->RowCount = 30;
    lastProcessingTable->DistributedRows = 20;
    lastProcessingTable->DistributingRanges[TRangeId(40000)] = std::pair<i64, i64>(12, 20);
    // clang-format off
    auto mapNode = NYT::NYTree::BuildYsonNodeFluently()
        .BeginAttributes()
            .Item("type").Value(NYT::NObjectClient::EObjectType::MapNode)
        .EndAttributes()
        .Entity();

    auto tableNode = NYT::NYTree::BuildYsonNodeFluently()
        .BeginAttributes()
            .Item("type").Value(NYT::NObjectClient::EObjectType::Table)
            .Item("id").Value("2025-01-01T00:00:00Z")
            .Item("creation_time").Value("2025-01-01T00:00:00Z")
            .Item("key").Value("2025-01-01T00:00:00Z")
            .Item("row_count").Value(10)
            .Item("uncompressed_data_size").Value(100)
        .EndAttributes()
        .Entity();
    // clang-format on

    auto sourceDynParameters = New<TDynamicTableSourceParameters>();
    auto sourceParameters = New<TTableSourceParameters>();
    sourceParameters->SkipNonTableNodes = true;

    // Map nodes are skipped when SkipNonTableNodes is true.
    EXPECT_TRUE(TSourceController::MakeTables({{"primary://home/raw-data", mapNode}}, sourceParameters, sourceDynParameters, lastProcessingTable, 10).empty());
    // Table nodes are still processed.
    EXPECT_EQ(TSourceController::MakeTables({{"primary://home/t1", tableNode}}, sourceParameters, sourceDynParameters, lastProcessingTable, 10).size(), 1u);
    // Mixed: map node is skipped, table is processed.
    EXPECT_EQ(TSourceController::MakeTables({{"primary://home/raw-data", mapNode}, {"primary://home/t1", tableNode}}, sourceParameters, sourceDynParameters, lastProcessingTable, 10).size(), 1u);

    sourceParameters->SkipNonTableNodes = false;
    // Without the flag, non-table node throws.
    EXPECT_THROW(
        TSourceController::MakeTables({{"primary://home/raw-data", mapNode}}, sourceParameters, sourceDynParameters, lastProcessingTable, 10),
        NYT::TErrorException);
}

TEST(TStaticTableSourceTest, ResolveTableRejectsSymlink)
{
    // clang-format off
    auto linkNode = NYT::NYTree::BuildYsonNodeFluently()
        .BeginAttributes()
            .Item("type").Value(NYT::NObjectClient::EObjectType::Link)
        .EndAttributes()
        .Entity();

    auto tableNode = NYT::NYTree::BuildYsonNodeFluently()
        .BeginAttributes()
            .Item("type").Value(NYT::NObjectClient::EObjectType::Table)
            .Item("id").Value("table-id")
        .EndAttributes()
        .Entity();
    // clang-format on

    // An explicitly listed symlink is fetched unresolved (the "&" suffix) and rejected instead of
    // being silently followed to its target.
    auto linkClient = New<testing::StrictMock<NApi::TMockClient>>();
    EXPECT_CALL(*linkClient, GetNode(NYPath::TYPath("//home/recent&"), testing::_))
        .WillOnce(testing::Return(MakeFuture(ConvertToYsonString(linkNode))));
    EXPECT_THROW(
        TSourceController::ResolveTable(linkClient, NYPath::TRichYPath("primary://home/recent"), /*attributes*/ {}),
        NYT::TErrorException);

    // A plain table is accepted.
    auto tableClient = New<testing::StrictMock<NApi::TMockClient>>();
    EXPECT_CALL(*tableClient, GetNode(NYPath::TYPath("//home/t&"), testing::_))
        .WillOnce(testing::Return(MakeFuture(ConvertToYsonString(tableNode))));
    auto [path, node] = TSourceController::ResolveTable(tableClient, NYPath::TRichYPath("primary://home/t"), /*attributes*/ {});
    EXPECT_EQ(
        node->Attributes().Get<NYT::NObjectClient::EObjectType>("type"),
        NYT::NObjectClient::EObjectType::Table);
}

TEST(TStaticTableSourceTest, MinEventTimestampFilter)
{
    auto lastProcessingTable = New<TSourceControllerTable>();
    lastProcessingTable->Path = "primary://home/t1";
    lastProcessingTable->RowCount = 30;
    lastProcessingTable->DistributedRows = 20;
    lastProcessingTable->DistributingRanges[TRangeId(40000)] = std::pair<i64, i64>(12, 20);
    auto sourceParameters = New<TTableSourceParameters>();
    // clang-format off
    auto tableNode = NYT::NYTree::BuildYsonNodeFluently()
        .BeginAttributes()
            .Item("type").Value(NYT::NObjectClient::EObjectType::Table)
            .Item("id").Value("2025-01-01T00:00:00Z")
            .Item("creation_time").Value("2025-01-01T00:00:00Z")
            .Item("key").Value("2025-01-01T00:00:00Z")
            .Item("row_count").Value(10)
            .Item("uncompressed_data_size").Value(100)
        .EndAttributes()
        .Entity();
    // clang-format on

    auto sourceDynParameters = New<TDynamicTableSourceParameters>();

    TInstant minEventTimestamp;
    TInstant::TryParseIso8601("2025-01-01T00:00:10Z", minEventTimestamp);

    // clang-format off
    std::vector<std::pair<TRichYPath, INodePtr>> InputTables({
        {
            "primary://home/t1",
            NYT::NYTree::BuildYsonNodeFluently()
                .BeginAttributes()
                .Item("type").Value(NYT::NObjectClient::EObjectType::Table)
                .Item("id").Value("2025-01-01T00:00:00Z")
                .Item("creation_time").Value("2025-01-01T00:00:00Z")
                .Item("key").Value("2025-01-01T00:00:00Z")
                .Item("row_count").Value(10)
                .Item("uncompressed_data_size").Value(100)
                .EndAttributes()
                .Entity()
        },
        {
            "primary://home/t2",
            NYT::NYTree::BuildYsonNodeFluently()
                .BeginAttributes()
                .Item("type").Value(NYT::NObjectClient::EObjectType::Table)
                .Item("id").Value("2025-01-01T00:00:20Z")
                .Item("creation_time").Value("2025-01-01T00:00:20Z")
                .Item("key").Value("2025-01-01T00:00:20Z")
                .Item("row_count").Value(10)
                .Item("uncompressed_data_size").Value(100)
                .EndAttributes()
                .Entity()
        }
    });
    // clang-format on

    std::vector<TSourceControllerTablePtr> tables = TSourceController::MakeTables(InputTables, sourceParameters, sourceDynParameters, lastProcessingTable, 2);

    EXPECT_TRUE(tables.size() == 2);

    sourceDynParameters->MinEventTimestamp = minEventTimestamp.Seconds();
    tables = TSourceController::MakeTables(InputTables, sourceParameters, sourceDynParameters, lastProcessingTable, 2);
    EXPECT_TRUE(tables.size() == 1);
}

TEST(TStaticTableSourceTest, MaxEventTimestampFilter)
{
    auto lastProcessingTable = New<TSourceControllerTable>();
    lastProcessingTable->Path = "primary://home/t1";
    lastProcessingTable->RowCount = 30;
    lastProcessingTable->DistributedRows = 20;
    lastProcessingTable->DistributingRanges[TRangeId(40000)] = std::pair<i64, i64>(12, 20);
    auto sourceParameters = New<TTableSourceParameters>();

    auto sourceDynParameters = New<TDynamicTableSourceParameters>();

    TInstant maxEventTimestamp;
    TInstant::TryParseIso8601("2025-01-01T00:00:10Z", maxEventTimestamp);

    // clang-format off
    std::vector<std::pair<TRichYPath, INodePtr>> InputTables({
        {
            "primary://home/t1",
            NYT::NYTree::BuildYsonNodeFluently()
                .BeginAttributes()
                .Item("type").Value(NYT::NObjectClient::EObjectType::Table)
                .Item("id").Value("2025-01-01T00:00:00Z")
                .Item("creation_time").Value("2025-01-01T00:00:00Z")
                .Item("key").Value("2025-01-01T00:00:00Z")
                .Item("row_count").Value(10)
                .Item("uncompressed_data_size").Value(100)
                .EndAttributes()
                .Entity()
        },
        {
            "primary://home/t2",
            NYT::NYTree::BuildYsonNodeFluently()
                .BeginAttributes()
                .Item("type").Value(NYT::NObjectClient::EObjectType::Table)
                .Item("id").Value("2025-01-01T00:00:20Z")
                .Item("creation_time").Value("2025-01-01T00:00:20Z")
                .Item("key").Value("2025-01-01T00:00:20Z")
                .Item("row_count").Value(10)
                .Item("uncompressed_data_size").Value(100)
                .EndAttributes()
                .Entity()
        }
    });
    // clang-format on

    std::vector<TSourceControllerTablePtr> tables = TSourceController::MakeTables(InputTables, sourceParameters, sourceDynParameters, lastProcessingTable, 2);

    EXPECT_TRUE(tables.size() == 2);

    sourceDynParameters->MaxEventTimestamp = maxEventTimestamp.Seconds();
    tables = TSourceController::MakeTables(InputTables, sourceParameters, sourceDynParameters, lastProcessingTable, 2);
    EXPECT_TRUE(tables.size() == 1);
    EXPECT_EQ(tables[0]->Path.Attributes().Get<std::string>("original_path"), "//home/t1");
}

TEST(TStaticTableSourceTest, TableNameFilter)
{
    auto lastProcessingTable = New<TSourceControllerTable>();
    lastProcessingTable->Path = "primary://home/t1";
    lastProcessingTable->RowCount = 30;
    lastProcessingTable->DistributedRows = 20;
    lastProcessingTable->DistributingRanges[TRangeId(40000)] = std::pair<i64, i64>(12, 20);

    auto makeTableNode = [] (const std::string& key) {
        // clang-format off
        return NYT::NYTree::BuildYsonNodeFluently()
            .BeginAttributes()
                .Item("type").Value(NYT::NObjectClient::EObjectType::Table)
                .Item("id").Value(key)
                .Item("creation_time").Value(key)
                .Item("key").Value(key)
                .Item("row_count").Value(10)
                .Item("uncompressed_data_size").Value(100)
            .EndAttributes()
            .Entity();
        // clang-format on
    };

    std::vector<std::pair<TRichYPath, INodePtr>> inputTables({
        {"primary://home/2025-01-01T00:00:00Z", makeTableNode("2025-01-01T00:00:00Z")},
        {"primary://home/tmp_backup", makeTableNode("2025-01-01T00:00:20Z")},
    });

    auto sourceDynParameters = New<TDynamicTableSourceParameters>();
    auto sourceParameters = New<TTableSourceParameters>();

    // No filter: all tables are visible.
    EXPECT_EQ(TSourceController::MakeTables(inputTables, sourceParameters, sourceDynParameters, lastProcessingTable, 2).size(), 2u);

    // Only tables with matching names are visible.
    sourceParameters->TableNameFilter = New<NRe2::TRe2>("2025-.*");
    auto tables = TSourceController::MakeTables(inputTables, sourceParameters, sourceDynParameters, lastProcessingTable, 2);
    ASSERT_EQ(tables.size(), 1u);
    EXPECT_EQ(tables[0]->Path.Attributes().Get<std::string>("original_path"), "//home/2025-01-01T00:00:00Z");

    // The whole name must match, a substring match is not enough.
    sourceParameters->TableNameFilter = New<NRe2::TRe2>("2025");
    EXPECT_TRUE(TSourceController::MakeTables(inputTables, sourceParameters, sourceDynParameters, lastProcessingTable, 2).empty());

    // The filter applies to tables only: a non-table node still fails type validation
    // even if its name does not match.
    // clang-format off
    auto mapNode = NYT::NYTree::BuildYsonNodeFluently()
        .BeginAttributes()
            .Item("type").Value(NYT::NObjectClient::EObjectType::MapNode)
        .EndAttributes()
        .Entity();
    // clang-format on

    sourceParameters->TableNameFilter = New<NRe2::TRe2>("2025-.*");
    EXPECT_THROW(
        TSourceController::MakeTables({{"primary://home/raw-data", mapNode}}, sourceParameters, sourceDynParameters, lastProcessingTable, 2),
        NYT::TErrorException);
    // But it is skipped without an error under SkipNonTableNodes.
    sourceParameters->SkipNonTableNodes = true;
    EXPECT_TRUE(TSourceController::MakeTables({{"primary://home/raw-data", mapNode}}, sourceParameters, sourceDynParameters, lastProcessingTable, 2).empty());
}

TEST(TStaticTableSourceTest, TableNameFilterSpecParsing)
{
    auto spec = ConvertTo<TTableSourceParametersPtr>(TYsonString(TString(
        R"({tables_path="primary://home/dir"; table_name_filter="2025-01-.*"})")));
    ASSERT_TRUE(spec->TableNameFilter);
    EXPECT_EQ(spec->TableNameFilter->pattern(), "2025-01-.*");

    // Invalid regexp is rejected at spec parse time.
    EXPECT_THROW(
        ConvertTo<TTableSourceParametersPtr>(TYsonString(TString(
            R"({tables_path="primary://home/dir"; table_name_filter="[unclosed"})"))),
        NYT::TErrorException);
}

TEST(TStaticTableSourceTest, DoDistributingFewRemainedRows)
{
    // Make correction of DesiredPartitionProcessTime for small table.
    {
        auto distributingTable = New<TSourceControllerTable>();
        distributingTable->Path = "primary://home/t1";
        distributingTable->RowCount = 15;
        distributingTable->DistributedRows = 0;

        auto committedOffsetsExclusive = THashMap<TRangeId, i64>{};

        EXPECT_EQ(TSourceController::GetRemainingTableRows(distributingTable, committedOffsetsExclusive), 15);

        auto sourceParameters = New<TDynamicTableSourceParameters>();
        sourceParameters->DesiredTableProcessTime = TDuration::Seconds(5); // Desired total speed: 3 rows/s.
        sourceParameters->DesiredPartitionProcessTime = TDuration::Seconds(15);
        sourceParameters->DesiredPartitionRowsPerSecond = 1.0;
        sourceParameters->MaxPartitionCount = TSize(10000);

        TSourceController::DoDistributing(
            sourceParameters,
            /*desiredRangeRowsPerSecond*/ 1.0,
            committedOffsetsExclusive,
            distributingTable,
            MakeRangeIdGenerator(42000));

        ASSERT_EQ(distributingTable->DistributingRanges.size(), 3u);
        EXPECT_EQ(distributingTable->DistributingRanges.at(TRangeId(42000)), (std::pair<i64, i64>(0, 5)));
        EXPECT_EQ(distributingTable->DistributingRanges.at(TRangeId(42001)), (std::pair<i64, i64>(5, 10)));
        EXPECT_EQ(distributingTable->DistributedRows, 15);
    }
    // Make correction of DesiredPartitionProcessTime for end of table.
    {
        auto distributingTable = New<TSourceControllerTable>();
        distributingTable->Path = "primary://home/t1";
        distributingTable->RowCount = 30;
        distributingTable->DistributedRows = 20;
        distributingTable->DistributingRanges[TRangeId(40000)] = std::pair<i64, i64>(12, 20);

        auto committedOffsetsExclusive = THashMap<TRangeId, i64>{{TRangeId(40000), 15}}; // [15, 20) is only range in processing right now.

        EXPECT_EQ(TSourceController::GetRemainingTableRows(distributingTable, committedOffsetsExclusive), 15);

        auto sourceParameters = New<TDynamicTableSourceParameters>();
        sourceParameters->DesiredTableProcessTime = TDuration::Seconds(10); // Desired total speed: 3 rows/s.
        sourceParameters->DesiredPartitionProcessTime = TDuration::Seconds(15);
        sourceParameters->DesiredPartitionRowsPerSecond = 1.0;
        sourceParameters->MaxPartitionCount = TSize(10000);

        TSourceController::DoDistributing(
            sourceParameters,
            /*desiredRangeRowsPerSecond*/ 1.0,
            committedOffsetsExclusive,
            distributingTable,
            MakeRangeIdGenerator(42000));

        ASSERT_EQ(distributingTable->DistributingRanges.size(), 3u);
        EXPECT_EQ(distributingTable->DistributingRanges.at(TRangeId(40000)), (std::pair<i64, i64>(12, 20)));
        EXPECT_EQ(distributingTable->DistributingRanges.at(TRangeId(42000)), (std::pair<i64, i64>(20, 25)));
        EXPECT_EQ(distributingTable->DistributingRanges.at(TRangeId(42001)), (std::pair<i64, i64>(25, 30)));
        EXPECT_EQ(distributingTable->DistributedRows, 30);
    }
}

TEST(TStaticTableSourceTest, GetSchemaAndMappingIndex)
{
    auto nameTable = New<NTableClient::TNameTable>();
    nameTable->RegisterNameOrThrow("a");
    nameTable->RegisterNameOrThrow("b");

    auto buffer = New<NTableClient::TRowBuffer>();
    std::vector<NTableClient::TUnversionedRow> rows;

    {
        NTableClient::TUnversionedRowBuilder builder(nameTable->GetSize());
        builder.AddValue(NTableClient::MakeUnversionedStringValue("Hello!", nameTable->GetIdOrThrow("b")));
        rows.push_back(buffer->CaptureRow(builder.GetRow(), /*captureValues*/ true));
    }
    {
        NTableClient::TUnversionedRowBuilder builder(nameTable->GetSize());
        builder.AddValue(NTableClient::MakeUnversionedUint64Value(42, nameTable->GetIdOrThrow("a")));
        builder.AddValue(NTableClient::MakeUnversionedStringValue("Hello!", nameTable->GetIdOrThrow("b")));
        rows.push_back(buffer->CaptureRow(builder.GetRow(), /*captureValues*/ true));
    }

    std::vector<EValueType> wireTypes(nameTable->GetSize(), EValueType::Min);
    auto [schema, idToColumnIndex] = TSource::GetSchemaAndMappingIndex(MakeSharedRange(std::move(rows), std::move(buffer)), nameTable, wireTypes, {});
    auto description = Format("Parameters = {Schema = %v; IdToColumnIndex = %v}",
        ConvertToYsonString(schema, EYsonFormat::Text),
        ConvertToYsonString(idToColumnIndex, EYsonFormat::Text));
    EXPECT_THAT(idToColumnIndex, ::testing::ElementsAre(1, 0)) << description;

    auto columns = schema->Columns();
    ASSERT_EQ(columns.size(), 3u) << description;
    EXPECT_EQ(columns[0].Name(), "b");
    EXPECT_EQ(columns[0].GetWireType(), EValueType::String);
    EXPECT_EQ(columns[1].Name(), "a");
    EXPECT_EQ(columns[1].GetWireType(), EValueType::Uint64);
    EXPECT_EQ(columns[2].Name(), SequenceNumberColumnName);
    EXPECT_EQ(columns[2].GetWireType(), EValueType::Int64);
}

TEST(TStaticTableSourceTest, GetSchemaAndMappingIndexNullFirst)
{
    // Reproduces the case: weak schema table with an optional column where null appears before
    // the first non-null value in the batch. The column type must be upgraded to String.
    auto nameTable = New<NTableClient::TNameTable>();
    nameTable->RegisterNameOrThrow("data");
    nameTable->RegisterNameOrThrow("opt");

    auto buffer = New<NTableClient::TRowBuffer>();
    std::vector<NTableClient::TUnversionedRow> rows;

    {
        NTableClient::TUnversionedRowBuilder builder(nameTable->GetSize());
        builder.AddValue(NTableClient::MakeUnversionedStringValue("hello", nameTable->GetIdOrThrow("data")));
        builder.AddValue(NTableClient::MakeUnversionedNullValue(nameTable->GetIdOrThrow("opt")));
        rows.push_back(buffer->CaptureRow(builder.GetRow(), /*captureValues*/ true));
    }
    {
        NTableClient::TUnversionedRowBuilder builder(nameTable->GetSize());
        builder.AddValue(NTableClient::MakeUnversionedStringValue("world", nameTable->GetIdOrThrow("data")));
        builder.AddValue(NTableClient::MakeUnversionedStringValue("value", nameTable->GetIdOrThrow("opt")));
        rows.push_back(buffer->CaptureRow(builder.GetRow(), /*captureValues*/ true));
    }

    const std::vector<EValueType> wireTypes(nameTable->GetSize(), EValueType::Min);
    const auto [schema, idToColumnIndex] = TSource::GetSchemaAndMappingIndex(MakeSharedRange(std::move(rows), std::move(buffer)), nameTable, wireTypes, {});

    const auto columns = schema->Columns();
    ASSERT_EQ(columns.size(), 3u); // data, opt, sequence_number
    const int optIdx = idToColumnIndex[nameTable->GetIdOrThrow("opt")];
    EXPECT_EQ(columns[optIdx].GetWireType(), EValueType::String);
}

TEST(TStaticTableSourceTest, GetSchemaAndMappingIndexAllNull)
{
    // Reproduces the case when a weak schema table has an optional column whose every cell
    // in the batch is null. The column type must fall back to Any since no concrete type can be inferred.
    auto nameTable = New<NTableClient::TNameTable>();
    nameTable->RegisterNameOrThrow("data");
    nameTable->RegisterNameOrThrow("opt");

    auto buffer = New<NTableClient::TRowBuffer>();
    std::vector<NTableClient::TUnversionedRow> rows;

    for (int i = 0; i < 3; ++i) {
        NTableClient::TUnversionedRowBuilder builder(nameTable->GetSize());
        builder.AddValue(NTableClient::MakeUnversionedStringValue("hello", nameTable->GetIdOrThrow("data")));
        builder.AddValue(NTableClient::MakeUnversionedNullValue(nameTable->GetIdOrThrow("opt")));
        rows.push_back(buffer->CaptureRow(builder.GetRow(), /*captureValues*/ true));
    }

    const std::vector<EValueType> wireTypes(nameTable->GetSize(), EValueType::Min);
    const auto [schema, idToColumnIndex] = TSource::GetSchemaAndMappingIndex(MakeSharedRange(std::move(rows), std::move(buffer)), nameTable, wireTypes, {});

    const auto columns = schema->Columns();
    ASSERT_EQ(columns.size(), 3u); // data, opt, sequence_number
    const int optIdx = idToColumnIndex[nameTable->GetIdOrThrow("opt")];
    EXPECT_EQ(columns[optIdx].GetWireType(), EValueType::Any);
}

TEST(TStaticTableSourceTest, GetSchemaAndMappingIndexV1Any)
{
    // Regression test: V1 any column with String-typed physical cells must not throw
    // "Inconsistent types in batch" and must report the column as Any in the schema.
    auto nameTable = New<NTableClient::TNameTable>();
    nameTable->RegisterNameOrThrow("data");

    auto buffer = New<NTableClient::TRowBuffer>();
    std::vector<NTableClient::TUnversionedRow> rows;

    TString ysonBytes1 = "{key=value1}";
    TString ysonBytes2 = "{key=value2}";
    for (const auto& ysonBytes : {ysonBytes1, ysonBytes2}) {
        NTableClient::TUnversionedRowBuilder builder(nameTable->GetSize());
        TUnversionedValue v{};
        v.Id = nameTable->GetIdOrThrow("data");
        v.Type = EValueType::String;
        v.Data.String = ysonBytes.data();
        v.Length = ysonBytes.size();
        builder.AddValue(v);
        rows.push_back(buffer->CaptureRow(builder.GetRow(), /*captureValues*/ true));
    }

    // Simulate InferTableSchema result for a V1 any column.
    std::vector<EValueType> wireTypes(nameTable->GetSize(), EValueType::Min);
    wireTypes[nameTable->GetIdOrThrow("data")] = EValueType::Any;
    THashMap<int, NComplexTypes::TYsonServerToClientConverter> anyConverters;
    anyConverters.emplace(nameTable->GetIdOrThrow("data"), TSource::MakeAnyColumnConverter());

    // Must not throw despite String cells clashing with the pre-seeded Any wire type.
    const auto [schema, idToColumnIndex] = TSource::GetSchemaAndMappingIndex(
        MakeSharedRange(std::move(rows), std::move(buffer)),
        nameTable,
        wireTypes,
        anyConverters);

    const auto columns = schema->Columns();
    const int dataIdx = idToColumnIndex[nameTable->GetIdOrThrow("data")];
    EXPECT_EQ(columns[dataIdx].GetWireType(), EValueType::Any);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStaticTableSourceTest, InferTableSchemaV1Types)
{
    auto nameTable = New<NTableClient::TNameTable>();
    nameTable->RegisterNameOrThrow("str_col");
    nameTable->RegisterNameOrThrow("int_col");

    auto tableSchema = New<NTableClient::TTableSchema>(std::vector<NTableClient::TColumnSchema>{
        NTableClient::TColumnSchema("str_col", EValueType::String),
        NTableClient::TColumnSchema("int_col", EValueType::Int64),
    });

    std::vector<EValueType> wireTypes(nameTable->GetSize(), EValueType::Min);
    const auto converters = TSource::InferTableSchema(nameTable, tableSchema, wireTypes);

    EXPECT_EQ(wireTypes[nameTable->GetIdOrThrow("str_col")], EValueType::String);
    EXPECT_EQ(wireTypes[nameTable->GetIdOrThrow("int_col")], EValueType::Int64);
    EXPECT_TRUE(converters.empty());
}

TEST(TStaticTableSourceTest, InferTableSchemaWithComplexType)
{
    auto nameTable = New<NTableClient::TNameTable>();
    nameTable->RegisterNameOrThrow("str_col");
    nameTable->RegisterNameOrThrow("struct_col");

    auto structType = StructLogicalType(
        {TStructField{.Name = "x", .StableName = "x", .Type = SimpleLogicalType(ESimpleLogicalValueType::String)}},
        /*removedFieldStableNames*/ {});
    auto tableSchema = New<NTableClient::TTableSchema>(std::vector<NTableClient::TColumnSchema>{
        NTableClient::TColumnSchema("str_col", EValueType::String),
        NTableClient::TColumnSchema("struct_col", structType),
    });

    std::vector<EValueType> wireTypes(nameTable->GetSize(), EValueType::Min);
    const auto converters = TSource::InferTableSchema(nameTable, tableSchema, wireTypes);

    EXPECT_EQ(wireTypes[nameTable->GetIdOrThrow("str_col")], EValueType::String);
    EXPECT_EQ(wireTypes[nameTable->GetIdOrThrow("struct_col")], EValueType::Any);
    EXPECT_FALSE(converters.contains(nameTable->GetIdOrThrow("str_col")));
    EXPECT_TRUE(converters.contains(nameTable->GetIdOrThrow("struct_col")));
}

TEST(TStaticTableSourceTest, InferTableSchemaV1AnyType)
{
    // V1 any columns must produce a converter entry so that
    // String-typed physical cells are normalized to Any.
    auto nameTable = New<NTableClient::TNameTable>();
    nameTable->RegisterNameOrThrow("str_col");
    nameTable->RegisterNameOrThrow("any_col");

    auto tableSchema = New<NTableClient::TTableSchema>(std::vector<NTableClient::TColumnSchema>{
        NTableClient::TColumnSchema("str_col", EValueType::String),
        NTableClient::TColumnSchema("any_col", EValueType::Any),
    });

    std::vector<EValueType> wireTypes(nameTable->GetSize(), EValueType::Min);
    const auto converters = TSource::InferTableSchema(nameTable, tableSchema, wireTypes);

    EXPECT_EQ(wireTypes[nameTable->GetIdOrThrow("str_col")], EValueType::String);
    EXPECT_EQ(wireTypes[nameTable->GetIdOrThrow("any_col")], EValueType::Any);
    EXPECT_FALSE(converters.contains(nameTable->GetIdOrThrow("str_col")));

    // Exercise the converter InferTableSchema actually produced:
    // a String cell with raw YSON bytes must be emitted via OnRaw, not double-encoded as a string scalar.
    const auto* converter = converters.FindPtr(nameTable->GetIdOrThrow("any_col"));
    ASSERT_NE(converter, nullptr);

    TString ysonBytes = "{key=value}";
    TUnversionedValue stringCell{};
    stringCell.Id = nameTable->GetIdOrThrow("any_col");
    stringCell.Type = EValueType::String;
    stringCell.Data.String = ysonBytes.data();
    stringCell.Length = ysonBytes.size();

    TString buffer;
    auto result = TSource::ConvertCellToAny(stringCell, converter, buffer);
    EXPECT_EQ(result.Type, EValueType::Any);
    auto node = NYT::NYTree::ConvertToNode(NYson::TYsonString(result.AsStringBuf()));
    ASSERT_EQ(node->GetType(), NYT::NYTree::ENodeType::Map);
    EXPECT_EQ(node->AsMap()->GetChildValueOrThrow<TString>("key"), "value");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStaticTableSourceTest, GetDesiredRowsPerSecond)
{
    auto distributingTable = New<TSourceControllerTable>();
    distributingTable->RowCount = 10;
    distributingTable->ByteSize = 100;

    auto sourceParameters = New<TDynamicTableSourceParameters>();
    sourceParameters->DesiredTableProcessTime = TDuration::Seconds(2);
    EXPECT_DOUBLE_EQ(TSourceController::GetDesiredRowsPerSecond(sourceParameters, distributingTable), 5.0);

    sourceParameters->MaxRowsPerSecond = 4.0;
    EXPECT_DOUBLE_EQ(TSourceController::GetDesiredRowsPerSecond(sourceParameters, distributingTable), 4.0);

    sourceParameters->MaxBytesPerSecond = 30.0;
    EXPECT_DOUBLE_EQ(TSourceController::GetDesiredRowsPerSecond(sourceParameters, distributingTable), 3.0);
}

TEST(TStaticTableSourceTest, GetDesiredRangeRowsPerSecond)
{
    auto distributingTable = New<TSourceControllerTable>();
    distributingTable->RowCount = 10;
    distributingTable->ByteSize = 100;

    auto sourceParameters = New<TDynamicTableSourceParameters>();
    sourceParameters->DesiredPartitionRowsPerSecond = 10;
    sourceParameters->DesiredPartitionBytesPerSecond = 1000;
    EXPECT_DOUBLE_EQ(TSourceController::GetDesiredRangeRowsPerSecond(sourceParameters, distributingTable), 10.0);

    sourceParameters->DesiredPartitionBytesPerSecond = 50;
    EXPECT_DOUBLE_EQ(TSourceController::GetDesiredRangeRowsPerSecond(sourceParameters, distributingTable), 5.0);

    distributingTable->ByteSize = 50;
    EXPECT_DOUBLE_EQ(TSourceController::GetDesiredRangeRowsPerSecond(sourceParameters, distributingTable), 10.0);
}

TEST(TStaticTableSourceTest, ExtractTimestamp)
{
    {
        // clang-format off
        auto node = NYT::NYTree::BuildYsonNodeFluently()
            .BeginAttributes()
                .Item("test_timestamp").Value("2021-01-01T00:00:00Z")
            .EndAttributes()
            .Entity();
        // clang-format on

        auto locator = New<TTableTimestampLocatorSpec>();
        locator->Attribute = "test_timestamp";
        locator->Format = ETimestampFormat::Iso8601;

        auto result = TSourceController::ExtractTimestamp(node, locator);
        EXPECT_EQ(result.Underlying(), 1609459200u);
    }

    {
        // clang-format off
        auto node = NYT::NYTree::BuildYsonNodeFluently()
            .BeginAttributes()
                .Item("test_timestamp").Value("invalid-timestamp")
            .EndAttributes()
            .Entity();
        // clang-format on

        auto locator = New<TTableTimestampLocatorSpec>();
        locator->Attribute = "test_timestamp";
        locator->Format = ETimestampFormat::Iso8601;

        EXPECT_THROW(
            TSourceController::ExtractTimestamp(node, locator),
            NYT::TErrorException);
    }

    {
        // clang-format off
        auto node = NYT::NYTree::BuildYsonNodeFluently()
            .BeginAttributes()
                .Item("test_timestamp").Value(42)
            .EndAttributes()
            .Entity();
        // clang-format on

        auto locator = New<TTableTimestampLocatorSpec>();
        locator->Attribute = "test_timestamp";
        locator->Format = ETimestampFormat::Iso8601;

        EXPECT_THROW(
            TSourceController::ExtractTimestamp(node, locator),
            NYT::TErrorException);
    }

    {
        // clang-format off
        auto node = NYT::NYTree::BuildYsonNodeFluently()
            .BeginAttributes()
                .Item("test_timestamp").Value(1609459200u)
            .EndAttributes()
            .Entity();
        // clang-format on

        auto locator = New<TTableTimestampLocatorSpec>();
        locator->Attribute = "test_timestamp";
        locator->Format = ETimestampFormat::Seconds;

        auto result = TSourceController::ExtractTimestamp(node, locator);
        EXPECT_EQ(result.Underlying(), 1609459200u);
    }

    {
        // clang-format off
        auto node = NYT::NYTree::BuildYsonNodeFluently()
            .BeginAttributes()
                .Item("test_timestamp").Value("2021-01-01T00:00:00Z")
            .EndAttributes()
            .Entity();
        // clang-format on

        auto locator = New<TTableTimestampLocatorSpec>();
        locator->Attribute = "test_timestamp";
        locator->Format = ETimestampFormat::Seconds;

        EXPECT_THROW(
            TSourceController::ExtractTimestamp(node, locator),
            NYT::TErrorException);
    }

    {
        // clang-format off
        auto node = NYT::NYTree::BuildYsonNodeFluently()
            .BeginAttributes()
                .Item("test_timestamp").Value(1609459200000u)
            .EndAttributes()
            .Entity();
        // clang-format on

        auto locator = New<TTableTimestampLocatorSpec>();
        locator->Attribute = "test_timestamp";
        locator->Format = ETimestampFormat::MilliSeconds;

        auto result = TSourceController::ExtractTimestamp(node, locator);
        EXPECT_EQ(result.Underlying(), 1609459200u);
    }

    {
        // clang-format off
        auto node = NYT::NYTree::BuildYsonNodeFluently()
            .BeginAttributes()
                .Item("test_timestamp").Value("2021-01-01T00:00:00Z")
            .EndAttributes()
            .Entity();
        // clang-format on

        auto locator = New<TTableTimestampLocatorSpec>();
        locator->Attribute = "test_timestamp";
        locator->Format = ETimestampFormat::MilliSeconds;

        EXPECT_THROW(
            TSourceController::ExtractTimestamp(node, locator),
            NYT::TErrorException);
    }
}

TEST(TStaticTableSourceTest, CreateThrottlerConfig)
{
    // Verify that throttler with rowsPerSecond * throttlerPeriod < 1 allows forward progress.
    {
        double rowsPerSecond = 0.5;                               // 0.5 rows per second = 1 row per 2 seconds.
        TDuration throttlerPeriod = TDuration::MilliSeconds(100); // 0.5 * 0.1 = 0.05 messages per period.

        auto config = TSource::CreateThrottlerConfig(rowsPerSecond, throttlerPeriod);
        auto throttler = NConcurrency::CreateReconfigurableThroughputThrottler(config);
        EXPECT_GE(throttler->TryAcquireAvailable(1), 1);
    }

    // Normal case where rowsPerSecond * throttlerPeriod >= 1.
    {
        double rowsPerSecond = 10.0;
        TDuration throttlerPeriod = TDuration::MilliSeconds(200);

        auto config = TSource::CreateThrottlerConfig(rowsPerSecond, throttlerPeriod);
        auto throttler = NConcurrency::CreateReconfigurableThroughputThrottler(config);

        // Should be able to acquire messages.
        i64 acquired = throttler->TryAcquireAvailable(5);
        EXPECT_GT(acquired, 0);
        EXPECT_LT(acquired, 5);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStaticTableSourceTest, ClassifyPendingRead)
{
    const auto readTimeout = TDuration::Minutes(5);

    // Not ready yet: the operation is simply not complete.
    {
        auto pendingRead = NewPromise<void>();
        EXPECT_TRUE(TSource::ClassifyPendingRead(pendingRead.ToFuture(), TDuration::Minutes(1), readTimeout).IsOK());
    }

    // A dead read stream surfaces only here, well before the timeout.
    {
        auto pendingRead = MakeFuture(NYT::TError("Chunk is unavailable"));
        auto error = TSource::ClassifyPendingRead(pendingRead, TDuration::Seconds(1), readTimeout);
        ASSERT_FALSE(error.IsOK());
        EXPECT_THAT(ToString(error), testing::HasSubstr("Chunk is unavailable"));
    }

    // A silently stalled operation is caught by the timeout, which is armed from reader creation
    // rather than from the first non-empty batch.
    {
        auto pendingRead = NewPromise<void>();
        auto error = TSource::ClassifyPendingRead(pendingRead.ToFuture(), readTimeout + TDuration::Seconds(1), readTimeout);
        ASSERT_FALSE(error.IsOK());
        EXPECT_TRUE(error.FindMatching(NYT::EErrorCode::Timeout).has_value());
    }

    // A reader with rows at hand is never dropped, however long the previous wait was.
    {
        auto pendingRead = MakeFuture(NYT::TError());
        EXPECT_TRUE(TSource::ClassifyPendingRead(pendingRead, TDuration::Zero(), readTimeout).IsOK());
        EXPECT_TRUE(TSource::ClassifyPendingRead(pendingRead, readTimeout + TDuration::Seconds(1), readTimeout).IsOK());
    }
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPendingRead,
    (ReaderCreation)
    (ReadyEvent)
);

class TStaticTableSourceReaderCancelTest
    : public TStaticTableSourceReaderTest
    , public ::testing::WithParamInterface<EPendingRead>
{
protected:
    // Kept alive: an abandoned promise resolves its future with a cancelation error on its own.
    TPromise<NApi::ITableReaderPtr> PendingReaderCreation_;
    TPromise<void> PendingReadyEvent_;

    // Leaves either the reader creation or the ready event of a created reader in flight.
    // Returns the future to watch for cancellation and the reader to feed the source with.
    std::pair<TFuture<void>, TFuture<NApi::ITableReaderPtr>> MakePendingRead()
    {
        if (GetParam() == EPendingRead::ReaderCreation) {
            PendingReaderCreation_ = NewPromise<NApi::ITableReaderPtr>();
            return {PendingReaderCreation_.ToFuture().AsVoid(), PendingReaderCreation_.ToFuture()};
        }
        PendingReadyEvent_ = NewPromise<void>();
        return {PendingReadyEvent_.ToFuture(), MakeFuture(MakeTableReader(/*values*/ {}, PendingReadyEvent_.ToFuture()))};
    }
};

TEST_P(TStaticTableSourceReaderCancelTest, CancelsOnReadTimeout)
{
    auto [pendingRead, reader] = MakePendingRead();
    auto source = MakeSource(MakeClient({reader}), /*readTimeout*/ TDuration::Zero());

    PollUntilCanceled(source, pendingRead);

    EXPECT_TRUE(IsCanceled(pendingRead));
}

TEST_P(TStaticTableSourceReaderCancelTest, CancelsOnTrim)
{
    auto [pendingRead, reader] = MakePendingRead();
    const auto readTimeout = TDuration::Minutes(1);
    auto source = MakeSource(MakeClient({reader, MakeFuture(MakeTableReader({2}, OKFuture))}), readTimeout);
    EXPECT_TRUE(ReadRows(source).empty());

    TrimSource(source, readTimeout, /*lowerRowIndex*/ 2);

    EXPECT_EQ(ReadRows(source), (std::vector<std::pair<i64, i64>>{{2, 2}}));
    EXPECT_TRUE(IsCanceled(pendingRead));
    EXPECT_EQ(ReaderStarts_, (std::vector<i64>{0, 2}));
}

TEST_P(TStaticTableSourceReaderCancelTest, CancelsOnTrimToEnd)
{
    auto [pendingRead, reader] = MakePendingRead();
    const auto readTimeout = TDuration::Minutes(1);
    auto source = MakeSource(MakeClient({reader}), readTimeout);
    EXPECT_TRUE(ReadRows(source).empty());

    TrimSource(source, readTimeout, /*lowerRowIndex*/ 3);
    CommitSource(source);

    EXPECT_TRUE(ReadRows(source).empty());
    EXPECT_TRUE(IsCanceled(pendingRead));
}

TEST_P(TStaticTableSourceReaderCancelTest, CancelsOnTerminate)
{
    auto [pendingRead, reader] = MakePendingRead();
    auto source = MakeSource(MakeClient({reader}), /*readTimeout*/ TDuration::Minutes(1));
    EXPECT_TRUE(ReadRows(source).empty());

    source->Terminate();
    // Termination is processed in the serialized invoker.
    RunInInvoker([] {
    });

    EXPECT_TRUE(IsCanceled(pendingRead));
}

INSTANTIATE_TEST_SUITE_P(
    , TStaticTableSourceReaderCancelTest,
    ::testing::ValuesIn(TEnumTraits<EPendingRead>::GetDomainValues()),
    [] (const testing::TestParamInfo<EPendingRead>& info) -> std::string {
        return ToString(info.param);
    });

TEST_F(TStaticTableSourceReaderTest, DoesNotCreateReaderOverTrimmedRange)
{
    const auto readTimeout = TDuration::Minutes(1);
    auto source = MakeSource(MakeClient({MakeFuture(MakeTableReader({0}, OKFuture))}), readTimeout);
    EXPECT_EQ(ReadRows(source), (std::vector<std::pair<i64, i64>>{{0, 0}}));

    // Skipping the remaining rows empties the range while the source still points at the old offset.
    TrimSource(source, readTimeout, /*lowerRowIndex*/ 3);

    EXPECT_TRUE(ReadRows(source).empty());
}

TEST_F(TStaticTableSourceReaderTest, ClearsReadErrorOnFullyReadRange)
{
    const auto readTimeout = TDuration::Minutes(1);
    auto source = MakeSource(
        MakeClient({MakeFuture(MakeTableReader({0}, MakeFuture<void>(TError("Read failed"))))}),
        readTimeout);
    EXPECT_EQ(ReadRows(source), (std::vector<std::pair<i64, i64>>{{0, 0}}));
    EXPECT_TRUE(ReadRows(source).empty());
    EXPECT_TRUE(HasReadError());

    TrimSource(source, readTimeout, /*lowerRowIndex*/ 3);

    EXPECT_TRUE(ReadRows(source).empty());
    EXPECT_FALSE(HasReadError());
}

TEST_F(TStaticTableSourceReaderTest, RecreatesReaderAtNextUnreadRow)
{
    auto source = MakeSource(
        MakeClient({
            MakeFuture(MakeTableReader({0}, MakeFuture<void>(TError("Read failed")))),
            MakeFuture(MakeTableReader({1, 2}, OKFuture)),
        }),
        /*readTimeout*/ TDuration::Minutes(1));

    std::vector<std::pair<i64, i64>> rows;
    auto appendRows = [&] {
        auto nextRows = ReadRows(source);
        rows.insert(rows.end(), nextRows.begin(), nextRows.end());
    };
    appendRows();
    EXPECT_TRUE(ReadRows(source).empty());
    appendRows();

    EXPECT_EQ(ReaderStarts_, (std::vector<i64>{0, 1}));
    EXPECT_EQ(rows, (std::vector<std::pair<i64, i64>>{{0, 0}, {1, 1}, {2, 2}}));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStaticTableSourceTest, ConvertCellToAny)
{
    // Build a struct<data: string> column and create a positional->named converter for it.
    auto structType = StructLogicalType(
        {TStructField{.Name = "data", .StableName = "data", .Type = SimpleLogicalType(ESimpleLogicalValueType::String)}},
        /*removedFieldStableNames*/ {});
    auto col = TColumnSchema("field", structType);
    TYsonConverterConfig config;
    auto converter = CreateYsonServerToClientConverter(TComplexTypeFieldDescriptor(col), config);

    // Composite cell with positional YSON ["hello"].
    TString positionalYson = "[\"hello\"]";
    TUnversionedValue cell{};
    cell.Id = 0;
    cell.Type = EValueType::Composite;
    cell.Data.String = positionalYson.data();
    cell.Length = positionalYson.size();

    // With V2 converter: should produce named-format map YSON {data="hello"}.
    {
        TString buffer;
        auto result = TSource::ConvertCellToAny(cell, &converter, buffer);
        EXPECT_EQ(result.Type, EValueType::Any);
        auto node = NYT::NYTree::ConvertToNode(NYson::TYsonString(result.AsStringBuf()));
        ASSERT_EQ(node->GetType(), NYT::NYTree::ENodeType::Map);
        EXPECT_EQ(node->AsMap()->GetChildValueOrThrow<TString>("data"), "hello");
    }

    // Without converter (schema-less Composite): should keep bytes as-is, only changing type to Any.
    {
        TString buffer;
        auto result = TSource::ConvertCellToAny(cell, nullptr, buffer);
        EXPECT_EQ(result.Type, EValueType::Any);
        EXPECT_EQ(result.AsStringBuf(), positionalYson);
    }

    // Null cell: must be preserved as-is regardless of the converter.
    {
        TUnversionedValue nullCell{};
        nullCell.Id = 0;
        nullCell.Type = EValueType::Null;
        TString buffer;
        auto result = TSource::ConvertCellToAny(nullCell, &converter, buffer);
        EXPECT_EQ(result.Type, EValueType::Null);
    }

    // V1 any column converter: String cell containing raw YSON bytes must be treated as raw YSON (OnRaw).
    {
        auto anyConverter = TSource::MakeAnyColumnConverter();

        TString ysonBytes = "{data=hello}";
        TUnversionedValue stringCell{};
        stringCell.Id = 0;
        stringCell.Type = EValueType::String;
        stringCell.Data.String = ysonBytes.data();
        stringCell.Length = ysonBytes.size();

        TString buffer;
        auto result = TSource::ConvertCellToAny(stringCell, &anyConverter, buffer);
        EXPECT_EQ(result.Type, EValueType::Any);
        auto node = NYT::NYTree::ConvertToNode(NYson::TYsonString(result.AsStringBuf()));
        ASSERT_EQ(node->GetType(), NYT::NYTree::ENodeType::Map);
        EXPECT_EQ(node->AsMap()->GetChildValueOrThrow<TString>("data"), "hello");
    }

    // V1 any column converter: Int64 cell must be serialized to YSON via UnversionedValueToYson.
    {
        auto anyConverter = TSource::MakeAnyColumnConverter();

        TUnversionedValue int64Cell{};
        int64Cell.Id = 0;
        int64Cell.Type = EValueType::Int64;
        int64Cell.Data.Int64 = 42;

        TString buffer;
        auto result = TSource::ConvertCellToAny(int64Cell, &anyConverter, buffer);
        EXPECT_EQ(result.Type, EValueType::Any);
        auto node = NYT::NYTree::ConvertToNode(NYson::TYsonString(result.AsStringBuf()));
        ASSERT_EQ(node->GetType(), NYT::NYTree::ENodeType::Int64);
        EXPECT_EQ(node->AsInt64()->GetValue(), 42);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NStaticTableConnector
