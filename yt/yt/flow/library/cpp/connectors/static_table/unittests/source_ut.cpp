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

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/yson/string.h>
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
using ::testing::ElementsAre;
using ::testing::NiceMock;
using ::testing::Return;
using ::testing::UnorderedElementsAre;

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

    EXPECT_CALL(*reader, GetReadyEvent()).WillRepeatedly([=] {
        return rows->empty() ? readyAfterRows : OKFuture;
    });
    EXPECT_CALL(*reader, Read(_)).WillRepeatedly([=] (const TRowBatchReadOptions& options) -> IUnversionedRowBatchPtr {
        if (rows->empty()) {
            return nullptr;
        }
        auto count = std::min<i64>(options.MaxRowsPerRead, std::ssize(*rows));
        auto batch = MakeRowBatch(std::vector<i64>(rows->begin(), rows->begin() + count), valueId);
        rows->erase(rows->begin(), rows->begin() + count);
        return batch;
    });

    return reader;
}

class TSourceReaderTimeProvider
    : public TFakeTimeProvider
{
public:
    TFuture<TGlobalUniqueSeqNo> GenerateGlobalUniqueSeqNo() const override
    {
        auto result = TFakeTimeProvider::GenerateGlobalUniqueSeqNo().GetOrCrash().ValueOrThrow();
        result.UniqueSeqNo = TUniqueSeqNo(
            NTransactionClient::TimestampFromUnixTime(1'790'000'000).Underlying() + result.UniqueSeqNo.Underlying());
        return MakeFuture(result);
    }
};

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

    TDynamicSourceContextPtr MakeDynamicContext(TDuration readTimeout, i64 lowerRowIndex, std::optional<TSystemTimestamp> plannedTimestamp = {})
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
                .Item("planned_timestamp").Value(plannedTimestamp.value_or(TSystemTimestamp(101)))
                .Item("use_planned_timestamps").Value(plannedTimestamp.has_value())
            .EndMap()
            ->AsMap();
        // clang-format on

        if (plannedTimestamp) {
            dynamicPartitionSpec->RemoveChild("system_timestamp");
            dynamicPartitionSpec->AddChild("system_timestamp", ConvertToNode(*plannedTimestamp));
            dynamicPartitionSpec->RemoveChild("event_timestamp");
            dynamicPartitionSpec->AddChild("event_timestamp", ConvertToNode(*plannedTimestamp));
        }
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
            .WillRepeatedly([this, pendingReaders] (const TRichYPath& path, const NApi::TTableReaderOptions&) {
                ReaderStarts_.push_back(GetRowIndexRange(path).first);
                auto reader = pendingReaders->front();
                pendingReaders->pop_front();
                return reader;
            });
        return client;
    }

    TIntrusivePtr<TSource> MakeSource(const NApi::IClientPtr& client, TDuration readTimeout, std::optional<TSystemTimestamp> plannedTimestamp = {})
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
        context->TimeProvider = New<TSourceReaderTimeProvider>();

        auto source = New<TSource>(std::move(context), MakeDynamicContext(readTimeout, 0, plannedTimestamp));
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

TLegacyReadRanges GetReadRanges(const TSourceControllerTable& table)
{
    TLegacyReadRanges result;
    for (const auto& [rangeId, range] : table.ReadState->Ranges) {
        result.emplace(rangeId, std::pair(range.BeginRowIndex, range.EndRowIndex));
    }
    return result;
}

void SetDistributedRows(const TSourceControllerTablePtr& table, i64 distributedRows)
{
    table->ReadState->NextRowIndex = distributedRows;
    table->LegacyDistributedRows = distributedRows;
}

void SetDistributingRanges(const TSourceControllerTablePtr& table, const TLegacyReadRanges& ranges)
{
    table->ReadState->Ranges.clear();
    table->ReadState->FreeSlotTimestamps.clear();
    table->LegacyDistributingRanges = ranges;
    for (const auto& [rangeId, bounds] : ranges) {
        TReadRange range;
        range.BeginRowIndex = bounds.first;
        range.EndRowIndex = bounds.second;
        table->ReadState->Ranges.emplace(rangeId, range);
    }
}

void SetDistributingRange(const TSourceControllerTablePtr& table, TRangeId rangeId, std::pair<i64, i64> bounds)
{
    auto ranges = GetReadRanges(*table);
    ranges[rangeId] = bounds;
    SetDistributingRanges(table, ranges);
}

TSourceControllerTablePtr MakeTable(
    const char* cluster,
    const char* name,
    const char* objectId,
    ui64 eventTimestamp,
    i64 rowCount = 10,
    i64 era = 1)
{
    auto table = New<TSourceControllerTable>();
    table->Path = TRichYPath(objectId);
    table->Path.SetCluster(cluster);
    table->Path.Attributes().Set("original_path", TString("//dir/") + name);
    table->EventTimestamp = TSystemTimestamp(eventTimestamp);
    table->SystemTimestamp = TSystemTimestamp(eventTimestamp);
    table->RowCount = rowCount;
    table->Era = era;
    return table;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStaticTableMigrationTest, DetectsStateOrigin)
{
    auto freshNativeV2 = New<TSourceControllerState>();
    TSourceController::InitializeMigrationState(freshNativeV2.Get(), true);
    EXPECT_EQ(freshNativeV2->Mode, EMigrationMode::V2);

    auto freshLegacy = New<TSourceControllerState>();
    TSourceController::InitializeMigrationState(freshLegacy.Get(), false);
    EXPECT_EQ(freshLegacy->Mode, EMigrationMode::V1);

    auto progressedV1 = New<TSourceControllerState>();
    progressedV1->Inited = true;
    progressedV1->DistributingTable = MakeTable("cluster", "v1", "#v1", 100);
    TSourceController::InitializeMigrationState(progressedV1.Get(), true);
    EXPECT_EQ(progressedV1->Mode, EMigrationMode::V1);

    auto existingV2 = New<TSourceControllerState>();
    existingV2->EventNameOrder = New<TEventNameOrder>();
    existingV2->ClusterProgress = New<TClusterProgress>();
    TSourceController::InitializeMigrationState(existingV2.Get(), false);
    EXPECT_EQ(existingV2->Mode, EMigrationMode::V2);

    auto persisted = New<TSourceControllerState>();
    persisted->Mode = EMigrationMode::Draining;
    persisted->CutoverEra = 7;
    persisted->CutoverEventTimestamp = TSystemTimestamp(100);
    TSourceController::InitializeMigrationState(persisted.Get(), false);
    EXPECT_EQ(persisted->Mode, EMigrationMode::Draining);
    EXPECT_EQ(persisted->CutoverEra, 7);
    EXPECT_EQ(persisted->CutoverEventTimestamp, TSystemTimestamp(100));

    auto persistedV2 = New<TSourceControllerState>();
    persistedV2->Mode = EMigrationMode::V2;
    TSourceController::InitializeMigrationState(persistedV2.Get(), false);
    EXPECT_EQ(persistedV2->Mode, EMigrationMode::V2);
}

TEST(TStaticTableMigrationTest, LatchesAndFinishesDraining)
{
    auto state = New<TSourceControllerState>();
    state->Mode = EMigrationMode::V1;
    state->Inited = true;
    state->Era = 3;
    state->DistributingTable = MakeTable("cluster", "current", "#20", 100, 10, 3);
    state->DistributingTable->SystemTimestamp = TSystemTimestamp(20);

    auto later = MakeTable("cluster", "later", "#30", 100, 10, 3);
    later->SystemTimestamp = TSystemTimestamp(30);
    auto earlier = MakeTable("cluster", "earlier", "#10", 100, 10, 3);
    earlier->SystemTimestamp = TSystemTimestamp(10);
    auto greater = MakeTable("cluster", "greater", "#01", 200, 10, 3);

    TSourceController::UpdateMigrationState(
        state.Get(),
        false,
        {earlier, state->DistributingTable, later, greater},
        NLogging::TLogger());
    EXPECT_EQ(state->Mode, EMigrationMode::V1);

    TSourceController::UpdateMigrationState(
        state.Get(),
        true,
        {earlier, state->DistributingTable, later, greater},
        NLogging::TLogger());
    EXPECT_EQ(state->Mode, EMigrationMode::Draining);
    EXPECT_EQ(state->CutoverEra, 3);
    EXPECT_EQ(state->CutoverEventTimestamp, TSystemTimestamp(100));
    EXPECT_THAT(state->CutoverProcessedTableNames, UnorderedElementsAre("earlier", "current"));

    TSourceController::UpdateMigrationState(
        state.Get(),
        false,
        {earlier, state->DistributingTable, later, greater},
        NLogging::TLogger());
    EXPECT_EQ(state->Mode, EMigrationMode::Draining);

    state->DistributionFinished = true;
    TSourceController::UpdateMigrationState(
        state.Get(),
        false,
        {earlier, state->DistributingTable, later, greater},
        NLogging::TLogger());
    EXPECT_EQ(state->Mode, EMigrationMode::Draining);

    state->DistributingTable = later;
    TSourceController::UpdateMigrationState(
        state.Get(),
        false,
        {earlier, state->DistributingTable, greater},
        NLogging::TLogger());
    EXPECT_EQ(state->Mode, EMigrationMode::V2);
    EXPECT_EQ(state->CutoverEra, 3);
    EXPECT_EQ(state->CutoverEventTimestamp, TSystemTimestamp(100));
    EXPECT_THAT(state->CutoverProcessedTableNames, UnorderedElementsAre("earlier", "current", "later"));

    auto late = MakeTable("cluster", "late", "#40", 100, 10, 3);
    late->SystemTimestamp = TSystemTimestamp(40);
    auto listed = std::vector<TSourceControllerTablePtr>{earlier, state->DistributingTable, late, greater};
    TSourceController::FilterTables(
        listed,
        New<TDynamicTableSourceParameters>(),
        state->DistributingTable,
        state->EventNameOrder,
        EMigrationMode::V2,
        state->CutoverEra,
        state->CutoverEventTimestamp,
        state->CutoverProcessedTableNames);
    TSourceController::AssignEventOrdinals(listed, state.Get());
    TSourceController::SortTables(listed, EMigrationMode::V2);
    ASSERT_THAT(listed, ElementsAre(late, greater));
    EXPECT_GT(late->EventOrdinal, state->DistributingTable->EventOrdinal);
}

TEST(TStaticTableMigrationTest, EnablesV2DirectlyBeforeFirstTable)
{
    auto state = New<TSourceControllerState>();
    state->Mode = EMigrationMode::V1;

    TSourceController::UpdateMigrationState(state.Get(), true, {}, NLogging::TLogger());

    EXPECT_EQ(state->Mode, EMigrationMode::V2);
    EXPECT_FALSE(state->CutoverEra.has_value());
    EXPECT_FALSE(state->CutoverEventTimestamp.has_value());
}

TEST(TStaticTableMigrationTest, V1OrderingMatchesSourceV1)
{
    auto current = MakeTable("cluster", "current", "#20", 100);
    current->SystemTimestamp = TSystemTimestamp(20);
    auto behind = MakeTable("cluster", "z-behind", "#10", 100);
    behind->SystemTimestamp = TSystemTimestamp(10);
    auto later = MakeTable("cluster", "a-later", "#30", 100);
    later->SystemTimestamp = TSystemTimestamp(30);
    auto greater = MakeTable("cluster", "greater", "#01", 200);

    std::vector<TSourceControllerTablePtr> tables{greater, later, behind, current};
    TSourceController::FilterTables(
        tables,
        New<TDynamicTableSourceParameters>(),
        current,
        {},
        EMigrationMode::Draining);
    TSourceController::SortTables(tables, EMigrationMode::Draining);

    ASSERT_EQ(std::ssize(tables), 4);
    EXPECT_EQ(tables[0]->Path.GetPath(), "#10");
    EXPECT_EQ(tables[1]->Path.GetPath(), "#20");
    EXPECT_EQ(tables[2]->Path.GetPath(), "#30");
    EXPECT_EQ(tables[3]->EventTimestamp, TSystemTimestamp(200));
}

TEST(TStaticTableMigrationTest, V1ObjectIdOrderCanOpposeV2NameOrder)
{
    auto firstById = MakeTable("cluster", "z-name", "#10", 100);
    auto firstByName = MakeTable("cluster", "a-name", "#20", 100);
    std::vector<TSourceControllerTablePtr> v1{firstByName, firstById};
    TSourceController::SortTables(v1, EMigrationMode::V1);
    EXPECT_EQ(v1[0]->GetName(), "z-name");

    auto state = New<TSourceControllerState>();
    std::vector<TSourceControllerTablePtr> v2{firstById, firstByName};
    TSourceController::AssignEventOrdinals(v2, state.Get());
    TSourceController::SortTables(v2, EMigrationMode::V2);
    EXPECT_EQ(v2[0]->GetName(), "a-name");
}

TEST(TStaticTableMigrationTest, CutoverExclusionIsEraScoped)
{
    auto current = New<TSourceControllerTable>();
    current->Era = 1;
    auto cutoverEraTable = MakeTable("cluster", "old", "#10", 100, 10, 1);
    auto older = MakeTable("cluster", "older", "#05", 90, 10, 1);
    auto late = MakeTable("cluster", "late", "#15", 100, 10, 1);
    auto greater = MakeTable("cluster", "greater", "#20", 200, 10, 1);
    auto replay = MakeTable("cluster", "replay", "#30", 100, 10, 2);
    std::vector<TSourceControllerTablePtr> tables{older, cutoverEraTable, late, greater, replay};

    TSourceController::FilterTables(
        tables,
        New<TDynamicTableSourceParameters>(),
        current,
        {},
        EMigrationMode::V2,
        1,
        TSystemTimestamp(100),
        {"old"});

    ASSERT_THAT(tables, ElementsAre(late, greater, replay));
}

TEST(TStaticTableMigrationTest, RestartAdvancesEraOnce)
{
    auto state = New<TSourceControllerState>();
    state->Era = 4;
    state->EraStartInstant = TInstant::Seconds(10);
    state->DistributingTable = MakeTable("cluster", "current", "#10", 100);

    EXPECT_TRUE(TSourceController::ApplyRestartInstantLogic(
        state.Get(),
        TInstant::Seconds(20),
        NLogging::TLogger()));
    EXPECT_EQ(state->Era, 5);
    EXPECT_FALSE(TSourceController::ApplyRestartInstantLogic(
        state.Get(),
        TInstant::Seconds(20),
        NLogging::TLogger()));
}

TEST(TStaticTableMigrationTest, FutureRestartDoesNotAdvanceEra)
{
    auto state = New<TSourceControllerState>();
    state->Era = 4;
    state->EraStartInstant = TInstant::Seconds(10);
    state->DistributingTable = MakeTable("cluster", "current", "#10", 100);

    EXPECT_FALSE(TSourceController::ApplyRestartInstantLogic(
        state.Get(),
        TInstant::Now() + TDuration::Hours(1),
        NLogging::TLogger()));
    EXPECT_EQ(state->Era, 4);
    EXPECT_EQ(state->EraStartInstant, TInstant::Seconds(10));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStaticTableSourceTest, DoDistributingSimple)
{

    auto distributingTable = New<TSourceControllerTable>();
    distributingTable->Path = "primary://home/t1";
    distributingTable->EventTimestamp = TSystemTimestamp(101);
    distributingTable->SystemTimestamp = TSystemTimestamp(102);
    distributingTable->RowCount = 30000;
    SetDistributedRows(distributingTable, 10000);
    SetDistributingRanges(distributingTable, {
            {TRangeId(41000), {8000, 9000}},
            {TRangeId(41001), {9000, 10000}},
                                             });
    auto originalDistributingRanges = GetReadRanges(*distributingTable);

    auto sourceParameters = New<TDynamicTableSourceParameters>();
    sourceParameters->DesiredTableProcessTime = TDuration::Seconds(10);
    sourceParameters->DesiredPartitionProcessTime = TDuration::Seconds(2);

    TSourceController::DoDistributing(
        sourceParameters,
        /*desiredRangeRowsPerSecond*/ 1000,
        /*committedOffsetsExclusive*/ {},
        distributingTable,
        MakeRangeIdGenerator(42000),
        /*usePlannedTimestamps*/ false,
        TSystemTimestamp(1000));

    EXPECT_EQ(GetReadRanges(*distributingTable)[TRangeId(41000)], originalDistributingRanges[TRangeId(41000)]);
    EXPECT_EQ(GetReadRanges(*distributingTable)[TRangeId(41001)], originalDistributingRanges[TRangeId(41001)]);

    ASSERT_EQ(GetReadRanges(*distributingTable).size(), 3u);
    EXPECT_EQ(GetReadRanges(*distributingTable).at(TRangeId(42000)), (std::pair<i64, i64>(10000, 12000)));
    EXPECT_EQ(distributingTable->ReadState->NextRowIndex, 12000);
}

TEST(TStaticTableSourceTest, ParallelRangesSharePlannedStart)
{
    auto table = MakeTable("cluster", "current", "#10", 100, 6000);
    table->ByteSize = 6000;
    auto parameters = New<TDynamicTableSourceParameters>();
    parameters->DesiredTableProcessTime = TDuration::Seconds(1200);
    parameters->DesiredPartitionRowsPerSecond = 1;
    parameters->DesiredPartitionProcessTime = TDuration::Minutes(10);
    auto specs = TSourceController::DoDistributing(
        parameters,
        1,
        {},
        table,
        MakeRangeIdGenerator(1),
        /*usePlannedTimestamps*/ true,
        TSystemTimestamp(1000));
    ASSERT_EQ(specs.size(), 5u);
    for (const auto& [rangeId, node] : specs) {
        auto spec = ConvertTo<TDynamicTableSourcePartitionSpecPtr>(node);
        EXPECT_EQ(spec->PlannedTimestamp, TSystemTimestamp(1000));
        EXPECT_EQ(spec->PlannedReadDuration, TDuration::Seconds(600));
    }
}

TEST(TStaticTableSourceTest, PlannedDurationRoundsUpToSeconds)
{
    auto table = MakeTable("cluster", "current", "#10", 100, 6001);
    table->ByteSize = 6001;
    auto parameters = New<TDynamicTableSourceParameters>();
    parameters->DesiredTableProcessTime = TDuration::MilliSeconds(600100);
    parameters->DesiredPartitionRowsPerSecond = 10;
    parameters->DesiredPartitionProcessTime = TDuration::Hours(1);
    auto specs = TSourceController::DoDistributing(
        parameters,
        10,
        {},
        table,
        MakeRangeIdGenerator(1),
        /*usePlannedTimestamps*/ true,
        TSystemTimestamp(1000));
    ASSERT_EQ(specs.size(), 1u);
    auto spec = ConvertTo<TDynamicTableSourcePartitionSpecPtr>(GetOrCrash(specs, TRangeId(1)));
    EXPECT_EQ(spec->PlannedReadDuration, TDuration::Seconds(601));
}

TEST(TStaticTableSourceTest, ReadLimitsMustBePositive)
{
    for (const auto* name : {"max_rows_per_second", "max_bytes_per_second"}) {
        for (double limit : {0.0, -1.0}) {
            // clang-format off
            auto node = BuildYsonNodeFluently()
                .BeginMap()
                    .Item(name).Value(limit)
                .EndMap();
            // clang-format on
            EXPECT_THROW(ConvertTo<TDynamicTableSourceParametersPtr>(node), std::exception);
        }
        // clang-format off
        auto node = BuildYsonNodeFluently()
            .BeginMap()
                .Item(name).Value(1.0)
            .EndMap();
        // clang-format on
        EXPECT_NO_THROW(ConvertTo<TDynamicTableSourceParametersPtr>(node));
    }
}

class TReadScheduleTimeProvider
    : public TFakeTimeProvider
{
public:
    TSystemTimestamp Now = TSystemTimestamp(1000);

    TFuture<TSystemTimestamp> GetTimestamp(bool /*barrier*/) const override
    {
        return MakeFuture(Now);
    }
};

class TReadScheduleController
    : public TSourceController
{
public:
    using TSourceController::TSourceController;

    TListedTables GetTables(
        const TTableSourceParametersPtr& /*sourceSpec*/,
        const TDynamicTableSourceParametersPtr& /*dynamicSourceSpec*/,
        i64 /*era*/,
        const TSourceControllerTablePtr& lastProcessingTable,
        const TEventNameOrderPtr& /*nameOrder*/,
        EMigrationMode /*mode*/,
        std::optional<i64> /*cutoverEra*/,
        std::optional<TSystemTimestamp> /*cutoverEventTimestamp*/,
        std::vector<std::string> /*cutoverProcessedTableNames*/) override
    {
        return {.Tables = lastProcessingTable->RowCount > 0
                ? std::vector<TSourceControllerTablePtr>{CloneYsonStruct(lastProcessingTable)}
                : std::vector<TSourceControllerTablePtr>{}};
    }
};

class TReadScheduleTest
    : public ::testing::Test
{
protected:
    const TActionQueuePtr ActionQueue_ = New<TActionQueue>("ReadScheduleTest");
    const TIntrusivePtr<TReadScheduleTimeProvider> Clock_ = New<TReadScheduleTimeProvider>();
    TStateManagerMockPtr StateManager_;
    TIntrusivePtr<TReadScheduleController> Controller_;
    TDynamicTableSourceParametersPtr Parameters_ = New<TDynamicTableSourceParameters>();

    template <typename TFunctor>
    auto Run(TFunctor&& functor)
    {
        auto result = WaitFor(BIND(std::forward<TFunctor>(functor)).AsyncVia(ActionQueue_->GetInvoker()).Run());
        if constexpr (requires { result.ValueOrThrow(); }) {
            return result.ValueOrThrow();
        } else {
            result.ThrowOnError();
        }
    }

    void Restore(const TYsonString& state, bool usePlannedTimestamps = true)
    {
        Run([&] {
            Controller_.Reset();
            StateManager_ = New<TStateManagerMock>();
            StateManager_->Set("/v0", state);
            auto context = New<TSourceControllerContext>();
            context->SourceSpec = New<TSourceSpec>();
            context->SourceSpec->SourceClassName = TypeName<TSource>();
            // clang-format off
            context->SourceSpec->Parameters = BuildYsonNodeFluently()
                .BeginMap()
                    .Item("tables").BeginList()
                    .EndList()
                    .Item("finite").Value(true)
                    .Item("use_planned_timestamps").Value(usePlannedTimestamps)
                .EndMap()
                ->AsMap();
            // clang-format on
            context->TimeProvider = Clock_;
            context->StatusProfiler = CreateSyncStatusProfiler();
            auto dynamicContext = New<TDynamicSourceControllerContext>();
            dynamicContext->DynamicSourceSpec = New<TDynamicSourceSpec>();
            dynamicContext->DynamicSourceSpec->Parameters = ConvertTo<IMapNodePtr>(Parameters_);
            Controller_ = New<TReadScheduleController>(context, dynamicContext);
            Controller_->Init(StateManager_->CreateContext());
        });
    }

    void Restore(const TSourceControllerStatePtr& state, bool usePlannedTimestamps = true)
    {
        Restore(ConvertToYsonString(state), usePlannedTimestamps);
    }

    TSourceControllerStatePtr Save()
    {
        return Run([&] {
            Controller_->Sync();
            StateManager_->Sync();
            return ConvertTo<TSourceControllerStatePtr>(StateManager_->Get("/v0"));
        });
    }

    void Reconfigure()
    {
        Run([&] {
            auto context = New<TDynamicSourceControllerContext>();
            context->DynamicSourceSpec = New<TDynamicSourceSpec>();
            context->DynamicSourceSpec->Parameters = ConvertTo<IMapNodePtr>(Parameters_);
            static_cast<ISourceController*>(Controller_.Get())->Reconfigure(context);
        });
    }

    THashMap<TRangeId, TDynamicTableSourcePartitionSpecPtr> List()
    {
        return Run([&] {
            THashMap<TRangeId, TDynamicTableSourcePartitionSpecPtr> specs;
            auto keys = Controller_->ListKeys();
            YT_VERIFY(keys);
            for (const auto& [key, node] : *keys) {
                specs.emplace(FromUnversionedValue<TRangeId>(key.Underlying()[0]), ConvertTo<TDynamicTableSourcePartitionSpecPtr>(node));
            }
            return specs;
        });
    }

    void Report(TRangeId rangeId, EPartitionState state, i64 offset = -1)
    {
        Run([&] {
            auto status = New<TExtendedSourcePartitionStatus>();
            status->PartitionState = state;
            auto partitionStatus = New<TPartitionStatus>();
            partitionStatus->CommittedOffsetExclusive = offset;
            status->PartitionStatus = ConvertTo<IMapNodePtr>(partitionStatus);
            Controller_->ProcessPartitionStatuses({{MakeKey(rangeId.Underlying()), status}});
        });
    }

    TStreamTraverseDataPtr Traverse()
    {
        return Run([&] {
            return *Controller_->GetFutureKeysStreamTraverseData();
        });
    }

    void SetUp() override
    {
        Parameters_->DesiredTableProcessTime = TDuration::Seconds(1200);
        Parameters_->DesiredPartitionRowsPerSecond = 1;
        Parameters_->DesiredPartitionProcessTime = TDuration::Minutes(10);
        auto state = New<TSourceControllerState>();
        state->Inited = true;
        state->DistributingTable = MakeTable("cluster", "current", "#10", 100, 6000);
        state->DistributingTable->ByteSize = 6000;
        Restore(state);
    }

    void TearDown() override
    {
        ActionQueue_->Shutdown();
        Controller_.Reset();
    }
};

TEST_F(TReadScheduleTest, CompletedSlotSurvivesRestartAndDuplicateStatus)
{
    auto specs = List();
    ASSERT_EQ(specs.size(), 5u);
    auto completedRangeId = specs.begin()->first;
    Report(completedRangeId, EPartitionState::Completed);
    auto saved = Save();
    Clock_->Now = TSystemTimestamp(1720);
    Restore(saved);
    Report(completedRangeId, EPartitionState::Completed);
    specs = List();
    ASSERT_EQ(specs.size(), 5u);
    ASSERT_FALSE(specs.contains(completedRangeId));
    int successors = 0;
    for (const auto& [rangeId, spec] : specs) {
        if (!GetReadRanges(*saved->DistributingTable).contains(rangeId)) {
            ++successors;
            EXPECT_EQ(spec->PlannedTimestamp, TSystemTimestamp(1600));
            EXPECT_EQ(spec->PlannedReadDuration, TDuration::Seconds(600));
        }
    }
    EXPECT_EQ(successors, 1);
}

TEST_F(TReadScheduleTest, CompletionBeforeFirstDistributionAfterRestartKeepsSuccessorPlan)
{
    auto initial = List();
    auto completedId = initial.begin()->first;
    auto saved = Save();
    Clock_->Now = TSystemTimestamp(1800);
    Restore(saved);
    Report(completedId, EPartitionState::Completed);
    auto specs = List();
    ASSERT_EQ(specs.size(), initial.size());
    int successors = 0;
    for (const auto& [rangeId, spec] : specs) {
        if (!initial.contains(rangeId)) {
            ++successors;
            EXPECT_EQ(spec->PlannedTimestamp, TSystemTimestamp(1600));
        }
    }
    EXPECT_EQ(successors, 1);
}

TEST_F(TReadScheduleTest, ScaleDownPreservesBusyPlansAndScaleUpStartsNow)
{
    auto initial = List();
    Parameters_->MaxPartitionCount = TSize(3);
    Reconfigure();
    EXPECT_EQ(List().size(), 5u);
    auto first = initial.begin()->first;
    auto second = std::next(initial.begin())->first;
    Report(first, EPartitionState::Completed);
    EXPECT_EQ(List().size(), 4u);
    Report(second, EPartitionState::Completed);
    EXPECT_EQ(List().size(), 3u);

    Clock_->Now = TSystemTimestamp(1720);
    Parameters_->MaxPartitionCount = TSize(5);
    Reconfigure();
    auto grown = List();
    ASSERT_EQ(grown.size(), 5u);
    int fresh = 0;
    for (const auto& [rangeId, spec] : grown) {
        if (initial.contains(rangeId)) {
            EXPECT_EQ(spec->PlannedTimestamp, TSystemTimestamp(1000));
            EXPECT_EQ(spec->PlannedReadDuration, TDuration::Seconds(600));
        } else {
            ++fresh;
            EXPECT_EQ(spec->PlannedTimestamp, TSystemTimestamp(1720));
        }
    }
    EXPECT_EQ(fresh, 2);
}

TEST_F(TReadScheduleTest, UnreportedLastRangesHoldWatermarkBeforeAndAfterRestart)
{
    Parameters_->DesiredTableProcessTime = TDuration::Seconds(600);
    Reconfigure();
    auto specs = List();
    ASSERT_EQ(specs.size(), 10u);
    Clock_->Now = TSystemTimestamp(1500);
    // Repeat traversal before publishing the newly issued specs.
    EXPECT_EQ(Traverse()->SystemWatermark, TSystemTimestamp(1000));
    EXPECT_EQ(Traverse()->State, EStreamState::Drained);
    auto saved = Save();
    ASSERT_EQ(saved->DistributingTable->GetNotDistributedRows(), 0);
    Restore(saved);
    EXPECT_EQ(Traverse()->SystemWatermark, TSystemTimestamp(1000));
    for (const auto& [rangeId, spec] : specs) {
        Report(rangeId, EPartitionState::Executing, GetRowIndexRange(spec->Table).first);
    }
    EXPECT_EQ(Traverse()->State, EStreamState::Completed);

    Restore(saved, /*usePlannedTimestamps*/ false);
    EXPECT_EQ(Traverse()->State, EStreamState::Drained);
    EXPECT_EQ(Traverse()->SystemWatermark, TSystemTimestamp(100));
    EXPECT_EQ(Traverse()->EventWatermark, TSystemTimestamp(100));
}

TEST_F(TReadScheduleTest, TrunkRangesInitializeBeforeFirstTraverse)
{
    for (bool usePlannedTimestamps : {false, true}) {
        SCOPED_TRACE(usePlannedTimestamps);
        auto state = New<TSourceControllerState>();
        state->Inited = true;
        auto table = MakeTable("cluster", "current", "#10", 100, 6000);
        table->ByteSize = 6000;
        SetDistributedRows(table, 1200);
        SetDistributingRanges(table, {{TRangeId(101), {0, 600}}, {TRangeId(102), {600, 1200}}});
        state->DistributingTable = table;
        auto node = ConvertTo<IMapNodePtr>(state);
        auto tableNode = node->GetChildOrThrow("distributing_table")->AsMap();
        tableNode->RemoveChild("read_state");
        if (usePlannedTimestamps) {
            tableNode->AddChild("planned_start_time", ConvertToNode(TInstant::Seconds(700)));
            tableNode->AddChild("planned_read_duration", ConvertToNode(TDuration::Seconds(1200)));
            tableNode->AddChild("planned_processed_rows", ConvertToNode(0));
            tableNode->AddChild("planned_range_timestamps", ConvertToNode(THashMap<TRangeId, TSystemTimestamp>{{TRangeId(101), TSystemTimestamp(700)}, {TRangeId(102), TSystemTimestamp(820)}}));
        }
        Clock_->Now = TSystemTimestamp(1000);
        auto restored = ConvertTo<TSourceControllerStatePtr>(node);
        restored->DistributingTable = CloneYsonStruct(restored->DistributingTable);
        Restore(restored);
        Clock_->Now = TSystemTimestamp(1500);
        EXPECT_EQ(Traverse()->SystemWatermark, TSystemTimestamp(100));
        auto specs = List();
        ASSERT_EQ(specs.size(), 5u);
        EXPECT_EQ(Save()->DistributingTable->ReadState->NextRowIndex, 3000);
        for (auto rangeId : {TRangeId(101), TRangeId(102)}) {
            ASSERT_TRUE(specs.contains(rangeId));
            const auto& spec = GetOrCrash(specs, rangeId);
            EXPECT_EQ(spec->PlannedTimestamp, TSystemTimestamp(100));
            EXPECT_EQ(spec->PlannedReadDuration, TDuration::Zero());
            const auto& range = GetOrCrash(table->ReadState->Ranges, rangeId);
            EXPECT_EQ(GetRowIndexRange(spec->Table), std::pair(range.BeginRowIndex, range.EndRowIndex));
        }
    }
}

TEST_F(TReadScheduleTest, RestoresCombinedRangePlans)
{
    auto node = ConvertTo<IMapNodePtr>(Save());
    auto table = node->GetChildOrThrow("distributing_table")->AsMap();
    table->RemoveChild("distributed_rows");
    table->AddChild("distributed_rows", ConvertToNode(6000));
    table->RemoveChild("distributing_ranges");
    table->AddChild("distributing_ranges", ConvertToNode(TLegacyReadRanges{}));
    table->RemoveChild("read_state");
    table->AddChild("read_state", ConvertToNode(TYsonString(TStringBuf(R"({
        next_row_index=1200;
        ranges={
            "101"={begin_row_index=0; end_row_index=600; planned_timestamp=700; planned_read_duration="600s";};
            "102"={begin_row_index=600; end_row_index=1200; planned_timestamp=800; planned_read_duration="600s";};
        };
        free_slot_timestamps=[900;];
    })"))));
    auto parsed = ConvertTo<TSourceControllerStatePtr>(node);
    EXPECT_EQ(parsed->DistributingTable->LegacyDistributedRows, 1200);
    EXPECT_EQ(parsed->DistributingTable->LegacyDistributingRanges, (TLegacyReadRanges{
            {TRangeId(101), {0, 600}},
            {TRangeId(102), {600, 1200}},
                                                                   }));
    Restore(parsed);
    EXPECT_EQ(Traverse()->SystemWatermark, TSystemTimestamp(700));
    auto specs = List();
    ASSERT_TRUE(specs.contains(TRangeId(101)));
    ASSERT_TRUE(specs.contains(TRangeId(102)));
    EXPECT_EQ(specs[TRangeId(101)]->PlannedTimestamp, TSystemTimestamp(700));
    EXPECT_EQ(specs[TRangeId(102)]->PlannedTimestamp, TSystemTimestamp(800));
    int successors = 0;
    for (const auto& [rangeId, spec] : specs) {
        successors += spec->PlannedTimestamp == TSystemTimestamp(900);
    }
    EXPECT_EQ(successors, 1);
}

TEST_F(TReadScheduleTest, RestoreProgressAfterTrunkRollback)
{
    auto initial = List();
    auto completedId = initial.begin()->first;
    auto node = ConvertTo<IMapNodePtr>(Save());
    auto table = node->GetChildOrThrow("distributing_table")->AsMap();
    // Trunk drops unknown fields when saving the table.
    table->RemoveChild("read_state");
    table->RemoveChild("distributed_rows");
    table->AddChild("distributed_rows", ConvertToNode(3600));
    auto ranges = table->GetChildOrThrow("distributing_ranges")->AsMap();
    ranges->RemoveChild(ToString(completedId.Underlying()));
    ranges->AddChild("999", ConvertToNode(std::pair<i64, i64>(3000, 3600)));
    Clock_->Now = TSystemTimestamp(2000);
    Restore(ConvertToYsonString(node));
    auto specs = List();
    EXPECT_EQ(specs.size(), 5u);
    EXPECT_FALSE(specs.contains(completedId));
    ASSERT_TRUE(specs.contains(TRangeId(999)));
    EXPECT_EQ(GetRowIndexRange(specs[TRangeId(999)]->Table), (std::pair<i64, i64>(3000, 3600)));
    for (const auto& [rangeId, spec] : specs) {
        EXPECT_EQ(spec->PlannedTimestamp, TSystemTimestamp(100));
        if (rangeId != TRangeId(999)) {
            EXPECT_EQ(GetRowIndexRange(spec->Table), GetRowIndexRange(GetOrCrash(initial, rangeId)->Table));
        }
    }
    EXPECT_EQ(Save()->DistributingTable->ReadState->NextRowIndex, 3600);
}

TEST_F(TReadScheduleTest, SerializationProjectsCurrentProgress)
{
    auto initial = List();
    auto completedId = initial.begin()->first;
    Report(completedId, EPartitionState::Completed);
    auto saved = Save();
    auto clone = NYTree::CloneYsonStruct(saved->DistributingTable);
    auto node = ConvertTo<IMapNodePtr>(clone);
    EXPECT_EQ(ConvertTo<i64>(node->GetChildOrThrow("distributed_rows")), 3000);
    auto ranges = ConvertTo<TLegacyReadRanges>(node->GetChildOrThrow("distributing_ranges"));
    EXPECT_EQ(ranges.size(), 4u);
    EXPECT_FALSE(ranges.contains(completedId));
    EXPECT_EQ(ranges, GetReadRanges(*clone));
    EXPECT_TRUE(node->FindChild("read_state"));
    Restore(saved);
    auto next = List();
    auto nextNode = ConvertTo<IMapNodePtr>(Save()->DistributingTable);
    auto nextRanges = ConvertTo<TLegacyReadRanges>(nextNode->GetChildOrThrow("distributing_ranges"));
    EXPECT_EQ(nextRanges.size(), next.size());
    EXPECT_EQ(ConvertTo<i64>(nextNode->GetChildOrThrow("distributed_rows")), 3600);
    for (const auto& [rangeId, spec] : next) {
        EXPECT_EQ(GetOrCrash(nextRanges, rangeId), GetRowIndexRange(spec->Table));
    }
}

TEST(TSourceControllerTableTest, LegacyProgressIsReadyAfterParsing)
{
    auto table = ConvertTo<TSourceControllerTablePtr>(TYsonString(TStringBuf(R"({
        row_count=20; system_timestamp=100;
        distributed_rows=10; distributing_ranges={"1"=[0;5;]; "2"=[5;10;];};
    })")));
    ASSERT_TRUE(table->ReadState);
    EXPECT_EQ(table->ReadState->NextRowIndex, 10);
    EXPECT_EQ(table->ReadState->Ranges.size(), 2u);
    EXPECT_EQ(CalculateRemainingRowCount(*table, {}), 20);
    EXPECT_TRUE(table->ReadState->FreeSlotTimestamps.empty());
    auto clone = CloneYsonStruct(table);
    EXPECT_EQ(GetReadRanges(*clone), table->LegacyDistributingRanges);
}

TEST(TSourceControllerTableTest, SchedulingUpdatesLegacyProgressImmediately)
{
    auto table = MakeTable("cluster", "table", "#10", 100, 20);
    table->SetLimit(2, TSystemTimestamp(100));
    table->ScheduleRange(TRangeId(1), 5, 1);
    table->ScheduleRange(TRangeId(2), 5, 1);
    EXPECT_EQ(table->LegacyDistributedRows, 10);
    EXPECT_EQ(table->LegacyDistributingRanges, (TLegacyReadRanges{
            {TRangeId(1), {0, 5}},
            {TRangeId(2), {5, 10}},
                                               }));
    table->FinishRange(TRangeId(1));
    EXPECT_EQ(table->LegacyDistributingRanges, (TLegacyReadRanges{{TRangeId(2), {5, 10}}}));
    table->SkipRemainingRows();
    EXPECT_EQ(table->LegacyDistributedRows, 20);
    EXPECT_EQ(table->LegacyDistributingRanges, (TLegacyReadRanges{{TRangeId(2), {10, 10}}}));
    table->FinishRange(TRangeId(2));
    EXPECT_TRUE(table->LegacyDistributingRanges.empty());
    EXPECT_FALSE(table->CanSchedule());
}

TEST(TSourceControllerTableTest, RestartReusesEarliestCompletedClock)
{
    auto table = MakeTable("cluster", "table", "#10", 100, 20);
    table->SetLimit(2, TSystemTimestamp(100));
    table->ScheduleRange(TRangeId(1), 5, 1);
    table->ScheduleRange(TRangeId(2), 5, 0.5);
    EXPECT_TRUE(table->FinishRange(TRangeId(2)));
    EXPECT_TRUE(table->FinishRange(TRangeId(1)));
    EXPECT_FALSE(table->FinishRange(TRangeId(1)));
    auto restored = NYTree::CloneYsonStruct(table);
    restored->SetLimit(2, TSystemTimestamp(999));
    restored->ScheduleRange(TRangeId(3), 5, 1);
    EXPECT_EQ(GetOrCrash(restored->ReadState->Ranges, TRangeId(3)).PlannedTimestamp, TSystemTimestamp(105));
    restored->ScheduleRange(TRangeId(4), 5, 1);
    EXPECT_EQ(GetOrCrash(restored->ReadState->Ranges, TRangeId(4)).PlannedTimestamp, TSystemTimestamp(110));
    EXPECT_FALSE(restored->CanSchedule());
}

TEST(TSourceControllerTableTest, LimitReductionKeepsEarliestClock)
{
    auto table = MakeTable("cluster", "table", "#10", 100, 30);
    table->SetLimit(2, TSystemTimestamp(100));
    table->ScheduleRange(TRangeId(1), 5, 1);
    table->ScheduleRange(TRangeId(2), 5, 0.5);
    table->FinishRange(TRangeId(2));
    table->FinishRange(TRangeId(1));
    table->SetLimit(1, TSystemTimestamp(999));
    table->ScheduleRange(TRangeId(3), 5, 1);
    EXPECT_EQ(GetOrCrash(table->ReadState->Ranges, TRangeId(3)).PlannedTimestamp, TSystemTimestamp(105));
    table->SetLimit(2, TSystemTimestamp(999));
    table->ScheduleRange(TRangeId(4), 5, 1);
    EXPECT_EQ(GetOrCrash(table->ReadState->Ranges, TRangeId(4)).PlannedTimestamp, TSystemTimestamp(999));
}

TEST(TSourceControllerTableTest, RestoredZeroDurationDoesNotDelaySuccessor)
{
    auto table = ConvertTo<TSourceControllerTablePtr>(TYsonString(TStringBuf(R"({
        row_count=10;
        read_state={
            next_row_index=5;
            ranges={
                "1"={begin_row_index=0; end_row_index=5; planned_timestamp=100; planned_read_duration="0s";};
            };
        };
    })")));
    table->SetLimit(1, TSystemTimestamp(999));
    EXPECT_EQ(GetOrCrash(table->ReadState->Ranges, TRangeId(1)).PlannedReadDuration, TDuration::Zero());
    table->FinishRange(TRangeId(1));
    table->ScheduleRange(TRangeId(2), 5, 1);
    EXPECT_EQ(GetOrCrash(table->ReadState->Ranges, TRangeId(2)).PlannedTimestamp, TSystemTimestamp(100));
}

TEST_F(TReadScheduleTest, SavedReplicaKeepsMigratedPlanWhenMovedToCleanup)
{
    auto state = Save();
    SetDistributedRows(state->DistributingTable, state->DistributingTable->RowCount);
    auto replica = MakeTable("replica", "current", "#20", 100, 6000);
    replica->ByteSize = 6000;
    SetDistributedRows(replica, 600);
    SetDistributingRanges(replica, {{TRangeId(101), {0, 600}}});
    state->ClusterProgress->ByCluster["replica"] = replica;
    // Trunk omits read_state entirely.
    auto node = ConvertTo<IMapNodePtr>(state);
    node->GetChildOrThrow("cluster_progress")->AsMap()->GetChildOrThrow("by_cluster")->AsMap()->GetChildOrThrow("replica")->AsMap()->RemoveChild("read_state");
    Restore(ConvertToYsonString(node));
    Clock_->Now = TSystemTimestamp(2000);
    Run([&] {
        Controller_->CheckDistributionFinished();
    });
    auto specs = List();
    ASSERT_EQ(specs.size(), 1u);
    const auto& spec = GetOrCrash(specs, TRangeId(101));
    EXPECT_EQ(GetRowIndexRange(spec->Table), (std::pair<i64, i64>(600, 600)));
    EXPECT_EQ(spec->PlannedTimestamp, TSystemTimestamp(100));
    EXPECT_EQ(spec->PlannedReadDuration, TDuration::Zero());
    EXPECT_EQ(Traverse()->SystemWatermark, TSystemTimestamp(100));
    Report(TRangeId(101), EPartitionState::Completed);
    EXPECT_TRUE(List().empty());
    EXPECT_TRUE(Save()->PendingCleanupTables.empty());
}

TEST_F(TReadScheduleTest, CleanupKeepsPlanAndDoesNotIssueSuccessor)
{
    auto original = List();
    auto state = Save();
    TSourceController::StashRangesForCleanup(state.Get(), state->DistributingTable);
    auto current = MakeTable("cluster", "next", "#20", 200, 0);
    state->DistributingTable = current;
    state->DistributionFinished = true;
    Restore(state);
    Clock_->Now = TSystemTimestamp(1800);
    EXPECT_EQ(Traverse()->SystemWatermark, TSystemTimestamp(1000));
    auto cleanup = List();
    ASSERT_EQ(cleanup.size(), 5u);
    for (const auto& table : Save()->PendingCleanupTables) {
        EXPECT_TRUE(table->ReadState->FreeSlotTimestamps.empty());
    }
    for (const auto& [id, spec] : cleanup) {
        EXPECT_EQ(spec->PlannedTimestamp, TSystemTimestamp(1000));
        EXPECT_EQ(spec->PlannedReadDuration, TDuration::Seconds(600));
        const auto& range = GetRowIndexRange(spec->Table);
        EXPECT_EQ(range.first, range.second);
        Report(id, EPartitionState::Completed);
    }
    EXPECT_TRUE(List().empty());
    EXPECT_TRUE(Save()->PendingCleanupTables.empty());
}

TEST(TStaticTableSourceTest, RowTimestampUsesWholeSecondsAndHandlesZeroDuration)
{
    EXPECT_EQ(ComputePlannedTimestamp(50, {0, 100}, TSystemTimestamp(1000), TDuration::Zero()), TSystemTimestamp(1000));
    EXPECT_EQ(ComputePlannedTimestamp(10, {10, 10}, TSystemTimestamp(1000), TDuration::Seconds(600)), TSystemTimestamp(1600));
    EXPECT_EQ(ComputePlannedTimestamp(5500, {0, 30'000}, TSystemTimestamp(1000), TDuration::Seconds(300)), TSystemTimestamp(1055));
    EXPECT_EQ(ComputePlannedTimestamp(4'000'000'000'000, {3'000'000'000'000, 5'000'000'000'000}, TSystemTimestamp(1000), TDuration::Seconds(20'000'000)), TSystemTimestamp(10'001'000));
}

TEST(TStaticTableSourceTest, ReadScheduleSurvivesRateChangesAndModeToggle)
{
    auto table = MakeTable("cluster", "current", "#10", 100, 600);
    auto parameters = New<TDynamicTableSourceParameters>();
    parameters->DesiredTableProcessTime = TDuration::Seconds(30);
    parameters->DesiredPartitionRowsPerSecond = 10;
    parameters->DesiredPartitionProcessTime = TDuration::Seconds(10);
    auto specs = TSourceController::DoDistributing(parameters, 10, {}, table, MakeRangeIdGenerator(1), false, TSystemTimestamp(1000));
    ASSERT_EQ(specs.size(), 2u);
    table = ConvertTo<TSourceControllerTablePtr>(ConvertToYsonString(table));
    parameters->MaxRowsPerSecond = 5;
    for (bool usePlanned : {true, false}) {
        specs = TSourceController::DoDistributing(parameters, 10, {}, table, MakeRangeIdGenerator(3), usePlanned, TSystemTimestamp(2000));
        for (const auto& [rangeId, node] : specs) {
            auto spec = ConvertTo<TDynamicTableSourcePartitionSpecPtr>(node);
            EXPECT_EQ(spec->RowsPerSecond, 2.5);
            EXPECT_EQ(spec->PlannedTimestamp, TSystemTimestamp(1000));
            EXPECT_EQ(spec->PlannedReadDuration, TDuration::Seconds(10));
            EXPECT_EQ(spec->SystemTimestamp, TSystemTimestamp(usePlanned ? 1000 : 100));
            EXPECT_EQ(spec->EventTimestamp, TSystemTimestamp(usePlanned ? 1000 : 100));
        }
    }
}

TEST(TStaticTableSourceTest, FailoverReplansSavedRangesWithoutChangingStash)
{
    auto current = MakeTable("primary", "current", "#10", 100, 1000);
    auto stashed = MakeTable("replica", "current", "#20", 100, 1000);
    SetDistributedRows(stashed, 800);
    SetDistributingRanges(stashed, {{TRangeId(2), {100, 200}}, {TRangeId(1), {600, 800}}});
    auto parameters = New<TDynamicTableSourceParameters>();
    parameters->MaxRowsPerSecond = 2;
    parameters->DesiredTableProcessTime = TDuration::Seconds(100);
    parameters->DesiredPartitionRowsPerSecond = 10;
    auto saved = ConvertToYsonString(stashed);
    auto resumed = TSourceController::MakeFailoverTable(current, stashed, stashed, TSystemTimestamp(2000), parameters);
    EXPECT_EQ(ConvertToYsonString(stashed), saved);
    EXPECT_EQ(GetReadRanges(*resumed), GetReadRanges(*stashed));
    EXPECT_EQ(resumed->ReadState->NextRowIndex, 800);
    auto specs = TSourceController::DoDistributing(parameters, 10, {}, resumed, MakeRangeIdGenerator(3), true, TSystemTimestamp(2100));
    auto first = ConvertTo<TDynamicTableSourcePartitionSpecPtr>(GetOrCrash(specs, TRangeId(2)));
    auto second = ConvertTo<TDynamicTableSourcePartitionSpecPtr>(GetOrCrash(specs, TRangeId(1)));
    EXPECT_EQ(first->PlannedTimestamp, TSystemTimestamp(2000));
    EXPECT_EQ(second->PlannedTimestamp, TSystemTimestamp(2000));
    EXPECT_EQ(first->PlannedReadDuration, TDuration::Seconds(100));
    EXPECT_EQ(second->PlannedReadDuration, TDuration::Seconds(200));
}

TEST(TStaticTableSourceTest, EmptyTableAndZeroDesiredTimeDoNotBlockPlanning)
{
    auto parameters = New<TDynamicTableSourceParameters>();
    parameters->DesiredTableProcessTime = TDuration::Zero();
    for (auto rows : {0, 1}) {
        auto table = MakeTable("cluster", "current", "#10", 100, rows);
        for (bool usePlanned : {false, true}) {
            auto specs = TSourceController::DoDistributing(parameters, 1000, {}, table, MakeRangeIdGenerator(1), usePlanned, TSystemTimestamp(1000));
            EXPECT_EQ(specs.size(), rows);
        }
    }
}

TEST_F(TStaticTableSourceReaderTest, LegacyStateContinuesReadingAfterUpgrade)
{
    for (auto [usePlannedTimestamps, tableTimestamp] : {std::pair(false, 101), std::pair(true, 101), std::pair(true, 0)}) {
        SCOPED_TRACE(Format("UsePlannedTimestamps: %v, TableTimestamp: %v", usePlannedTimestamps, tableTimestamp));
        // The old controller saved range timestamps but no per-row plan or mode flag.
        auto node = ConvertTo<IMapNodePtr>(TYsonString(TStringBuf(R"({
            path=<cluster=test>"//table";
            row_count=3; byte_size=3; distributed_rows=3;
            event_timestamp=100; system_timestamp=101;
            distributing_ranges={"1"=[0;3;];};
        })")));
        if (usePlannedTimestamps) {
            node->AddChild("planned_start_time", ConvertToNode(TInstant::Seconds(1000)));
            node->AddChild("planned_read_duration", ConvertToNode(TDuration::Seconds(30)));
            node->AddChild("planned_range_timestamps", ConvertToNode(THashMap<TRangeId, TSystemTimestamp>{{TRangeId(1), TSystemTimestamp(1000)}}));
        }
        node->RemoveChild("system_timestamp");
        node->AddChild("system_timestamp", ConvertToNode(tableTimestamp));
        auto beforeParse = TSystemTimestamp(TInstant::Now().Seconds());
        auto table = ConvertTo<TSourceControllerTablePtr>(node);
        auto plannedTimestamp = GetOrCrash(table->ReadState->Ranges, TRangeId(1)).PlannedTimestamp;
        if (tableTimestamp == 0) {
            EXPECT_GE(plannedTimestamp, beforeParse);
            EXPECT_LE(plannedTimestamp, TSystemTimestamp(TInstant::Now().Seconds()));
        } else {
            EXPECT_EQ(plannedTimestamp, TSystemTimestamp(tableTimestamp));
        }
        auto parameters = New<TDynamicTableSourceParameters>();
        auto specs = TSourceController::DoDistributing(parameters, 1e9, {}, table, MakeRangeIdGenerator(2), usePlannedTimestamps, TSystemTimestamp(1000));
        auto spec = ConvertTo<TDynamicTableSourcePartitionSpecPtr>(GetOrCrash(specs, TRangeId(1)));
        EXPECT_EQ(GetRowIndexRange(spec->Table), (std::pair<i64, i64>(0, 3)));
        EXPECT_EQ(spec->UsePlannedTimestamps, usePlannedTimestamps);
        if (usePlannedTimestamps) {
            EXPECT_EQ(spec->PlannedTimestamp, plannedTimestamp);
            EXPECT_EQ(spec->PlannedReadDuration, TDuration::Zero());
        } else {
            EXPECT_EQ(spec->EventTimestamp, TSystemTimestamp(100));
            EXPECT_EQ(spec->SystemTimestamp, TSystemTimestamp(101));
        }
        auto client = MakeClient({MakeFuture(MakeTableReader({10, 11, 12}, OKFuture))});
        auto source = MakeSource(client, TDuration::Minutes(1));
        RunInInvoker([&] {
            auto legacyContext = MakeDynamicContext(TDuration::Minutes(1), 0, usePlannedTimestamps ? std::make_optional(TSystemTimestamp(1000)) : std::nullopt);
            for (auto key : {"planned_timestamp", "use_planned_timestamps"}) {
                legacyContext->DynamicPartitionSpec->RemoveChild(key);
            }
            source->Reconfigure(legacyContext);
            auto context = MakeDynamicContext(TDuration::Minutes(1), 0);
            spec->RowsPerSecond = 1e9;
            context->DynamicPartitionSpec = ConvertTo<IMapNodePtr>(spec);
            source->Reconfigure(context);
        });
        std::vector<ISource::TMessageBatch> batches;
        for (int attempt = 0; attempt < 3 && batches.empty(); ++attempt) {
            batches = RunInInvoker([&] {
                return WaitFor(source->GetNextBatch(BatcherSettings_)).ValueOrThrow();
            });
        }
        ASSERT_EQ(batches.size(), 3u);
        RunInInvoker([&] {
            for (int index = 0; index < 3; ++index) {
                const auto& message = batches[index].Messages.front();
                EXPECT_EQ(GetColumnValue<i64>(message, "value"), 10 + index);
                EXPECT_EQ(GetColumnValue<i64>(message, SequenceNumberColumnName), index);
                if (usePlannedTimestamps) {
                    EXPECT_EQ(message->SystemTimestamp, plannedTimestamp);
                    EXPECT_EQ(message->EventTimestamp, message->SystemTimestamp);
                } else {
                    EXPECT_EQ(message->SystemTimestamp, TSystemTimestamp(101));
                    EXPECT_EQ(message->EventTimestamp, TSystemTimestamp(100));
                }
                source->MarkPublished(batches[index].Cookie);
                source->MarkPersisted(batches[index].Cookie);
            }
            source->Sync();
            source->Commit();
        });
        table->SkipRemainingRows();
        specs = TSourceController::DoDistributing(parameters, 1e9, {}, table, MakeRangeIdGenerator(2), usePlannedTimestamps, TSystemTimestamp(1000));
        RunInInvoker([&] {
            auto context = MakeDynamicContext(TDuration::Minutes(1), 3);
            context->DynamicPartitionSpec = GetOrCrash(specs, TRangeId(1));
            source->Reconfigure(context);
            EXPECT_TRUE(source->BuildInflight()->Empty);
        });
        EXPECT_TRUE(ReadRows(source).empty());
    }
}

TEST_F(TStaticTableSourceReaderTest, PlannedInflightIncludesUnreadAndUnpersistedRows)
{
    auto client = MakeClient({MakeFuture(MakeTableReader({10, 11, 12}, OKFuture))});
    auto source = MakeSource(client, TDuration::Minutes(1), TSystemTimestamp(1000));
    RunInInvoker([&] {
        auto inflight = source->BuildInflight();
        EXPECT_EQ(inflight->MinSystemTimestamp, TSystemTimestamp(1000));
        EXPECT_EQ(inflight->MinEventTimestamp, TSystemTimestamp(1000));
        EXPECT_FALSE(inflight->Empty);
    });

    std::vector<ISource::TMessageBatch> batches;
    for (int attempt = 0; attempt < 3 && batches.empty(); ++attempt) {
        batches = RunInInvoker([&] {
            return WaitFor(source->GetNextBatch(BatcherSettings_)).ValueOrThrow();
        });
    }
    ASSERT_EQ(batches.size(), 3u);
    RunInInvoker([&] {
        for (const auto& batch : batches) {
            for (const auto& message : batch.Messages) {
                EXPECT_EQ(message->SystemTimestamp, TSystemTimestamp(1000));
                EXPECT_EQ(message->EventTimestamp, TSystemTimestamp(1000));
            }
            source->MarkPublished(batch.Cookie);
        }
        auto inflight = source->BuildInflight();
        EXPECT_EQ(inflight->MinSystemTimestamp, TSystemTimestamp(1000));
        EXPECT_EQ(inflight->MinEventTimestamp, TSystemTimestamp(1000));
        for (const auto& batch : batches) {
            source->MarkPersisted(batch.Cookie);
        }
        source->Sync();
        source->Commit();
        inflight = source->BuildInflight();
        EXPECT_TRUE(inflight->Empty);
        EXPECT_FALSE(inflight->MinSystemTimestamp);
        EXPECT_FALSE(inflight->MinEventTimestamp);
    });
}

TEST_F(TStaticTableSourceReaderTest, PlannedRowsAdvanceOnlyAfterPersistingThePrefix)
{
    auto client = MakeClient({MakeFuture(MakeTableReader({10, 11, 12}, OKFuture))});
    auto source = MakeSource(client, TDuration::Minutes(1), TSystemTimestamp(1000));
    RunInInvoker([&] {
        auto context = MakeDynamicContext(TDuration::Minutes(1), 0, TSystemTimestamp(1000));
        context->DynamicPartitionSpec->AddChild("planned_read_duration", ConvertToNode(TDuration::Seconds(30)));
        source->Reconfigure(context);
    });

    std::vector<ISource::TMessageBatch> batches;
    for (int attempt = 0; attempt < 3 && batches.empty(); ++attempt) {
        batches = RunInInvoker([&] {
            return WaitFor(source->GetNextBatch(BatcherSettings_)).ValueOrThrow();
        });
    }
    ASSERT_EQ(batches.size(), 3u);
    for (int index = 0; index < 3; ++index) {
        EXPECT_EQ(batches[index].Messages.front()->SystemTimestamp, TSystemTimestamp(1000 + 10 * index));
        EXPECT_EQ(batches[index].Messages.front()->EventTimestamp, TSystemTimestamp(1000 + 10 * index));
    }
    RunInInvoker([&] {
        for (const auto& batch : batches) {
            source->MarkPublished(batch.Cookie);
        }
        source->MarkPersisted(batches[0].Cookie);
        source->Sync();
        source->Commit();
        EXPECT_EQ(source->BuildInflight()->MinSystemTimestamp, TSystemTimestamp(1010));
        source->MarkPersisted(batches[2].Cookie);
        source->Sync();
        source->Commit();
        EXPECT_EQ(source->BuildInflight()->MinSystemTimestamp, TSystemTimestamp(1010));
        source->Reconfigure(MakeDynamicContext(TDuration::Minutes(1), 3, TSystemTimestamp(2000)));
        EXPECT_EQ(source->BuildInflight()->MinSystemTimestamp, TSystemTimestamp(1010));
        source->MarkPersisted(batches[1].Cookie);
        source->Sync();
        source->Commit();
        EXPECT_TRUE(source->BuildInflight()->Empty);
    });
}

TEST_F(TStaticTableSourceReaderTest, ParallelPartitionsKeepTheSlowestUnpersistedTimestamp)
{
    std::vector<TIntrusivePtr<TSource>> sources;
    std::vector<std::vector<ISource::TMessageBatch>> batches;
    for (int partition = 0; partition < 2; ++partition) {
        auto client = MakeClient({MakeFuture(MakeTableReader({10, 11, 12}, OKFuture))});
        auto source = MakeSource(client, TDuration::Minutes(1), TSystemTimestamp(1000));
        RunInInvoker([&] {
            auto context = MakeDynamicContext(TDuration::Minutes(1), 0, TSystemTimestamp(1000));
            context->DynamicPartitionSpec->AddChild("planned_read_duration", ConvertToNode(TDuration::Seconds(30)));
            source->Reconfigure(context);
        });
        sources.push_back(source);
        batches.emplace_back();
        for (int attempt = 0; attempt < 3 && batches.back().empty(); ++attempt) {
            batches.back() = RunInInvoker([&] {
                return WaitFor(source->GetNextBatch(BatcherSettings_)).ValueOrThrow();
            });
        }
        ASSERT_EQ(batches.back().size(), 3u);
    }
    RunInInvoker([&] {
        for (int partition = 0; partition < 2; ++partition) {
            for (const auto& batch : batches[partition]) {
                sources[partition]->MarkPublished(batch.Cookie);
            }
        }
        for (const auto& batch : batches[0]) {
            sources[0]->MarkPersisted(batch.Cookie);
        }
        sources[0]->Sync();
        sources[0]->Commit();
        EXPECT_TRUE(sources[0]->BuildInflight()->Empty);
        auto merged = MergeInflightTraverseData({sources[0]->BuildInflight(), sources[1]->BuildInflight()});
        EXPECT_FALSE(merged->Empty);
        EXPECT_EQ(merged->MinSystemTimestamp, TSystemTimestamp(1000));
        sources[1]->MarkPersisted(batches[1][0].Cookie);
        sources[1]->Sync();
        sources[1]->Commit();
        merged = MergeInflightTraverseData({sources[0]->BuildInflight(), sources[1]->BuildInflight()});
        EXPECT_EQ(merged->MinSystemTimestamp, TSystemTimestamp(1010));
        EXPECT_EQ(merged->MinEventTimestamp, TSystemTimestamp(1010));
    });
}

TEST_F(TStaticTableSourceReaderTest, ReconfiguredPlannedTimestampIsUsedForUnreadRows)
{
    auto client = MakeClient({MakeFuture(MakeTableReader({10, 11, 12}, OKFuture))});
    auto source = MakeSource(client, TDuration::Minutes(1), TSystemTimestamp(1000));
    RunInInvoker([&] {
        source->Reconfigure(MakeDynamicContext(TDuration::Minutes(1), 0, TSystemTimestamp(1000)));
        auto context = MakeDynamicContext(TDuration::Minutes(1), 0, TSystemTimestamp(2000));
        context->DynamicPartitionSpec->AddChild("planned_read_duration", ConvertToNode(TDuration::Seconds(30)));
        source->Reconfigure(context);
        auto inflight = source->BuildInflight();
        EXPECT_EQ(inflight->MinSystemTimestamp, TSystemTimestamp(2000));
        EXPECT_EQ(inflight->MinEventTimestamp, TSystemTimestamp(2000));
    });

    std::vector<ISource::TMessageBatch> batches;
    for (int attempt = 0; attempt < 3 && batches.empty(); ++attempt) {
        batches = RunInInvoker([&] {
            return WaitFor(source->GetNextBatch(BatcherSettings_)).ValueOrThrow();
        });
    }
    ASSERT_EQ(batches.size(), 3u);
    int rowIndex = 0;
    for (const auto& batch : batches) {
        for (const auto& message : batch.Messages) {
            EXPECT_EQ(message->SystemTimestamp, TSystemTimestamp(2000 + 10 * rowIndex));
            EXPECT_EQ(message->EventTimestamp, TSystemTimestamp(2000 + 10 * rowIndex));
            ++rowIndex;
        }
    }
    EXPECT_THAT(ReaderStarts_, ElementsAre(0));
}

TEST_F(TStaticTableSourceReaderTest, ReplannedInflightRetainsUnpersistedBatchTimestamps)
{
    auto client = MakeClient({MakeFuture(MakeTableReader({10, 11, 12}, OKFuture))});
    auto source = MakeSource(client, TDuration::Minutes(1), TSystemTimestamp(1000));
    BatcherSettings_->MaxRowsPerBatch = TSize(1);
    auto readBatch = [&] {
        std::vector<ISource::TMessageBatch> batches;
        for (int attempt = 0; attempt < 3 && batches.empty(); ++attempt) {
            batches = RunInInvoker([&] {
                return WaitFor(source->GetNextBatch(BatcherSettings_)).ValueOrThrow();
            });
        }
        return batches;
    };
    auto oldBatches = readBatch();
    ASSERT_EQ(oldBatches.size(), 1u);
    RunInInvoker([&] {
        source->Reconfigure(MakeDynamicContext(TDuration::Minutes(1), 0, TSystemTimestamp(2000)));
        auto inflight = source->BuildInflight();
        EXPECT_EQ(inflight->MinSystemTimestamp, TSystemTimestamp(1000));
        EXPECT_EQ(inflight->MinEventTimestamp, TSystemTimestamp(1000));
        source->MarkPublished(oldBatches.front().Cookie);
    });
    auto newBatches = readBatch();
    ASSERT_EQ(newBatches.size(), 1u);
    EXPECT_EQ(newBatches.front().Messages.front()->EventTimestamp, TSystemTimestamp(2000));
    RunInInvoker([&] {
        source->MarkPublished(newBatches.front().Cookie);
        source->MarkPersisted(newBatches.front().Cookie);
        auto inflight = source->BuildInflight();
        EXPECT_EQ(inflight->MinSystemTimestamp, TSystemTimestamp(1000));
        EXPECT_EQ(inflight->MinEventTimestamp, TSystemTimestamp(1000));

        source->MarkPersisted(oldBatches.front().Cookie);
        source->Sync();
        source->Commit();
        inflight = source->BuildInflight();
        EXPECT_FALSE(inflight->Empty);
        EXPECT_EQ(inflight->MinSystemTimestamp, TSystemTimestamp(2000));
        EXPECT_EQ(inflight->MinEventTimestamp, TSystemTimestamp(2000));
    });
    EXPECT_THAT(ReaderStarts_, ElementsAre(0));
}

TEST(TStaticTableSourceTest, ActiveTableAdvancesEventWatermarkWithoutDelay)
{
    EXPECT_EQ(
        TSourceController::CalculateEventWatermark(
            TSystemTimestamp(100),
            TSystemTimestamp(1000),
            /*isIdle*/ false,
            TDuration::Seconds(10)),
        TSystemTimestamp(100));
}

TEST(TStaticTableSourceTest, IdleEventWatermarkUsesClockDelay)
{
    EXPECT_EQ(
        TSourceController::CalculateEventWatermark(
            TSystemTimestamp(100),
            TSystemTimestamp(1000),
            /*isIdle*/ true,
            TDuration::Seconds(10)),
        TSystemTimestamp(990));
    EXPECT_EQ(
        TSourceController::CalculateEventWatermark(
            TSystemTimestamp(995),
            TSystemTimestamp(1000),
            /*isIdle*/ true,
            TDuration::Seconds(10)),
        TSystemTimestamp(995));
}

TEST(TStaticTableSourceTest, IdleEventWatermarkClampsUnderflowToTableTimestamp)
{
    auto distributingTable = New<TSourceControllerTable>();
    distributingTable->EventTimestamp = TSystemTimestamp(3);

    EXPECT_EQ(
        TSourceController::CalculateEventWatermark(
            distributingTable->EventTimestamp,
            TSystemTimestamp(5),
            /*isIdle*/ true,
            TDuration::Seconds(10)),
        distributingTable->EventTimestamp);
}

TEST(TStaticTableSourceTest, DisabledIdleDelayKeepsTableEventWatermark)
{
    EXPECT_EQ(
        TSourceController::CalculateEventWatermark(
            TSystemTimestamp(100),
            TSystemTimestamp(1000),
            /*isIdle*/ true,
            std::nullopt),
        TSystemTimestamp(100));
}

TEST(TStaticTableSourceTest, DoDistributingMaxPartitionCount)
{
    auto distributingTable = New<TSourceControllerTable>();
    distributingTable->Path = "primary://home/t1";
    distributingTable->RowCount = 15;
    SetDistributedRows(distributingTable, 10);

    auto sourceParameters = New<TDynamicTableSourceParameters>();
    sourceParameters->DesiredTableProcessTime = TDuration::Seconds(1);
    sourceParameters->DesiredPartitionProcessTime = TDuration::Seconds(1);
    sourceParameters->MaxPartitionCount = TSize(1);

    TSourceController::DoDistributing(
        sourceParameters,
        /*desiredRangeRowsPerSecond*/ 0.01,
        /*committedOffsetsExclusive*/ {},
        distributingTable,
        MakeRangeIdGenerator(42000),
        /*usePlannedTimestamps*/ false,
        TSystemTimestamp(1000));

    ASSERT_EQ(GetReadRanges(*distributingTable).size(), 1u);
    EXPECT_EQ(GetReadRanges(*distributingTable).at(TRangeId(42000)), (std::pair<i64, i64>(10, 11)));
    EXPECT_EQ(distributingTable->ReadState->NextRowIndex, 11);

    // Check that max partition count has an effect.
    sourceParameters->MaxPartitionCount = TSize(2);
    TSourceController::DoDistributing(
        sourceParameters,
        /*desiredRangeRowsPerSecond*/ 0.01,
        /*committedOffsetsExclusive*/ {},
        distributingTable,
        MakeRangeIdGenerator(43000),
        /*usePlannedTimestamps*/ false,
        TSystemTimestamp(1000));

    ASSERT_EQ(GetReadRanges(*distributingTable).size(), 2u);
    EXPECT_EQ(GetReadRanges(*distributingTable).at(TRangeId(43000)), (std::pair<i64, i64>(11, 12)));
    EXPECT_EQ(distributingTable->ReadState->NextRowIndex, 12);
}

TEST(TStaticTableSourceTest, IgnoreSymlinks)
{
    auto lastProcessingTable = New<TSourceControllerTable>();
    lastProcessingTable->Path = "primary://home/t1";
    lastProcessingTable->RowCount = 30;
    SetDistributedRows(lastProcessingTable, 20);
    SetDistributingRange(lastProcessingTable, TRangeId(40000), std::pair<i64, i64>(12, 20));
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
    SetDistributedRows(lastProcessingTable, 20);
    SetDistributingRange(lastProcessingTable, TRangeId(40000), std::pair<i64, i64>(12, 20));
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
    SetDistributedRows(lastProcessingTable, 20);
    SetDistributingRange(lastProcessingTable, TRangeId(40000), std::pair<i64, i64>(12, 20));
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
    SetDistributedRows(lastProcessingTable, 20);
    SetDistributingRange(lastProcessingTable, TRangeId(40000), std::pair<i64, i64>(12, 20));
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
    SetDistributedRows(lastProcessingTable, 20);
    SetDistributingRange(lastProcessingTable, TRangeId(40000), std::pair<i64, i64>(12, 20));

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
        SetDistributedRows(distributingTable, 0);

        auto committedOffsetsExclusive = THashMap<TRangeId, i64>{};

        EXPECT_EQ(CalculateRemainingRowCount(*distributingTable, committedOffsetsExclusive), 15);

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
            MakeRangeIdGenerator(42000),
            /*usePlannedTimestamps*/ false,
            TSystemTimestamp(1000));

        ASSERT_EQ(GetReadRanges(*distributingTable).size(), 3u);
        EXPECT_EQ(GetReadRanges(*distributingTable).at(TRangeId(42000)), (std::pair<i64, i64>(0, 5)));
        EXPECT_EQ(GetReadRanges(*distributingTable).at(TRangeId(42001)), (std::pair<i64, i64>(5, 10)));
        EXPECT_EQ(distributingTable->ReadState->NextRowIndex, 15);
    }
    // Make correction of DesiredPartitionProcessTime for end of table.
    {
        auto distributingTable = New<TSourceControllerTable>();
        distributingTable->Path = "primary://home/t1";
        distributingTable->RowCount = 30;
        SetDistributedRows(distributingTable, 20);
        SetDistributingRange(distributingTable, TRangeId(40000), std::pair<i64, i64>(12, 20));

        auto committedOffsetsExclusive = THashMap<TRangeId, i64>{{TRangeId(40000), 15}}; // [15, 20) is only range in processing right now.

        EXPECT_EQ(CalculateRemainingRowCount(*distributingTable, committedOffsetsExclusive), 15);

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
            MakeRangeIdGenerator(42000),
            /*usePlannedTimestamps*/ false,
            TSystemTimestamp(1000));

        ASSERT_EQ(GetReadRanges(*distributingTable).size(), 3u);
        EXPECT_EQ(GetReadRanges(*distributingTable).at(TRangeId(40000)), (std::pair<i64, i64>(12, 20)));
        EXPECT_EQ(GetReadRanges(*distributingTable).at(TRangeId(42000)), (std::pair<i64, i64>(20, 25)));
        EXPECT_EQ(GetReadRanges(*distributingTable).at(TRangeId(42001)), (std::pair<i64, i64>(25, 30)));
        EXPECT_EQ(distributingTable->ReadState->NextRowIndex, 30);
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
    EXPECT_DOUBLE_EQ(TSourceController::GetDesiredRowsPerSecond(sourceParameters, distributingTable.Get()), 5.0);

    sourceParameters->MaxRowsPerSecond = 4.0;
    EXPECT_DOUBLE_EQ(TSourceController::GetDesiredRowsPerSecond(sourceParameters, distributingTable.Get()), 4.0);

    sourceParameters->MaxBytesPerSecond = 30.0;
    EXPECT_DOUBLE_EQ(TSourceController::GetDesiredRowsPerSecond(sourceParameters, distributingTable.Get()), 3.0);
}

TEST(TStaticTableSourceTest, GetDesiredRangeRowsPerSecond)
{
    auto distributingTable = New<TSourceControllerTable>();
    distributingTable->RowCount = 10;
    distributingTable->ByteSize = 100;

    auto sourceParameters = New<TDynamicTableSourceParameters>();
    sourceParameters->DesiredPartitionRowsPerSecond = 10;
    sourceParameters->DesiredPartitionBytesPerSecond = 1000;
    EXPECT_DOUBLE_EQ(TSourceController::GetDesiredRangeRowsPerSecond(sourceParameters, distributingTable.Get()), 10.0);

    sourceParameters->DesiredPartitionBytesPerSecond = 50;
    EXPECT_DOUBLE_EQ(TSourceController::GetDesiredRangeRowsPerSecond(sourceParameters, distributingTable.Get()), 5.0);

    distributingTable->ByteSize = 50;
    EXPECT_DOUBLE_EQ(TSourceController::GetDesiredRangeRowsPerSecond(sourceParameters, distributingTable.Get()), 10.0);
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
                .Item("test_timestamp").Value("2021-01-01T00:00:00")
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
                .Item("test_timestamp").Value("2021-01-01T00:00:00")
            .EndAttributes()
            .Entity();
        // clang-format on

        auto locator = New<TTableTimestampLocatorSpec>();
        locator->Attribute = "test_timestamp";
        locator->Format = ETimestampFormat::Iso8601;
        locator->Timezone = "Europe/Moscow";

        auto result = TSourceController::ExtractTimestamp(node, locator);
        EXPECT_EQ(result.Underlying(), 1609448400u);
    }

    {
        // clang-format off
        auto node = NYT::NYTree::BuildYsonNodeFluently()
            .BeginAttributes()
                .Item("test_timestamp").Value("2021-01-01")
            .EndAttributes()
            .Entity();
        // clang-format on

        auto locator = New<TTableTimestampLocatorSpec>();
        locator->Attribute = "test_timestamp";
        locator->Format = ETimestampFormat::Iso8601;
        locator->Timezone = "Europe/Moscow";

        auto result = TSourceController::ExtractTimestamp(node, locator);
        EXPECT_EQ(result.Underlying(), 1609448400u);
    }

    {
        // clang-format off
        auto node = NYT::NYTree::BuildYsonNodeFluently()
            .BeginAttributes()
                .Item("test_timestamp").Value("1970-01-01T00:00:00")
            .EndAttributes()
            .Entity();
        // clang-format on

        auto locator = New<TTableTimestampLocatorSpec>();
        locator->Attribute = "test_timestamp";
        locator->Format = ETimestampFormat::Iso8601;
        locator->Timezone = "Europe/Moscow";

        EXPECT_THROW(
            TSourceController::ExtractTimestamp(node, locator),
            NYT::TErrorException);
    }

    {
        // clang-format off
        auto node = NYT::NYTree::BuildYsonNodeFluently()
            .BeginAttributes()
                .Item("test_timestamp").Value("2021-01-01T00:00:00+02:00")
            .EndAttributes()
            .Entity();
        // clang-format on

        auto locator = New<TTableTimestampLocatorSpec>();
        locator->Attribute = "test_timestamp";
        locator->Format = ETimestampFormat::Iso8601;
        locator->Timezone = "Europe/Moscow";

        auto result = TSourceController::ExtractTimestamp(node, locator);
        EXPECT_EQ(result.Underlying(), 1609452000u);
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

TEST(TStaticTableSourceTest, TimestampLocatorTimezoneValidation)
{
    EXPECT_NO_THROW(ConvertTo<TTableTimestampLocatorSpecPtr>(TYsonString(TStringBuf(
        "{attribute=test_timestamp;timezone=\"Europe/Moscow\";}"))));

    EXPECT_THROW(
        ConvertTo<TTableTimestampLocatorSpecPtr>(TYsonString(TStringBuf(
            "{attribute=test_timestamp;timezone=\"Invalid/Timezone\";}"))),
        NYT::TErrorException);

    EXPECT_THROW(
        ConvertTo<TTableTimestampLocatorSpecPtr>(TYsonString(TStringBuf(
            "{attribute=test_timestamp;format=seconds;timezone=\"Europe/Moscow\";}"))),
        NYT::TErrorException);
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

TEST(TMultiClusterMergeTest, KeepsDistinctNames)
{
    auto result = TSourceController::MergeByName({
        {MakeTable("cluster-a", "t1", "#id-a-t1", 100)},
        {MakeTable("cluster-b", "t2", "#id-b-t2", 200)},
    });

    ASSERT_EQ(std::ssize(result), 2);
}

TEST(TMultiClusterMergeTest, SameNameDeduplicatesActiveFirstWins)
{
    // Sticky failover passes the active cluster's list first, so its replica wins ties on the name.
    auto result = TSourceController::MergeByName({
        {MakeTable("cluster-b", "t", "#id-b", 100)},
        {MakeTable("cluster-a", "t", "#id-a", 100)},
    });

    ASSERT_EQ(std::ssize(result), 1);
    EXPECT_EQ(*result[0]->Path.GetCluster(), "cluster-b");
}

TEST(TMultiClusterMergeTest, DistinctNamesSameEventTimestampNotCollapsed)
{
    // Two different tables that happen to share an EventTimestamp must both survive (distinct names);
    // dedup is by name, not by EventTimestamp.
    auto result = TSourceController::MergeByName({
        {MakeTable("cluster-a", "t-x", "#id-x", 100), MakeTable("cluster-a", "t-y", "#id-y", 100)},
    });

    ASSERT_EQ(std::ssize(result), 2);
}

TEST(TMultiClusterMergeTest, EmptyLists)
{
    EXPECT_TRUE(TSourceController::MergeByName({}).empty());
    EXPECT_TRUE(TSourceController::MergeByName({{}, {}}).empty());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TMultiClusterOrdinalTest, ZeroWhenNoCollision)
{
    auto state = New<TSourceControllerState>();
    std::vector<TSourceControllerTablePtr> tables{
        MakeTable("cluster-a", "t1", "#id1", 100),
        MakeTable("cluster-a", "t2", "#id2", 200),
    };

    TSourceController::AssignEventOrdinals(tables, state.Get());

    EXPECT_EQ(tables[0]->EventOrdinal, 0);
    EXPECT_EQ(tables[1]->EventOrdinal, 0);
}

TEST(TMultiClusterOrdinalTest, DistinguishesCollisionByCreationTime)
{
    auto state = New<TSourceControllerState>();
    auto later = MakeTable("cluster-a", "t-later", "#id-later", 100);
    later->SystemTimestamp = TSystemTimestamp(90);
    auto earlier = MakeTable("cluster-a", "t-earlier", "#id-earlier", 100);
    earlier->SystemTimestamp = TSystemTimestamp(50);
    std::vector<TSourceControllerTablePtr> tables{later, earlier};

    TSourceController::AssignEventOrdinals(tables, state.Get());

    // Ordinals are assigned in creation_time order: earlier (50) -> 0, later (90) -> 1.
    EXPECT_EQ(earlier->EventOrdinal, 0);
    EXPECT_EQ(later->EventOrdinal, 1);
}

TEST(TMultiClusterOrdinalTest, LateNameAppendsAtEnd)
{
    auto state = New<TSourceControllerState>();
    state->Inited = true;
    state->DistributingTable = MakeTable("cluster-a", "t-first", "#id-first", 100);

    auto first = MakeTable("cluster-a", "t-first", "#id-first", 100);
    first->SystemTimestamp = TSystemTimestamp(50);
    std::vector<TSourceControllerTablePtr> firstRound{first};
    TSourceController::AssignEventOrdinals(firstRound, state.Get());
    ASSERT_EQ(first->EventOrdinal, 0);

    // A new table with the same EventTimestamp but an earlier creation_time must still land after the
    // already-numbered one — its ordinal grows, so it is never mistaken for already-processed.
    auto late = MakeTable("cluster-a", "t-late", "#id-late", 100);
    late->SystemTimestamp = TSystemTimestamp(10);
    auto firstAgain = MakeTable("cluster-a", "t-first", "#id-first", 100);
    std::vector<TSourceControllerTablePtr> secondRound{late, firstAgain};
    TSourceController::AssignEventOrdinals(secondRound, state.Get());

    EXPECT_EQ(firstAgain->EventOrdinal, 0);
    EXPECT_EQ(late->EventOrdinal, 1);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TMultiClusterFindReplicaTest, ReturnsReplicaWithMatchingName)
{
    std::vector<TSourceControllerTablePtr> tables{
        MakeTable("cluster-a", "t-a", "#id-a", 100),
        MakeTable("cluster-b", "t-b", "#id-b", 200),
    };

    auto serving = TSourceController::FindReplicaServingCurrentTable(tables, "t-b");
    ASSERT_NE(serving, nullptr);
    EXPECT_EQ(*serving->Path.GetCluster(), "cluster-b");
}

TEST(TMultiClusterFindReplicaTest, ReturnsNullWhenAbsent)
{
    std::vector<TSourceControllerTablePtr> tables{MakeTable("cluster-a", "t-a", "#id-a", 100)};
    EXPECT_EQ(TSourceController::FindReplicaServingCurrentTable(tables, "t-missing"), nullptr);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TMultiClusterFailoverTableTest, FreshStartOnServingCluster)
{
    auto current = MakeTable("cluster-a", "t", "#id-a", 200, /*rowCount*/ 100, /*era*/ 3);
    current->EventOrdinal = 2;
    SetDistributedRows(current, 40);
    SetDistributingRange(current, TRangeId(7), {30, 40});

    auto serving = MakeTable("cluster-b", "t", "#id-b", 200, /*rowCount*/ 100, /*era*/ 9);

    auto result = TSourceController::MakeFailoverTable(current, serving, /*resumeFrom*/ nullptr, TSystemTimestamp(2000), New<TDynamicTableSourceParameters>());

    EXPECT_EQ(result->ReadState->NextRowIndex, 0);
    EXPECT_TRUE(GetReadRanges(*result).empty());
    EXPECT_EQ(*result->Path.GetCluster(), "cluster-b");
    EXPECT_EQ(result->Path.GetPath(), "#id-b");
    EXPECT_EQ(result->RowCount, 100);
    EXPECT_EQ(result->Era, 3);
    EXPECT_EQ(result->EventTimestamp, TSystemTimestamp(200));
    // Unified key carried from the current table so the replica is recognized as the continuation.
    EXPECT_EQ(result->EventOrdinal, 2);
}

TEST(TMultiClusterFailoverTableTest, ResumesStashedProgress)
{
    auto resumeFrom = MakeTable("cluster-b", "t", "#id-b", 200, /*rowCount*/ 100, /*era*/ 2);
    SetDistributedRows(resumeFrom, 60);
    SetDistributingRange(resumeFrom, TRangeId(9), {55, 60});

    auto current = MakeTable("cluster-a", "t", "#id-a", 200, /*rowCount*/ 100, /*era*/ 3);
    current->EventOrdinal = 1;
    auto serving = MakeTable("cluster-b", "t", "#id-b", 200, /*rowCount*/ 100, /*era*/ 9);

    auto result = TSourceController::MakeFailoverTable(current, serving, resumeFrom, TSystemTimestamp(2000), New<TDynamicTableSourceParameters>());

    EXPECT_EQ(result->ReadState->NextRowIndex, 60);
    ASSERT_TRUE(GetReadRanges(*result).contains(TRangeId(9)));
    EXPECT_EQ(GetReadRanges(*result).at(TRangeId(9)), (std::pair<i64, i64>(55, 60)));
    EXPECT_EQ(*result->Path.GetCluster(), "cluster-b");
    EXPECT_EQ(result->Path.GetPath(), "#id-b");
    EXPECT_EQ(result->RowCount, 100);
    EXPECT_EQ(result->Era, 3);
    EXPECT_EQ(result->EventOrdinal, 1);
}

TEST(TMultiClusterFailoverTableTest, SystemTimestampDoesNotRegress)
{
    auto current = MakeTable("cluster-a", "t", "#id-a", 200, /*rowCount*/ 100);
    current->SystemTimestamp = TSystemTimestamp(500);
    auto serving = MakeTable("cluster-b", "t", "#id-b", 200, /*rowCount*/ 100);
    serving->SystemTimestamp = TSystemTimestamp(100);

    auto result = TSourceController::MakeFailoverTable(current, serving, /*resumeFrom*/ nullptr, TSystemTimestamp(2000), New<TDynamicTableSourceParameters>());

    EXPECT_EQ(result->SystemTimestamp, TSystemTimestamp(500));
    EXPECT_EQ(*result->Path.GetCluster(), "cluster-b");
}

////////////////////////////////////////////////////////////////////////////////

using TStash = THashMap<std::string, TSourceControllerTablePtr>;

TEST(TMultiClusterDecideFailoverTest, NoSwapWhenNothingServesTheTable)
{
    auto current = MakeTable("cluster-a", "t", "#id-a", 200);
    EXPECT_FALSE(TSourceController::DecideFailover(
        current,
        /*servingReplica*/ nullptr,
        TStash{},
        NLogging::TLogger(),
        TSystemTimestamp(2000),
        New<TDynamicTableSourceParameters>())
            .has_value());
}

TEST(TMultiClusterDecideFailoverTest, NoSwapWhenActiveClusterStillServes)
{
    auto current = MakeTable("cluster-a", "t", "#id-a", 200);
    auto serving = MakeTable("cluster-a", "t", "#id-a", 200);
    EXPECT_FALSE(TSourceController::DecideFailover(current, serving, TStash{}, NLogging::TLogger(), TSystemTimestamp(2000), New<TDynamicTableSourceParameters>()).has_value());
}

TEST(TMultiClusterDecideFailoverTest, FreshFailoverWhenNoStashForTargetCluster)
{
    auto current = MakeTable("cluster-a", "t", "#id-a", 200, /*rowCount*/ 100, /*era*/ 3);
    SetDistributedRows(current, 40);
    auto serving = MakeTable("cluster-b", "t", "#id-b", 200, /*rowCount*/ 100);

    auto decision = TSourceController::DecideFailover(current, serving, TStash{}, NLogging::TLogger(), TSystemTimestamp(2000), New<TDynamicTableSourceParameters>());

    // No stash for the target cluster → start from scratch; logical identity (Era) preserved.
    ASSERT_TRUE(decision.has_value());
    EXPECT_EQ(decision->StashedCluster, "cluster-a");
    EXPECT_EQ(*decision->NewTable->Path.GetCluster(), "cluster-b");
    EXPECT_EQ(decision->NewTable->ReadState->NextRowIndex, 0);
    EXPECT_EQ(decision->NewTable->Era, 3);
}

TEST(TMultiClusterDecideFailoverTest, ResumesFromStashForTargetCluster)
{
    auto current = MakeTable("cluster-a", "t", "#id-a", 200, /*rowCount*/ 100);

    // Stash id matches the live replica id → safe to resume.
    auto stashedB = MakeTable("cluster-b", "t", "#id-b", 200, /*rowCount*/ 100);
    SetDistributedRows(stashedB, 60);
    SetDistributingRange(stashedB, TRangeId(9), {55, 60});
    TStash stash{{"cluster-b", stashedB}};

    auto serving = MakeTable("cluster-b", "t", "#id-b", 200, /*rowCount*/ 100);

    auto decision = TSourceController::DecideFailover(current, serving, stash, NLogging::TLogger(), TSystemTimestamp(2000), New<TDynamicTableSourceParameters>());

    ASSERT_TRUE(decision.has_value());
    EXPECT_EQ(decision->StashedCluster, "cluster-a");
    EXPECT_EQ(*decision->NewTable->Path.GetCluster(), "cluster-b");
    EXPECT_EQ(decision->NewTable->ReadState->NextRowIndex, 60);
    ASSERT_TRUE(GetReadRanges(*decision->NewTable).contains(TRangeId(9)));
    // DecideFailover must not mutate the stash it was given.
    EXPECT_TRUE(stash.contains("cluster-b"));
}

TEST(TMultiClusterDecideFailoverTest, RereadsWhenStashIdDiffers)
{
    auto current = MakeTable("cluster-a", "t", "#id-a", 200, /*rowCount*/ 100);

    // Stash id differs from the live replica id → table was recreated → start fresh, don't resume.
    auto stashedB = MakeTable("cluster-b", "t", "#id-b-old", 200, /*rowCount*/ 100);
    SetDistributedRows(stashedB, 60);
    TStash stash{{"cluster-b", stashedB}};

    auto serving = MakeTable("cluster-b", "t", "#id-b-new", 200, /*rowCount*/ 100);

    auto decision = TSourceController::DecideFailover(current, serving, stash, NLogging::TLogger(), TSystemTimestamp(2000), New<TDynamicTableSourceParameters>());

    ASSERT_TRUE(decision.has_value());
    EXPECT_EQ(decision->NewTable->ReadState->NextRowIndex, 0);
}

TEST(TMultiClusterDecideFailoverTest, IgnoresStashOfUnrelatedCluster)
{
    auto current = MakeTable("cluster-a", "t", "#id-a", 200, /*rowCount*/ 100);
    auto stashedC = MakeTable("cluster-c", "t", "#id-c", 200, /*rowCount*/ 100);
    SetDistributedRows(stashedC, 77);
    TStash stash{{"cluster-c", stashedC}};

    auto serving = MakeTable("cluster-b", "t", "#id-b", 200, /*rowCount*/ 100);

    auto decision = TSourceController::DecideFailover(current, serving, stash, NLogging::TLogger(), TSystemTimestamp(2000), New<TDynamicTableSourceParameters>());

    // The cluster-c stash is irrelevant when failing over to cluster-b.
    ASSERT_TRUE(decision.has_value());
    EXPECT_EQ(*decision->NewTable->Path.GetCluster(), "cluster-b");
    EXPECT_EQ(decision->NewTable->ReadState->NextRowIndex, 0);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStashRangesForCleanupTest, IgnoresTableWithoutRanges)
{
    auto state = New<TSourceControllerState>();
    auto table = MakeTable("cluster-a", "t", "#id", 100);

    TSourceController::StashRangesForCleanup(state.Get(), table);

    EXPECT_TRUE(state->PendingCleanupTables.empty());
}

TEST(TStashRangesForCleanupTest, SnapshotsAndTrims)
{
    auto state = New<TSourceControllerState>();
    auto table = MakeTable("cluster-a", "t", "#id", 100, /*rowCount*/ 100);
    SetDistributedRows(table, 40);
    SetDistributingRange(table, TRangeId(7), {30, 40});

    TSourceController::StashRangesForCleanup(state.Get(), table);

    ASSERT_EQ(std::ssize(state->PendingCleanupTables), 1);
    const auto& snapshot = state->PendingCleanupTables[0];
    EXPECT_EQ(snapshot->Path.GetPath(), "#id");
    ASSERT_TRUE(GetReadRanges(*snapshot).contains(TRangeId(7)));
    auto range = GetReadRanges(*snapshot).at(TRangeId(7));
    EXPECT_EQ(range.first, range.second);
    EXPECT_EQ(snapshot->ReadState->NextRowIndex, snapshot->RowCount);
    // The clone must leave the original table untouched.
    EXPECT_EQ(GetReadRanges(*table).at(TRangeId(7)), (std::pair<i64, i64>(30, 40)));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TResetStashIfTableChangedTest, KeepsStashWhenIdentityMatches)
{
    auto state = New<TSourceControllerState>();
    state->ClusterProgress = New<TClusterProgress>();
    auto current = MakeTable("cluster-a", "t", "#id", 100, /*rowCount*/ 100, /*era*/ 3);
    current->EventOrdinal = 2;

    state->ClusterProgress->Era = 3;
    state->ClusterProgress->EventTimestamp = TSystemTimestamp(100);
    state->ClusterProgress->EventOrdinal = 2;
    auto stashed = MakeTable("cluster-b", "t", "#id-b", 100, /*rowCount*/ 100);
    SetDistributingRange(stashed, TRangeId(9), {50, 60});
    state->ClusterProgress->ByCluster["cluster-b"] = stashed;

    TSourceController::ResetStashIfTableChanged(state.Get(), current);

    EXPECT_TRUE(state->ClusterProgress->ByCluster.contains("cluster-b"));
    EXPECT_TRUE(state->PendingCleanupTables.empty());
}

TEST(TResetStashIfTableChangedTest, MovesStaleStashToCleanupAndRebinds)
{
    auto state = New<TSourceControllerState>();
    state->ClusterProgress = New<TClusterProgress>();
    auto current = MakeTable("cluster-a", "t", "#id", 200, /*rowCount*/ 100, /*era*/ 3);
    current->EventOrdinal = 1;

    // Stash belongs to a different (previous) table.
    state->ClusterProgress->Era = 3;
    state->ClusterProgress->EventTimestamp = TSystemTimestamp(100);
    state->ClusterProgress->EventOrdinal = 0;
    auto stashed = MakeTable("cluster-b", "t-old", "#id-b", 100, /*rowCount*/ 100);
    SetDistributingRange(stashed, TRangeId(9), {50, 60});
    state->ClusterProgress->ByCluster["cluster-b"] = stashed;

    TSourceController::ResetStashIfTableChanged(state.Get(), current);

    EXPECT_TRUE(state->ClusterProgress->ByCluster.empty());
    EXPECT_EQ(std::ssize(state->PendingCleanupTables), 1);
    EXPECT_EQ(state->ClusterProgress->Era, current->Era);
    EXPECT_EQ(state->ClusterProgress->EventTimestamp, current->EventTimestamp);
    EXPECT_EQ(state->ClusterProgress->EventOrdinal, current->EventOrdinal);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TEnsureCurrentPresentTest, AddsCurrentWhenAbsent)
{
    auto current = MakeTable("cluster-a", "t", "#id", 200, /*rowCount*/ 100);
    std::vector<TSourceControllerTablePtr> tables{
        MakeTable("cluster-a", "t-later", "#id-later", 300),
    };

    TSourceController::EnsureCurrentPresent(tables, current);

    ASSERT_EQ(std::ssize(tables), 2);
    // Re-sorted by ordering key: current (EventTimestamp 200) lands before the 300 one.
    EXPECT_EQ(tables[0]->Path.GetPath(), "#id");
}

TEST(TEnsureCurrentPresentTest, NoOpWhenReplicaSharesOrderingKey)
{
    auto current = MakeTable("cluster-a", "t", "#id", 200, /*rowCount*/ 100);
    std::vector<TSourceControllerTablePtr> tables{
        MakeTable("cluster-b", "t", "#id-b", 200, /*rowCount*/ 100),
    };

    TSourceController::EnsureCurrentPresent(tables, current);

    EXPECT_EQ(std::ssize(tables), 1);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TMultiClusterSpecTest, ParsesClustersList)
{
    auto spec = ConvertTo<TTableSourceParametersPtr>(TYsonStringBuf(R"""({
        tables_path = "<clusters=[primary;remote]>//home/data";
    })"""));

    ASSERT_TRUE(spec->TablesPath.has_value());
    auto clusters = spec->TablesPath->GetClusters();
    ASSERT_TRUE(clusters.has_value());
    EXPECT_EQ(std::ssize(*clusters), 2);
    EXPECT_EQ((*clusters)[0], "primary");
    EXPECT_EQ((*clusters)[1], "remote");
}

TEST(TMultiClusterSpecTest, SingleClusterStillValid)
{
    auto spec = ConvertTo<TTableSourceParametersPtr>(TYsonStringBuf(R"""({
        tables_path = "<cluster=primary>//home/data";
    })"""));

    ASSERT_TRUE(spec->TablesPath->GetCluster().has_value());
    EXPECT_EQ(*spec->TablesPath->GetCluster(), "primary");
    EXPECT_FALSE(spec->TablesPath->GetClusters().has_value());
}

TEST(TMultiClusterSpecTest, RejectsBothClusterAndClusters)
{
    EXPECT_THROW(
        ConvertTo<TTableSourceParametersPtr>(TYsonStringBuf(R"""({
            tables_path = "<cluster=primary;clusters=[primary;remote]>//home/data";
        })""")),
        NYT::TErrorException);
}

TEST(TMultiClusterSpecTest, RejectsEmptyClusters)
{
    EXPECT_THROW(
        ConvertTo<TTableSourceParametersPtr>(TYsonStringBuf(R"""({
            tables_path = "<clusters=[]>//home/data";
        })""")),
        NYT::TErrorException);
}

TEST(TMultiClusterSpecTest, RejectsDuplicateClusters)
{
    EXPECT_THROW(
        ConvertTo<TTableSourceParametersPtr>(TYsonStringBuf(R"""({
            tables_path = "<clusters=[primary;primary]>//home/data";
        })""")),
        NYT::TErrorException);
}

////////////////////////////////////////////////////////////////////////////////


} // namespace
} // namespace NYT::NFlow::NStaticTableConnector
