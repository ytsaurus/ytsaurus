#include "query_executor.h"
#include "private.h"
#include "config.h"

#include <yt/server/cell_node/bootstrap.h>
#include <yt/server/cell_node/config.h>

#include <yt/server/data_node/chunk_block_manager.h>
#include <yt/server/data_node/chunk.h>
#include <yt/server/data_node/chunk_registry.h>
#include <yt/server/data_node/local_chunk_reader.h>
#include <yt/server/data_node/master_connector.h>

#include <yt/server/hydra/hydra_manager.h>

#include <yt/server/tablet_node/config.h>
#include <yt/server/tablet_node/security_manager.h>
#include <yt/server/tablet_node/slot_manager.h>
#include <yt/server/tablet_node/tablet.h>
#include <yt/server/tablet_node/tablet_manager.h>
#include <yt/server/tablet_node/tablet_reader.h>
#include <yt/server/tablet_node/tablet_slot.h>
#include <yt/server/tablet_node/tablet_profiling.h>

#include <yt/ytlib/api/native/connection.h>
#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/chunk_client/block_cache.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/client/chunk_client/proto/chunk_spec.pb.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/replication_reader.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/client/object_client/helpers.h>

#include <yt/client/query_client/query_statistics.h>

#include <yt/ytlib/query_client/callbacks.h>
#include <yt/ytlib/query_client/column_evaluator.h>
#include <yt/ytlib/query_client/coordinator.h>
#include <yt/ytlib/query_client/evaluator.h>
#include <yt/ytlib/query_client/functions_cache.h>
#include <yt/ytlib/query_client/helpers.h>
#include <yt/ytlib/query_client/query.h>
#include <yt/ytlib/query_client/query_helpers.h>
#include <yt/ytlib/query_client/private.h>
#include <yt/ytlib/query_client/executor.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/ytlib/table_client/config.h>
#include <yt/ytlib/table_client/pipe.h>
#include <yt/ytlib/table_client/schemaful_chunk_reader.h>
#include <yt/client/table_client/schemaful_reader.h>
#include <yt/client/table_client/schemaful_writer.h>
#include <yt/ytlib/table_client/unordered_schemaful_reader.h>

#include <yt/ytlib/tablet_client/public.h>

#include <yt/ytlib/misc/memory_chunk_provider.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/string.h>
#include <yt/core/misc/collection_helpers.h>
#include <yt/core/misc/tls_cache.h>

namespace NYT {
namespace NQueryAgent {

using namespace NCellNode;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NDataNode;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NQueryClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTabletNode;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

TColumnFilter GetColumnFilter(const TTableSchema& desiredSchema, const TTableSchema& tabletSchema)
{
    // Infer column filter.
    TColumnFilter columnFilter;
    columnFilter.All = false;
    for (const auto& column : desiredSchema.Columns()) {
        const auto& tabletColumn = tabletSchema.GetColumnOrThrow(column.Name());
        if (tabletColumn.GetPhysicalType() != column.GetPhysicalType()) {
            THROW_ERROR_EXCEPTION("Mismatched type of column %Qv in schema: expected %Qlv, found %Qlv",
                column.Name(),
                tabletColumn.GetPhysicalType(),
                column.GetPhysicalType());
        }
        columnFilter.Indexes.push_back(tabletSchema.GetColumnIndex(tabletColumn));
    }

    return columnFilter;
}

struct TRangeFormatter
{
    void operator()(TStringBuilder* builder, const TRowRange& source) const
    {
        builder->AppendFormat("[%v .. %v]",
            source.first,
            source.second);
    }
};

struct TSelectCpuCounters
{
    explicit TSelectCpuCounters(const TTagIdList& list)
        : CpuTime("/select/cpu_time", list)
        , ChunkReaderStatisticsCounters("/select/chunk_reader_statistics", list)
    { }

    TMonotonicCounter CpuTime;
    TChunkReaderStatisticsCounters ChunkReaderStatisticsCounters;
};

using TSelectCpuProfilerTrait = TTabletProfilerTrait<TSelectCpuCounters>;

struct TSelectReadCounters
{
    explicit TSelectReadCounters(const TTagIdList& list)
        : RowCount("/select/row_count", list)
        , DataWeight("/select/data_weight", list)
        , UnmergedRowCount("/select/unmerged_row_count", list)
        , UnmergedDataWeight("/select/unmerged_data_weight", list)
        , DecompressionCpuTime("/select/decompression_cpu_time", list)
    { }

    TMonotonicCounter RowCount;
    TMonotonicCounter DataWeight;
    TMonotonicCounter UnmergedRowCount;
    TMonotonicCounter UnmergedDataWeight;
    TMonotonicCounter DecompressionCpuTime;
};

using TSelectReadProfilerTrait = TTabletProfilerTrait<TSelectReadCounters>;

class TProfilingReaderWrapper
    : public ISchemafulReader
{
private:
    ISchemafulReaderPtr Underlying_;
    NProfiling::TTagIdList Tags_;

public:
    TProfilingReaderWrapper(ISchemafulReaderPtr underlying, NProfiling::TTagIdList tags)
        : Underlying_(std::move(underlying))
        , Tags_(tags)
    { }

    virtual bool Read(std::vector<TUnversionedRow>* rows) override
    {
        return Underlying_->Read(rows);
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return Underlying_->GetReadyEvent();
    }

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        return Underlying_->GetDataStatistics();
    }

    virtual NChunkClient::TCodecStatistics GetDecompressionStatistics() const override
    {
        return Underlying_->GetDecompressionStatistics();
    }

    ~TProfilingReaderWrapper()
    {
        auto statistics = GetDataStatistics();
        auto decompressionCputTime = GetDecompressionStatistics().GetTotalDuration();
        auto& counters = GetLocallyGloballyCachedValue<TSelectReadProfilerTrait>(Tags_);
        TabletNodeProfiler.Increment(counters.RowCount, statistics.row_count());
        TabletNodeProfiler.Increment(counters.DataWeight, statistics.data_weight());
        TabletNodeProfiler.Increment(counters.UnmergedRowCount, statistics.unmerged_row_count());
        TabletNodeProfiler.Increment(counters.UnmergedDataWeight, statistics.unmerged_data_weight());
        TabletNodeProfiler.Increment(counters.DecompressionCpuTime, DurationToValue(decompressionCputTime));
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

struct TQuerySubexecutorBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

class TTabletSnapshotCache
{
public:
    TTabletSnapshotCache(
        TSlotManagerPtr slotManager,
        const NLogging::TLogger& logger)
        : SlotManager_(std::move(slotManager)),
        Logger(logger)
    { }

    void ValidateAndRegisterTabletSnapshot(
        const TTabletId& tabletId,
        const i64 mountRevision,
        const TTimestamp timestamp)
    {
        auto tabletSnapshot = SlotManager_->GetTabletSnapshotOrThrow(tabletId);

        tabletSnapshot->ValidateMountRevision(mountRevision);

        SlotManager_->ValidateTabletAccess(
            tabletSnapshot,
            NYTree::EPermission::Read,
            timestamp);

        Map_.insert(std::make_pair(tabletId, tabletSnapshot));

        if (!MultipleTables_) {
            if (TableId_ && tabletSnapshot->TableId != TableId_) {
                LOG_ERROR("Found different tables in query, profiling will be incorrect (TableId1: %v, TableId2: %v)",
                    TableId_,
                    tabletSnapshot->TableId);
                MultipleTables_ = true;
            }

            TableId_ = tabletSnapshot->TableId;
            ProfilerTags_ = tabletSnapshot->ProfilerTags;
        }
    }

    NProfiling::TTagIdList GetProfilerTags()
    {
        return MultipleTables_ ? NProfiling::TTagIdList() : ProfilerTags_;
    }

    TTabletSnapshotPtr GetCachedTabletSnapshot(const TTabletId& tabletId)
    {
        auto it = Map_.find(tabletId);
        YCHECK(it != Map_.end());
        return it->second;
    }

private:
    const TSlotManagerPtr SlotManager_;
    const NLogging::TLogger Logger;

    THashMap<TTabletId, TTabletSnapshotPtr> Map_;
    TObjectId TableId_;
    NProfiling::TTagIdList ProfilerTags_;
    bool MultipleTables_ = false;
};

template <class TIter>
TIter MergeOverlappingRanges(TIter begin, TIter end)
{
    if (begin == end) {
        return end;
    }

    auto it = begin;
    auto dest = it;
    ++it;

    for (; it != end; ++it) {
        if (dest->second < it->first) {
            *++dest = std::move(*it);
        } else if (dest->second < it->second) {
            dest->second = std::move(it->second);
        }
    }

    ++dest;
    return dest;
}

////////////////////////////////////////////////////////////////////////////////

class TQueryExecution
    : public TIntrinsicRefCounted
{
public:
    TQueryExecution(
        TQueryAgentConfigPtr config,
        TFunctionImplCachePtr functionImplCache,
        TBootstrap* const bootstrap,
        TColumnEvaluatorCachePtr columnEvaluatorCache,
        TEvaluatorPtr evaluator,
        TConstQueryPtr query,
        const TClientBlockReadOptions& blockReadOptions,
        const TQueryOptions& options)
        : Config_(std::move(config))
        , FunctionImplCache_(std::move(functionImplCache))
        , Bootstrap_(bootstrap)
        , ColumnEvaluatorCache_(std::move(columnEvaluatorCache))
        , Evaluator_(std::move(evaluator))
        , Query_(std::move(query))
        , Options_(std::move(options))
        , BlockReadOptions_(blockReadOptions)
        , Logger(MakeQueryLogger(Query_))
        , TabletSnapshots_(Bootstrap_->GetTabletSlotManager(), Logger)
        , Invoker_(Bootstrap_->GetQueryPoolInvoker(ToString(Options_.ReadSessionId)))
    { }

    TFuture<TQueryStatistics> Execute(
        TConstExternalCGInfoPtr externalCGInfo,
        std::vector<TDataRanges> dataSources,
        ISchemafulWriterPtr writer)
    {
        for (const auto& source : dataSources) {
            if (TypeFromId(source.Id) == EObjectType::Tablet) {
                TabletSnapshots_.ValidateAndRegisterTabletSnapshot(
                    source.Id,
                    source.MountRevision,
                    Options_.Timestamp);
            } else {
                THROW_ERROR_EXCEPTION("Unsupported data split type %Qlv",
                    TypeFromId(source.Id));
            }
        }

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        MaybeUser_ = securityManager->GetAuthenticatedUser();

        return BIND(&TQueryExecution::DoExecute, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run(
                std::move(externalCGInfo),
                std::move(dataSources),
                std::move(writer));
    }

private:
    const TQueryAgentConfigPtr Config_;
    const TFunctionImplCachePtr FunctionImplCache_;
    TBootstrap* const Bootstrap_;
    const TColumnEvaluatorCachePtr ColumnEvaluatorCache_;
    const TEvaluatorPtr Evaluator_;

    const TConstQueryPtr Query_;
    const TQueryOptions Options_;
    const TClientBlockReadOptions BlockReadOptions_;

    const NLogging::TLogger Logger;

    TTabletSnapshotCache TabletSnapshots_;
    const IInvokerPtr Invoker_;

    TNullable<TString> MaybeUser_;

    typedef std::function<ISchemafulReaderPtr()> TSubreaderCreator;

    void LogSplits(const std::vector<TDataRanges>& splits)
    {
        if (Options_.VerboseLogging) {
            for (const auto& split : splits) {
                LOG_DEBUG("Ranges in split %v: %v",
                    split.Id,
                    MakeFormattableRange(split.Ranges, TRangeFormatter()));
            }
        }
    }

    TQueryStatistics DoCoordinateAndExecute(
        TConstExternalCGInfoPtr externalCGInfo,
        ISchemafulWriterPtr writer,
        std::vector<TRefiner> refiners,
        std::vector<TSubreaderCreator> subreaderCreators,
        std::vector<std::vector<TDataRanges>> readRanges)
    {
        NApi::TClientOptions clientOptions;
        if (MaybeUser_) {
            clientOptions.User = MaybeUser_.Get();
        }

        auto client = Bootstrap_
            ->GetMasterClient()
            ->GetNativeConnection()
            ->CreateNativeClient(clientOptions);

        auto remoteExecutor = CreateQueryExecutor(
            client->GetNativeConnection(),
            Invoker_,
            ColumnEvaluatorCache_,
            Evaluator_,
            client->GetChannelFactory(),
            FunctionImplCache_);

        auto functionGenerators = New<TFunctionProfilerMap>();
        auto aggregateGenerators = New<TAggregateProfilerMap>();
        MergeFrom(functionGenerators.Get(), *BuiltinFunctionProfilers);
        MergeFrom(aggregateGenerators.Get(), *BuiltinAggregateProfilers);
        FetchFunctionImplementationsFromCypress(
            functionGenerators,
            aggregateGenerators,
            externalCGInfo,
            FunctionImplCache_,
            BlockReadOptions_);

        return CoordinateAndExecute(
            Query_,
            writer,
            refiners,
            [&] (TConstQueryPtr subquery, int index) {
                auto asyncSubqueryResults = std::make_shared<std::vector<TFuture<TQueryStatistics>>>();

                auto foreignProfileCallback = [
                    asyncSubqueryResults,
                    externalCGInfo,
                    remoteExecutor,
                    dataSplits = std::move(readRanges[index]),
                    this,
                    this_ = MakeStrong(this)
                ] (TQueryPtr subquery, TConstJoinClausePtr joinClause) -> TJoinSubqueryEvaluator {
                    auto remoteOptions = Options_;
                    remoteOptions.MaxSubqueries = 1;

                    auto verboseLogging = Options_.VerboseLogging;

                    size_t minKeyWidth = std::numeric_limits<size_t>::max();
                    for (const auto& split : dataSplits) {
                        minKeyWidth = std::min(minKeyWidth, split.KeyWidth);
                    }

                    LOG_DEBUG("Profiling (CommonKeyPrefix: %v, minKeyWidth: %v)",
                        joinClause->CommonKeyPrefix,
                        minKeyWidth);

                    if (joinClause->CommonKeyPrefix >= minKeyWidth && minKeyWidth > 0) {
                        auto rowBuffer = New<TRowBuffer>();

                        std::vector<TRowRange> prefixRanges;
                        std::vector<TRow> prefixKeys;
                        bool isRanges = false;
                        bool isKeys = false;

                        std::vector<EValueType> schema;
                        for (const auto& split : dataSplits) {
                            for (int index = 0; index < split.Ranges.Size(); ++index) {
                                isRanges = true;
                                YCHECK(!isKeys);
                                const auto& range = split.Ranges[index];
                                int lowerBoundWidth = std::min(
                                    GetSignificantWidth(range.first),
                                    joinClause->CommonKeyPrefix);

                                auto lowerBound = rowBuffer->AllocateUnversioned(lowerBoundWidth);
                                for (int column = 0; column < lowerBoundWidth; ++column) {
                                    lowerBound[column] = rowBuffer->Capture(range.first[column]);
                                }

                                int upperBoundWidth = std::min(
                                    GetSignificantWidth(range.second),
                                    joinClause->CommonKeyPrefix);

                                auto upperBound = rowBuffer->AllocateUnversioned(upperBoundWidth + 1);
                                for (int column = 0; column < upperBoundWidth; ++column) {
                                    upperBound[column] = rowBuffer->Capture(range.second[column]);
                                }

                                upperBound[upperBoundWidth] = MakeUnversionedSentinelValue(EValueType::Max);
                                prefixRanges.emplace_back(lowerBound, upperBound);

                                LOG_DEBUG_IF(verboseLogging, "Transforming range [%v .. %v] -> [%v .. %v]",
                                    range.first,
                                    range.second,
                                    lowerBound,
                                    upperBound);
                            }

                            schema = split.Schema;

                            for (int index = 0; index < split.Keys.Size(); ++index) {
                                isKeys = true;
                                YCHECK(!isRanges);
                                const auto& key = split.Keys[index];

                                int keyWidth = std::min(
                                    size_t(key.GetCount()),
                                    joinClause->CommonKeyPrefix);

                                auto prefixKey = rowBuffer->AllocateUnversioned(keyWidth);
                                for (int column = 0; column < keyWidth; ++column) {
                                    prefixKey[column] = rowBuffer->Capture(key[column]);
                                }
                                prefixKeys.emplace_back(prefixKey);
                            }
                        }

                        TDataRanges dataSource;
                        dataSource.Id = joinClause->ForeignDataId;

                        if (isRanges) {
                            prefixRanges.erase(
                                MergeOverlappingRanges(prefixRanges.begin(), prefixRanges.end()),
                                prefixRanges.end());
                            dataSource.Ranges = MakeSharedRange(prefixRanges, rowBuffer);
                        }

                        if (isKeys) {
                            prefixKeys.erase(std::unique(prefixKeys.begin(), prefixKeys.end()), prefixKeys.end());
                            dataSource.Keys = MakeSharedRange(prefixKeys, rowBuffer);
                            dataSource.Schema = schema;
                        }

                        // COMPAT(lukyan): Use ordered read without modification of protocol
                        subquery->Limit = std::numeric_limits<i64>::max() - 1;

                        LOG_DEBUG("Evaluating remote subquery (SubqueryId: %v)", subquery->Id);

                        auto pipe = New<NTableClient::TSchemafulPipe>();

                        auto asyncResult = remoteExecutor->Execute(
                            subquery,
                            externalCGInfo,
                            std::move(dataSource),
                            pipe->GetWriter(),
                            BlockReadOptions_,
                            remoteOptions);

                        asyncResult.Subscribe(BIND([pipe] (const TErrorOr<TQueryStatistics>& error) {
                            if (!error.IsOK()) {
                                pipe->Fail(error);
                            }
                        }));

                        asyncSubqueryResults->push_back(asyncResult);

                        return [
                            reader = pipe->GetReader()
                        ] (std::vector<TRow> keys, TRowBufferPtr permanentBuffer) {
                            return reader;
                        };
                    } else {
                        return [
                            asyncSubqueryResults,
                            externalCGInfo,
                            remoteExecutor,
                            subquery,
                            joinClause,
                            remoteOptions,
                            this,
                            this_ = MakeStrong(this)
                        ] (std::vector<TRow> keys, TRowBufferPtr permanentBuffer) mutable {
                            TDataRanges dataSource;
                            std::tie(subquery, dataSource) = GetForeignQuery(
                                subquery,
                                joinClause,
                                std::move(keys),
                                permanentBuffer);

                            LOG_DEBUG("Evaluating remote subquery (SubqueryId: %v)", subquery->Id);

                            auto pipe = New<NTableClient::TSchemafulPipe>();

                            auto asyncResult = remoteExecutor->Execute(
                                subquery,
                                externalCGInfo,
                                std::move(dataSource),
                                pipe->GetWriter(),
                                BlockReadOptions_,
                                remoteOptions);

                            asyncResult.Subscribe(BIND([pipe] (const TErrorOr<TQueryStatistics>& error) {
                                if (!error.IsOK()) {
                                    pipe->Fail(error);
                                }
                            }));

                            asyncSubqueryResults->push_back(asyncResult);

                            return pipe->GetReader();
                        };
                    }
                };

                auto mergingReader = subreaderCreators[index]();

                LOG_DEBUG("Evaluating subquery (SubqueryId: %v)", subquery->Id);

                auto pipe = New<TSchemafulPipe>();

                auto asyncStatistics = BIND(&TEvaluator::Run, Evaluator_)
                    .AsyncVia(Invoker_)
                    .Run(
                        subquery,
                        mergingReader,
                        pipe->GetWriter(),
                        foreignProfileCallback,
                        functionGenerators,
                        aggregateGenerators,
                        Options_);

                asyncStatistics = asyncStatistics.Apply(BIND([
                    =,
                    this_ = MakeStrong(this)
                ] (const TErrorOr<TQueryStatistics>& result) -> TFuture<TQueryStatistics>
                {
                    if (!result.IsOK()) {
                        pipe->Fail(result);
                        LOG_DEBUG(result, "Failed evaluating subquery (SubqueryId: %v)", subquery->Id);
                        return MakeFuture(result);
                    } else {
                        TQueryStatistics statistics = result.Value();

                        return Combine(*asyncSubqueryResults)
                        .Apply(BIND([
                            =,
                            this_ = MakeStrong(this)
                        ] (const std::vector<TQueryStatistics>& subqueryResults) mutable {
                            for (const auto& subqueryResult : subqueryResults) {
                                LOG_DEBUG("Remote subquery statistics %v", subqueryResult);
                                statistics.AddInnerStatistics(subqueryResult);
                            }
                            return statistics;
                        }));
                    }
                }));

                return std::make_pair(pipe->GetReader(), asyncStatistics);
            },
            [&] (TConstFrontQueryPtr topQuery, ISchemafulReaderPtr reader, ISchemafulWriterPtr writer) {
                LOG_DEBUG("Evaluating top query (TopQueryId: %v)", topQuery->Id);
                auto result = Evaluator_->Run(
                    topQuery,
                    std::move(reader),
                    std::move(writer),
                    nullptr,
                    functionGenerators,
                    aggregateGenerators,
                    Options_);
                LOG_DEBUG("Finished evaluating top query (TopQueryId: %v)", topQuery->Id);
                return result;
            });
    }

    TQueryStatistics DoExecute(
        TConstExternalCGInfoPtr externalCGInfo,
        std::vector<TDataRanges> dataSources,
        ISchemafulWriterPtr writer)
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager, MaybeUser_);

        auto statistics = DoExecuteImpl(std::move(externalCGInfo), std::move(dataSources), std::move(writer));

        auto profilerTags = MaybeAddUserTag(TabletSnapshots_.GetProfilerTags());
        if (!profilerTags.empty()) {
            auto& counters = GetLocallyGloballyCachedValue<TSelectCpuProfilerTrait>(profilerTags);
            TabletNodeProfiler.Increment(counters.CpuTime, DurationToValue(statistics.SyncTime));
            counters.ChunkReaderStatisticsCounters.Increment(TabletNodeProfiler, BlockReadOptions_.ChunkReaderStatistics);
        }

        return statistics;
    }

    TQueryStatistics DoExecuteImpl(
        TConstExternalCGInfoPtr externalCGInfo,
        std::vector<TDataRanges> dataSources,
        ISchemafulWriterPtr writer)
    {
        LOG_DEBUG("Classifying data sources into ranges and lookup keys");

        std::vector<TDataRanges> rangesByTablet;

        auto rowBuffer = New<TRowBuffer>(TQuerySubexecutorBufferTag());

        auto keySize = Query_->OriginalSchema.GetKeyColumnCount();

        std::vector<EValueType> keySchema;
        for (size_t index = 0; index < keySize; ++index) {
            keySchema.push_back(Query_->OriginalSchema.Columns()[index].GetPhysicalType());
        }

        size_t rangesCount = 0;
        for (const auto& source : dataSources) {
            TRowRanges rowRanges;
            std::vector<TRow> keys;

            auto pushRanges = [&] () {
                if (!rowRanges.empty()) {
                    rangesCount += rowRanges.size();
                    TDataRanges item;
                    item.Id = source.Id;
                    item.KeyWidth = source.KeyWidth;
                    item.Ranges = MakeSharedRange(std::move(rowRanges), source.Ranges.GetHolder(), rowBuffer);
                    item.LookupSupported = source.LookupSupported;
                    rangesByTablet.emplace_back(std::move(item));
                }
            };

            auto pushKeys = [&] () {
                if (!keys.empty()) {
                    TDataRanges item;
                    item.Id = source.Id;
                    item.KeyWidth = source.KeyWidth;
                    item.Keys = MakeSharedRange(std::move(keys), source.Ranges.GetHolder());
                    item.Schema = keySchema;
                    item.LookupSupported = source.LookupSupported;
                    rangesByTablet.emplace_back(std::move(item));
                }
            };

            for (const auto& range : source.Ranges) {
                auto lowerBound = range.first;
                auto upperBound = range.second;

                if (source.LookupSupported &&
                    keySize == lowerBound.GetCount() &&
                    keySize + 1 == upperBound.GetCount() &&
                    upperBound[keySize].Type == EValueType::Max &&
                    CompareRows(lowerBound.Begin(), lowerBound.End(), upperBound.Begin(), upperBound.Begin() + keySize) == 0)
                {
                    pushRanges();
                    keys.push_back(lowerBound);
                } else {
                    pushKeys();
                    rowRanges.push_back(range);
                }
            }

            for (const auto& key : source.Keys) {
                auto rowSize = key.GetCount();
                if (source.LookupSupported &&
                    keySize == key.GetCount())
                {
                    pushRanges();
                    keys.push_back(key);
                } else {
                    auto lowerBound = key;

                    auto upperBound = rowBuffer->AllocateUnversioned(rowSize + 1);
                    for (int column = 0; column < rowSize; ++column) {
                        upperBound[column] = lowerBound[column];
                    }

                    upperBound[rowSize] = MakeUnversionedSentinelValue(EValueType::Max);
                    pushKeys();
                    rowRanges.emplace_back(lowerBound, upperBound);
                }
            }
            pushRanges();
            pushKeys();
        }

        LOG_DEBUG("Splitting %v ranges", rangesCount);

        auto splits = Split(std::move(rangesByTablet), rowBuffer);

        std::vector<TRefiner> refiners;
        std::vector<TSubreaderCreator> subreaderCreators;
        std::vector<std::vector<TDataRanges>> readRanges;

        auto processSplitsRanges = [&] (int beginIndex, int endIndex) {
            if (beginIndex == endIndex) {
                return;
            }

            std::vector<TDataRanges> groupedSplit(splits.begin() + beginIndex, splits.begin() + endIndex);
            readRanges.push_back(groupedSplit);
            std::vector<TRowRange> keyRanges;
            for (const auto& dataRange : groupedSplit) {
                keyRanges.insert(keyRanges.end(), dataRange.Ranges.Begin(), dataRange.Ranges.End());
            }

            refiners.push_back([MOVE(keyRanges), inferRanges = Query_->InferRanges] (
                TConstExpressionPtr expr,
                const TKeyColumns& keyColumns)
            {
                if (inferRanges) {
                    return EliminatePredicate(keyRanges, expr, keyColumns);
                } else {
                    return expr;
                }
            });
            subreaderCreators.push_back([&, MOVE(groupedSplit)] () {
                size_t rangesCount = std::accumulate(
                    groupedSplit.begin(),
                    groupedSplit.end(),
                    0,
                    [] (size_t sum, const TDataRanges& element) {
                        return sum + element.Ranges.Size();
                    });
                LOG_DEBUG("Generating reader for %v splits from %v ranges",
                    groupedSplit.size(),
                    rangesCount);

                LogSplits(groupedSplit);

                auto bottomSplitReaderGenerator = [
                    MOVE(groupedSplit),
                    index = 0,
                    this,
                    this_ = MakeStrong(this)
                ] () mutable -> ISchemafulReaderPtr {
                    if (index == groupedSplit.size()) {
                        return nullptr;
                    }

                    const auto& group = groupedSplit[index++];
                    return GetMultipleRangesReader(group.Id, group.Ranges);
                };

                return CreatePrefetchingOrderedSchemafulReader(std::move(bottomSplitReaderGenerator));
            });
        };

        auto processSplitKeys = [&] (int index) {
            const auto& tabletId = splits[index].Id;
            auto& keys = splits[index].Keys;

            readRanges.push_back({splits[index]});

            refiners.push_back([&, inferRanges = Query_->InferRanges] (
                TConstExpressionPtr expr, const
                TKeyColumns& keyColumns)
            {
                if (inferRanges) {
                    return EliminatePredicate(keys, expr, keyColumns);
                } else {
                    return expr;
                }
            });
            subreaderCreators.push_back([&, MOVE(keys)] () {
                return GetTabletReader(tabletId, keys);
            });
        };

        int splitCount = splits.size();
        auto maxSubqueries = std::min({Options_.MaxSubqueries, Config_->MaxSubqueries, splitCount});
        int splitOffset = 0;
        int queryIndex = 1;
        int nextSplitOffset = queryIndex * splitCount / maxSubqueries;
        for (size_t splitIndex = 0; splitIndex < splitCount;) {
            if (splits[splitIndex].Keys) {
                processSplitsRanges(splitOffset, splitIndex);
                processSplitKeys(splitIndex);
                splitOffset = ++splitIndex;
            } else {
                ++splitIndex;
            }

            if (splitIndex == nextSplitOffset) {
                processSplitsRanges(splitOffset, nextSplitOffset);
                splitOffset = nextSplitOffset;
                ++queryIndex;
                nextSplitOffset = queryIndex * splitCount / maxSubqueries;
            }
        }

        YCHECK(splitOffset == splitCount);

        return DoCoordinateAndExecute(
            externalCGInfo,
            std::move(writer),
            std::move(refiners),
            std::move(subreaderCreators),
            std::move(readRanges));
    }

    std::vector<TSharedRange<TRowRange>> SplitTablet(
        const std::vector<TPartitionSnapshotPtr>& partitions,
        TSharedRange<TRowRange> ranges,
        TRowBufferPtr rowBuffer)
    {
        auto verboseLogging = Options_.VerboseLogging;

        auto holder = MakeHolder(ranges.GetHolder(), rowBuffer);

        TRow lowerCapBound = rowBuffer->Capture(partitions.front()->PivotKey);
        TRow upperCapBound = rowBuffer->Capture(partitions.back()->NextPivotKey);

        struct TGroup
        {
            std::vector<TPartitionSnapshotPtr>::const_iterator PartitionIt;
            TSharedRange<TRowRange>::iterator BeginIt;
            TSharedRange<TRowRange>::iterator EndIt;
        };

        std::vector<TGroup> groupedByPartitions;

        auto appendGroup = [&] (const TGroup& group) {
            if (!groupedByPartitions.empty() && groupedByPartitions.back().PartitionIt == group.PartitionIt) {
                Y_ASSERT(groupedByPartitions.back().EndIt < group.EndIt);
                groupedByPartitions.back().EndIt = group.EndIt;
            } else {
                groupedByPartitions.push_back(group);
            }
        };

        for (auto rangesIt = begin(ranges); rangesIt != end(ranges);) {
            auto lowerBound = std::max(rangesIt->first, lowerCapBound);
            auto upperBound = std::min(rangesIt->second, upperCapBound);

            if (lowerBound >= upperBound) {
                ++rangesIt;
                continue;
            }

            // Run binary search to find the relevant partitions.
            auto startIt = std::upper_bound(
                partitions.begin(),
                partitions.end(),
                lowerBound,
                [] (TKey lhs, const TPartitionSnapshotPtr& rhs) {
                    return lhs < rhs->NextPivotKey;
                });
            YCHECK(startIt != partitions.end());

            auto nextPivotKey = (*startIt)->NextPivotKey.Get();

            if (upperBound < nextPivotKey) {
                auto rangesItEnd = std::upper_bound(
                    rangesIt,
                    end(ranges),
                    nextPivotKey,
                    [] (TKey key, const TRowRange& rowRange) {
                        return key < rowRange.second;
                    });

                appendGroup(TGroup{
                    startIt,
                    rangesIt,
                    rangesItEnd});
                rangesIt = rangesItEnd;
            } else {
                auto nextRangeIt = rangesIt;
                ++nextRangeIt;

                for (auto it = startIt; it != partitions.end() && (*it)->PivotKey < upperBound; ++it) {
                    appendGroup(TGroup{
                        it,
                        rangesIt,
                        nextRangeIt});
                }
                rangesIt = nextRangeIt;
            }
        }

        auto iterate = [&] (auto onRanges, auto onSamples) {
            for (const auto& group : groupedByPartitions) {
                // calculate touched sample count

                auto partitionIt = group.PartitionIt;
                const auto& sampleKeys = (*partitionIt)->SampleKeys->Keys;

                TRow pivot = rowBuffer->Capture((*partitionIt)->PivotKey);
                TRow nextPivot = rowBuffer->Capture((*partitionIt)->NextPivotKey);

                LOG_DEBUG_IF(verboseLogging, "Iterating over partition (%v .. %v): [%v .. %v]",
                    pivot,
                    nextPivot,
                    group.BeginIt - begin(ranges),
                    group.EndIt - begin(ranges));

                for (auto rangesIt = group.BeginIt; rangesIt != group.EndIt;) {
                    auto lowerBound = rangesIt == group.BeginIt
                        ? std::max(rangesIt->first, pivot)
                        : rangesIt->first;
                    auto upperBound = rangesIt + 1 == group.EndIt
                        ? std::min(rangesIt->second, nextPivot)
                        : rangesIt->second;

                    auto startSampleIt = std::upper_bound(sampleKeys.begin(), sampleKeys.end(), lowerBound);

                    auto nextPivotKey = startSampleIt == sampleKeys.end()
                        ? (*partitionIt)->NextPivotKey.Get()
                        : *startSampleIt;

                    if (upperBound < nextPivotKey) {
                        auto rangesItEnd = std::upper_bound(
                            rangesIt,
                            group.EndIt,
                            nextPivotKey,
                            [] (TKey key, const TRowRange& rowRange) {
                                return key < rowRange.second;
                            });
                        onRanges(rangesIt, rangesItEnd, pivot, nextPivot);
                        rangesIt = rangesItEnd;
                    } else {
                        auto endSampleIt = std::lower_bound(startSampleIt, sampleKeys.end(), upperBound);
                        onSamples(rangesIt, startSampleIt, endSampleIt, pivot, nextPivot);
                        ++rangesIt;
                    }
                }
            }
        };

        size_t totalSampleCount = 0;
        size_t totalBatchCount = 0;
        iterate(
            [&] (auto rangesIt, auto rangesItEnd, auto pivot, auto nextPivot) {
                ++totalBatchCount;
            },
            [&] (auto rangesIt, auto startSampleIt, auto endSampleIt, auto pivot, auto nextPivot) {
                ++totalBatchCount;
                totalSampleCount += std::distance(startSampleIt, endSampleIt);
            });

        size_t freeSlotCount = Config_->MaxSubsplitsPerTablet > totalBatchCount
            ? Config_->MaxSubsplitsPerTablet - totalBatchCount
            : 0;
        size_t cappedSampleCount = std::min(freeSlotCount, totalSampleCount);

        LOG_DEBUG_IF(verboseLogging, "Total sample count: %v", totalSampleCount);
        LOG_DEBUG_IF(verboseLogging, "Capped sample count: %v", cappedSampleCount);

        size_t sampleIndex = 0;
        size_t nextSampleCount;
        auto incrementSampleIndex = [&] {
            ++sampleIndex;
            nextSampleCount = cappedSampleCount != 0
                ? sampleIndex * totalSampleCount / cappedSampleCount
                : totalSampleCount;
        };

        incrementSampleIndex();

        size_t currentSampleCount = 0;

        std::vector<TSharedRange<TRowRange>> groupedSplits;
        std::vector<TRowRange> group;

        auto addGroup = [&] () {
            YCHECK(!group.empty());
            LOG_DEBUG_IF(verboseLogging, "(%v, %v) make batch [%v .. %v] from %v ranges",
                currentSampleCount,
                nextSampleCount,
                group.front().second,
                group.back().second,
                group.size());
            groupedSplits.push_back(MakeSharedRange(std::move(group), holder));
        };

        iterate(
            [&] (auto rangesIt, auto rangesItEnd, auto pivot, auto nextPivot) {
                for (auto it = rangesIt; it != rangesItEnd; ++it) {
                    auto lowerBound = it == rangesIt ? std::max(it->first, pivot) : it->first;
                    auto upperBound = it + 1 == rangesItEnd ? std::min(it->second, nextPivot) : it->second;

                    group.emplace_back(lowerBound, upperBound);
                }
                addGroup();
            },
            [&] (auto rangesIt, auto startSampleIt, auto endSampleIt, auto pivot, auto nextPivot) {
                TRow lowerBound = std::max(rangesIt->first, pivot);
                TRow upperBound = std::min(rangesIt->second, nextPivot);

                auto currentBound = lowerBound;

                size_t sampleCount = std::distance(startSampleIt, endSampleIt);
                size_t nextGroupSampleCount = currentSampleCount + sampleCount;

                YCHECK(nextSampleCount >= currentSampleCount);

                auto it = startSampleIt;
                while (nextSampleCount < nextGroupSampleCount) {
                    size_t step = nextSampleCount - currentSampleCount;
                    it += step;

                    auto nextBound = rowBuffer->Capture(*it);
                    group.emplace_back(currentBound, nextBound);
                    currentBound = nextBound;

                    addGroup();
                    currentSampleCount += step;
                    incrementSampleIndex();
                }

                group.emplace_back(currentBound, upperBound);

                addGroup();
                currentSampleCount = nextGroupSampleCount;
            });

        return groupedSplits;
    }

    std::vector<TDataRanges> Split(std::vector<TDataRanges> rangesByTablet, TRowBufferPtr rowBuffer)
    {
        std::vector<TDataRanges> groupedSplits;

        for (auto& tablePartIdRange : rangesByTablet) {
            auto tablePartId = tablePartIdRange.Id;
            auto& ranges = tablePartIdRange.Ranges;

            auto tabletSnapshot = TabletSnapshots_.GetCachedTabletSnapshot(tablePartId);

            YCHECK(tablePartIdRange.Keys.Empty() != ranges.Empty());

            if (!tabletSnapshot->TableSchema.IsSorted() || ranges.Empty()) {
                groupedSplits.push_back(tablePartIdRange);
                continue;
            }

            YCHECK(std::is_sorted(
                ranges.Begin(),
                ranges.End(),
                [] (const TRowRange& lhs, const TRowRange& rhs) {
                    return lhs.first < rhs.first;
                }));

            const auto& partitions = tabletSnapshot->PartitionList;
            YCHECK(!partitions.empty());

            auto splits = SplitTablet(partitions, ranges, rowBuffer);

            for (const auto& split : splits) {
                TDataRanges dataRanges;

                dataRanges.Id = tablePartId;
                dataRanges.KeyWidth = tablePartIdRange.KeyWidth;
                dataRanges.Ranges = split;
                dataRanges.LookupSupported = tablePartIdRange.LookupSupported;

                groupedSplits.push_back(std::move(dataRanges));
            }
        }

        for (const auto& split : groupedSplits) {
            YCHECK(std::is_sorted(
                split.Ranges.Begin(),
                split.Ranges.End(),
                [] (const TRowRange& lhs, const TRowRange& rhs) {
                    return lhs.second <= rhs.first;
                }));
        }

        YCHECK(std::is_sorted(
            groupedSplits.begin(),
            groupedSplits.end(),
            [] (const TDataRanges& lhs, const TDataRanges& rhs) {
                const auto& lhsValue = lhs.Ranges ? lhs.Ranges.Back().second : lhs.Keys.Back();
                const auto& rhsValue = rhs.Ranges ? rhs.Ranges.Front().first : rhs.Keys.Front();

                return lhsValue <= rhsValue;
            }));
        return groupedSplits;
    }

    ISchemafulReaderPtr GetMultipleRangesReader(
        const TObjectId& tabletId,
        const TSharedRange<TRowRange>& bounds)
    {
        auto tabletSnapshot = TabletSnapshots_.GetCachedTabletSnapshot(tabletId);
        auto columnFilter = GetColumnFilter(Query_->GetReadSchema(), tabletSnapshot->QuerySchema);
        auto profilerTags = tabletSnapshot->ProfilerTags;

        ISchemafulReaderPtr reader;

        if (!tabletSnapshot->TableSchema.IsSorted()) {
            auto bottomSplitReaderGenerator = [
                MOVE(tabletSnapshot),
                columnFilter,
                MOVE(bounds),
                index = 0,
                this,
                this_ = MakeStrong(this)
            ] () mutable -> ISchemafulReaderPtr {
                if (index == bounds.Size()) {
                    return nullptr;
                }

                const auto& range = bounds[index++];

                TOwningKey lowerBound(range.first);
                TOwningKey upperBound(range.second);

                return CreateSchemafulOrderedTabletReader(
                    tabletSnapshot,
                    columnFilter,
                    lowerBound,
                    upperBound,
                    Options_.Timestamp,
                    BlockReadOptions_);
            };

            reader = CreateUnorderedSchemafulReader(std::move(bottomSplitReaderGenerator), 1);
        } else {
            reader = CreateSchemafulSortedTabletReader(
                std::move(tabletSnapshot),
                columnFilter,
                bounds,
                Options_.Timestamp,
                BlockReadOptions_);
        }

        return New<TProfilingReaderWrapper>(reader, MaybeAddUserTag(profilerTags));
    }

    ISchemafulReaderPtr GetTabletReader(
        const TTabletId& tabletId,
        const TSharedRange<TRow>& keys)
    {
        auto tabletSnapshot = TabletSnapshots_.GetCachedTabletSnapshot(tabletId);
        auto columnFilter = GetColumnFilter(Query_->GetReadSchema(), tabletSnapshot->QuerySchema);
        auto profilerTags = tabletSnapshot->ProfilerTags;

        auto reader = CreateSchemafulTabletReader(
            std::move(tabletSnapshot),
            columnFilter,
            keys,
            Options_.Timestamp,
            BlockReadOptions_);

        return New<TProfilingReaderWrapper>(reader, MaybeAddUserTag(profilerTags));
    }

    TTagIdList MaybeAddUserTag(const TTagIdList& tags)
    {
        if (MaybeUser_) {
            return AddUserTag(*MaybeUser_, tags);
        } else {
            return tags;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TQuerySubexecutor
    : public ISubexecutor
{
public:
    TQuerySubexecutor(
        TQueryAgentConfigPtr config,
        TBootstrap* bootstrap)
        : Config_(config)
        , FunctionImplCache_(CreateFunctionImplCache(
            config->FunctionImplCache,
            bootstrap->GetMasterClient()))
        , Bootstrap_(bootstrap)
        , Evaluator_(New<TEvaluator>(
            Config_,
            QueryAgentProfiler,
            New<TPooledMemoryChunkProvider<PoolChunkSize, EMemoryCategory::QueryCache>>(
                EMemoryCategory::Query,
                Bootstrap_->GetMemoryUsageTracker())))
        , ColumnEvaluatorCache_(Bootstrap_
            ->GetMasterClient()
            ->GetNativeConnection()
            ->GetColumnEvaluatorCache())
    { }

    // ISubexecutor implementation.
    virtual TFuture<TQueryStatistics> Execute(
        TConstQueryPtr query,
        TConstExternalCGInfoPtr externalCGInfo,
        std::vector<TDataRanges> dataSources,
        ISchemafulWriterPtr writer,
        const TClientBlockReadOptions& blockReadOptions,
        const TQueryOptions& options) override
    {
        ValidateReadTimestamp(options.Timestamp);

        auto execution = New<TQueryExecution>(
            Config_,
            FunctionImplCache_,
            Bootstrap_,
            ColumnEvaluatorCache_,
            Evaluator_,
            std::move(query),
            blockReadOptions,
            options);

        return execution->Execute(
            std::move(externalCGInfo),
            std::move(dataSources),
            std::move(writer));
    }

private:
    const TQueryAgentConfigPtr Config_;
    const TFunctionImplCachePtr FunctionImplCache_;
    TBootstrap* const Bootstrap_;
    const TEvaluatorPtr Evaluator_;
    const TColumnEvaluatorCachePtr ColumnEvaluatorCache_;

};

ISubexecutorPtr CreateQuerySubexecutor(
    TQueryAgentConfigPtr config,
    TBootstrap* bootstrap)
{
    return New<TQuerySubexecutor>(config, bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT

