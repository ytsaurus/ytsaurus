#include "join_profiler.h"

#include <yt/yt/library/query/base/coordination_helpers.h>
#include <yt/yt/library/query/base/helpers.h>
#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_helpers.h>

#include <yt/yt/client/query_client/query_statistics.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/pipe.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/unversioned_writer.h>

namespace NYT::NQueryClient {

using namespace NLogging;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

DECLARE_REFCOUNTED_CLASS(TSimpleRowsetWriter)
DECLARE_REFCOUNTED_CLASS(TRowsetSubrangeReader)

class TSimpleRowsetWriter
    : public IUnversionedRowsetWriter
{
public:
    explicit TSimpleRowsetWriter(IMemoryChunkProviderPtr chunkProvider)
        : RowBuffer_(New<TRowBuffer>(TSchemafulRowsetWriterBufferTag(), std::move(chunkProvider)))
    { }

    TSharedRange<TUnversionedRow> GetRows() const
    {
        return MakeSharedRange(Rows_, RowBuffer_);
    }

    TFuture<TSharedRange<TUnversionedRow>> GetResult() const
    {
        return Result_.ToFuture();
    }

    TFuture<void> Close() override
    {
        Result_.TrySet(GetRows());
        return VoidFuture;
    }

    bool Write(TRange<TUnversionedRow> rows) override
    {
        for (auto row : rows) {
            Rows_.push_back(RowBuffer_->CaptureRow(row));
        }
        return true;
    }

    TFuture<void> GetReadyEvent() override
    {
        return VoidFuture;
    }

    void Fail(const TError& error)
    {
        Result_.TrySet(error);
    }

    std::optional<NCrypto::TMD5Hash> GetDigest() const override
    {
        return std::nullopt;
    }

private:
    struct TSchemafulRowsetWriterBufferTag
    { };

    const TPromise<TSharedRange<TUnversionedRow>> Result_ = NewPromise<TSharedRange<TUnversionedRow>>();
    const TRowBufferPtr RowBuffer_;
    std::vector<TUnversionedRow> Rows_;
};

class TRowsetSubrangeReader
    : public ISchemafulUnversionedReader
{
public:
    TRowsetSubrangeReader(
        TFuture<TSharedRange<TUnversionedRow>> asyncRows,
        std::optional<std::pair<TKeyBoundRef, TKeyBoundRef>> readRange)
        : AsyncRows_(std::move(asyncRows))
        , ReadRange_(std::move(readRange))
    { }

    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions& options) override
    {
        if (!ReadRange_) {
            return nullptr;
        }
        auto readRange = *ReadRange_;

        if (!AsyncRows_.IsSet() || !AsyncRows_.Get().IsOK()) {
            return CreateEmptyUnversionedRowBatch();
        }

        const auto& rows = AsyncRows_.Get().Value();

        CurrentRowIndex_ = BinarySearch(CurrentRowIndex_, std::ssize(rows), [&] (i64 index) {
            return !TestKeyWithWidening(
                ToKeyRef(rows[index]),
                readRange.first);
        });

        auto startIndex = CurrentRowIndex_;

        CurrentRowIndex_ = std::min(CurrentRowIndex_ + options.MaxRowsPerRead, std::ssize(rows));

        CurrentRowIndex_ = BinarySearch(startIndex, CurrentRowIndex_, [&] (i64 index) {
            return TestKeyWithWidening(
                ToKeyRef(rows[index]),
                readRange.second);
        });

        if (startIndex == CurrentRowIndex_) {
            return nullptr;
        }

        return CreateBatchFromUnversionedRows(MakeSharedRange(rows.Slice(startIndex, CurrentRowIndex_), rows));
    }

    TFuture<void> GetReadyEvent() const override
    {
        return AsyncRows_.AsVoid();
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        return NChunkClient::NProto::TDataStatistics();
    }

    NChunkClient::TCodecStatistics GetDecompressionStatistics() const override
    {
        return NChunkClient::TCodecStatistics();
    }

    bool IsFetchingCompleted() const override
    {
        return false;
    }

    std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override
    {
        return {};
    }

private:
    TFuture<TSharedRange<TUnversionedRow>> AsyncRows_;
    i64 CurrentRowIndex_ = 0;
    std::optional<std::pair<TKeyBoundRef, TKeyBoundRef>> ReadRange_;
};

DEFINE_REFCOUNTED_TYPE(TSimpleRowsetWriter)
DEFINE_REFCOUNTED_TYPE(TRowsetSubrangeReader)

} // namespace

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TForeignJoinRowsPrefetcher)

class TForeignJoinRowsPrefetcher
    : public IJoinRowsProducer
{
public:
    TForeignJoinRowsPrefetcher(
        TFuture<TSharedRange<TRow>> asyncRows,
        size_t foreignKeyPrefix)
        : AsyncRows_(std::move(asyncRows))
        , ForeignKeyPrefix_(foreignKeyPrefix)
    { }

    ISchemafulUnversionedReaderPtr FetchJoinedRows(std::vector<TRow> keys, TRowBufferPtr /*permanentBuffer*/) override {
        std::optional<std::pair<TKeyBoundRef, TKeyBoundRef>> readRange;
        if (!keys.empty()) {
            readRange = {
                TKeyBoundRef(keys.front().FirstNElements(ForeignKeyPrefix_), /*inclusive*/ true, /*upper*/ false),
                TKeyBoundRef(keys.back().FirstNElements(ForeignKeyPrefix_), /*inclusive*/ true, /*upper*/ true)};
        }

        return New<TRowsetSubrangeReader>(AsyncRows_, std::move(readRange));
    }

private:
    const TFuture<TSharedRange<TRow>> AsyncRows_;
    const size_t ForeignKeyPrefix_;
};

DEFINE_REFCOUNTED_TYPE(TForeignJoinRowsPrefetcher)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TJoinSubqueryProfiler)

IJoinRowsProducerPtr MakeJoinRowsFetcher(TConstJoinClausePtr joinClause, TJoinSubqueryProfilerPtr profiler);

class TJoinSubqueryProfiler
    : public IJoinSubqueryProfiler
{
public:
    TJoinSubqueryProfiler(
        TConstQueryPtr query,
        const TQueryOptions& queryOptions,
        size_t minKeyWidth,
        bool orderedExecution,
        IMemoryChunkProviderPtr memoryChunkProvider,
        TConsumeSubqueryStatistics consumeSubqueryStatistics,
        TGetMergeJoinDataSource getMergeJoinDataSource,
        TExecuteForeign executeForeign,
        TLogger logger)
        : Query_(std::move(query))
        , QueryOptions_(queryOptions)
        , MinKeyWidth_(minKeyWidth)
        , OrderedExecution_(orderedExecution)
        , MemoryChunkProvider_(std::move(memoryChunkProvider))
        , ConsumeSubqueryStatistics_(std::move(consumeSubqueryStatistics))
        , GetMergeJoinDataSourceCallback_(std::move(getMergeJoinDataSource))
        , ExecuteForeignCallback_(std::move(executeForeign))
        , Logger(std::move(logger))
    { }

    IJoinRowsProducerPtr Profile(int joinIndex)
    {
        auto joinClause = Query_->JoinClauses[joinIndex];

        auto definedKeyColumns = Query_->GetRenamedSchema()->Columns();
        definedKeyColumns.resize(MinKeyWidth_);

        auto lhsTableWhereClause = SplitPredicateByColumnSubset(Query_->WhereClause, *Query_->GetRenamedSchema()).first;
        bool lhsQueryCanBeSelective = SplitPredicateByColumnSubset(lhsTableWhereClause, TTableSchema(definedKeyColumns))
            .second->As<TLiteralExpression>() == nullptr;
        bool inferredRangesCompletelyDefineRhsRanges = joinClause->CommonKeyPrefix >= MinKeyWidth_ && MinKeyWidth_ > 0;
        bool canUseMergeJoin = inferredRangesCompletelyDefineRhsRanges && !OrderedExecution_ && !lhsQueryCanBeSelective;

        YT_LOG_DEBUG("Profiling query (CommonKeyPrefix: %v, MinKeyWidth: %v, LhsQueryCanBeSelective: %v)",
            joinClause->CommonKeyPrefix,
            MinKeyWidth_,
            lhsQueryCanBeSelective);

        if (canUseMergeJoin) {
            auto dataSource = GetMergeJoinDataSourceCallback_(joinClause->CommonKeyPrefix);
            dataSource.ObjectId = joinClause->ForeignObjectId;
            dataSource.CellId = joinClause->ForeignCellId;

            auto joinSubquery = joinClause->GetJoinSubquery();
            joinSubquery->InferRanges = false;
            // COMPAT(lukyan): Use ordered read without modification of protocol
            joinSubquery->Limit = OrderedReadWithPrefetchHint;

            YT_LOG_DEBUG("Evaluating remote subquery (SubqueryId: %v)", joinSubquery->Id);

            auto writer = New<TSimpleRowsetWriter>(MemoryChunkProvider_);

            ExecuteForeignCallback_(
                TPlanFragment{
                    .Query = std::move(joinSubquery),
                    .DataSource = std::move(dataSource),
                },
                writer)
                .SubscribeUnique(BIND([this, this_ = MakeStrong(this), writer] (TErrorOr<TQueryStatistics>&& error) {
                    if (!error.IsOK()) {
                        writer->Fail(error);
                    } else {
                        ConsumeSubqueryStatistics_(std::move(error.Value()));
                    }
                }));

            return New<TForeignJoinRowsPrefetcher>(writer->GetResult(), joinClause->ForeignKeyPrefix);
        } else {
            return MakeJoinRowsFetcher(joinClause, MakeStrong(this));
        }
    }

private:
    const TConstQueryPtr Query_;
    const TQueryOptions QueryOptions_;
    const size_t MinKeyWidth_;
    const bool OrderedExecution_;
    const IMemoryChunkProviderPtr MemoryChunkProvider_;

    const TConsumeSubqueryStatistics ConsumeSubqueryStatistics_;
    const TGetMergeJoinDataSource GetMergeJoinDataSourceCallback_;
    const TExecuteForeign ExecuteForeignCallback_;

    const TLogger Logger;

    friend class TForeignJoinRowsFetcher;
};

DEFINE_REFCOUNTED_TYPE(TJoinSubqueryProfiler)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TForeignJoinRowsFetcher)

class TForeignJoinRowsFetcher
    : public IJoinRowsProducer
{
public:
    TForeignJoinRowsFetcher(TConstJoinClausePtr joinClause, TJoinSubqueryProfilerPtr profiler)
        : JoinClause_(std::move(joinClause))
        , Profiler_(std::move(profiler))
    { }

    ISchemafulUnversionedReaderPtr FetchJoinedRows(
        std::vector<TRow> keys,
        TRowBufferPtr permanentBuffer) override
    {
        if (keys.empty()) {
            return ISchemafulUnversionedReaderPtr{};
        }
        const auto& Logger = Profiler_->Logger;

        auto joinFragment = GetForeignQuery(std::move(keys), std::move(permanentBuffer), *JoinClause_, Logger);

        YT_LOG_DEBUG("Evaluating remote subquery (SubqueryId: %v)", joinFragment.Query->Id);

        auto pipe = New<NTableClient::TSchemafulPipe>(Profiler_->MemoryChunkProvider_);

        Profiler_->ExecuteForeignCallback_(joinFragment, pipe->GetWriter())
            .SubscribeUnique(BIND([this, this_ = MakeStrong(this), pipe] (TErrorOr<TQueryStatistics>&& error) {
                if (!error.IsOK()) {
                    pipe->Fail(error);
                } else {
                    Profiler_->ConsumeSubqueryStatistics_(std::move(error.Value()));
                }
            }));


        return pipe->GetReader();
    }

private:
    static TPlanFragment GetForeignQuery(
        std::vector<TRow> keys,
        TRowBufferPtr buffer,
        const TJoinClause& joinClause,
        const NLogging::TLogger& Logger)
    {
        TDataSource dataSource;
        dataSource.ObjectId = joinClause.ForeignObjectId;
        dataSource.CellId = joinClause.ForeignCellId;

        const auto& foreignEquations = joinClause.ForeignEquations;
        auto foreignKeyPrefix = joinClause.ForeignKeyPrefix;
        auto newQuery = joinClause.GetJoinSubquery();

        auto predicateRefines = false;

        if (joinClause.Predicate) {
            auto keyColumns = joinClause.Schema.GetKeyColumns();

            auto dummyInClause = New<TInExpression>(
                foreignEquations,
                nullptr);

            auto dummyWhereClause = MakeAndExpression(std::move(dummyInClause), joinClause.Predicate);

            auto signature = GetExpressionConstraintSignature(std::move(dummyWhereClause), keyColumns);

            auto score = GetConstraintSignatureScore(signature);

            YT_LOG_DEBUG("Calculated score for join via IN with predicate (Signature: %v, Score: %v)",
                signature,
                score);

            predicateRefines = score > static_cast<int>(2 * foreignKeyPrefix);
        }

        if (foreignKeyPrefix == 0 || predicateRefines) {
            YT_LOG_DEBUG("Using join via IN clause");

            TRowRanges universalRange{{
                buffer->CaptureRow(NTableClient::MinKey().Get()),
                buffer->CaptureRow(NTableClient::MaxKey().Get()),
            }};

            dataSource.Ranges = MakeSharedRange(std::move(universalRange), buffer);

            auto inClause = New<TInExpression>(
                foreignEquations,
                MakeSharedRange(std::move(keys), std::move(buffer)));

            newQuery->WhereClause = newQuery->WhereClause
                ? MakeAndExpression(inClause, newQuery->WhereClause)
                : inClause;

            if (joinClause.Schema.Original->HasComputedColumns() &&
                AllComputedColumnsEvaluated(joinClause))
            {
                newQuery->ForceLightRangeInference = true;
            }

            if (foreignKeyPrefix > 0) {
                // COMPAT(lukyan): Use ordered read without modification of protocol
                newQuery->Limit = OrderedReadWithPrefetchHint;
            }
        } else {
            if (foreignKeyPrefix == foreignEquations.size()) {
                YT_LOG_DEBUG("Using join via source ranges");
                dataSource.Keys = MakeSharedRange(std::move(keys), std::move(buffer));
            } else {
                YT_LOG_DEBUG("Using join via prefix ranges");
                std::vector<TRow> prefixKeys;
                prefixKeys.reserve(keys.size());
                for (auto key : keys) {
                    prefixKeys.push_back(buffer->CaptureRow(
                        TRange(key.Begin(), foreignKeyPrefix),
                        /*captureValues*/ false));
                }
                prefixKeys.erase(std::unique(prefixKeys.begin(), prefixKeys.end()), prefixKeys.end());
                dataSource.Keys = MakeSharedRange(std::move(prefixKeys), std::move(buffer));
            }

            newQuery->InferRanges = false;
            // COMPAT(lukyan): Use ordered read without modification of protocol
            newQuery->Limit = OrderedReadWithPrefetchHint;
        }

        newQuery->GroupClause = joinClause.GroupClause;

        return {
            .Query = std::move(newQuery),
            .DataSource = std::move(dataSource),
            .SubqueryFragment = nullptr,
        };
    }

    static bool AllComputedColumnsEvaluated(const TJoinClause& joinClause)
    {
        auto isColumnInEquations = [&] (TStringBuf column) {
            for (const auto& equation : joinClause.ForeignEquations) {
                if (const auto* reference = equation->As<TReferenceExpression>();
                    reference && column == reference->ColumnName)
                {
                    return true;
                }
            }

            return false;
        };

        auto renamedSchema = joinClause.Schema.GetRenamedSchema();

        for (const auto& column : renamedSchema->Columns()) {
            if (!column.SortOrder()) {
                break;
            }
            if (!column.Expression()) {
                continue;
            }
            if (!isColumnInEquations(column.Name())) {
                return false;
            }
        }

        return true;
    }

    const TConstJoinClausePtr JoinClause_;
    const TJoinSubqueryProfilerPtr Profiler_;
};

DEFINE_REFCOUNTED_TYPE(TForeignJoinRowsFetcher)

IJoinRowsProducerPtr MakeJoinRowsFetcher(TConstJoinClausePtr joinClause, TJoinSubqueryProfilerPtr profiler)
{
    return New<TForeignJoinRowsFetcher>(std::move(joinClause), std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

IJoinSubqueryProfilerPtr CreateJoinProfiler(
    TConstQueryPtr query,
    const TQueryOptions& queryOptions,
    size_t minKeyWidth,
    bool orderedExecution,
    IMemoryChunkProviderPtr memoryChunkProvider,
    TConsumeSubqueryStatistics consumeSubqueryStatistics,
    TGetMergeJoinDataSource getMergeJoinDataSource,
    TExecuteForeign executeForeign,
    TLogger logger)
{
    return New<TJoinSubqueryProfiler>(
        std::move(query),
        queryOptions,
        minKeyWidth,
        orderedExecution,
        std::move(memoryChunkProvider),
        std::move(consumeSubqueryStatistics),
        std::move(getMergeJoinDataSource),
        std::move(executeForeign),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

