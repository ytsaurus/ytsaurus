#include "join_profiler.h"

#include <yt/yt/library/query/base/coordination_helpers.h>
#include <yt/yt/library/query/base/helpers.h>
#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_helpers.h>

#include <yt/yt/library/query/misc/rowset_subrange_reader.h>
#include <yt/yt/library/query/misc/rowset_writer.h>

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

DECLARE_REFCOUNTED_CLASS(TEmptyReader)

class TEmptyReader
    : public ISchemafulUnversionedReader
{
    IUnversionedRowBatchPtr Read(const TRowBatchReadOptions&) override
    {
        return nullptr;
    }

    TFuture<void> GetReadyEvent() const override
    {
        return VoidFuture;
    }

    NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        return {};
    }

    NChunkClient::TCodecStatistics GetDecompressionStatistics() const override
    {
        return {};
    }

    bool IsFetchingCompleted() const override
    {
        return false;
    }

    std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override
    {
        return {};
    }
};

DEFINE_REFCOUNTED_TYPE(TEmptyReader)

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
        if (keys.empty()) {
            return New<TEmptyReader>();
        } else {
            return CreateRowsetSubrangeReader(
                AsyncRows_,
                {
                    TKeyBoundRef(keys.front().FirstNElements(ForeignKeyPrefix_), /*inclusive*/ true, /*upper*/ false),
                    TKeyBoundRef(keys.back().FirstNElements(ForeignKeyPrefix_), /*inclusive*/ true, /*upper*/ true),
                });
        }
    }

private:
    const TFuture<TSharedRange<TRow>> AsyncRows_;
    const size_t ForeignKeyPrefix_;
};

DEFINE_REFCOUNTED_TYPE(TForeignJoinRowsPrefetcher)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TJoinSubqueryProfiler)

class TJoinSubqueryProfiler
    : public virtual IJoinProfiler
    , public virtual IJoinRowsProducer
{
public:
    TJoinSubqueryProfiler(
        TConstJoinClausePtr joinClause,
        TExecutePlan executeForeign,
        TConsumeSubqueryStatistics consumeSubqueryStatistics,
        TGetPrefetchJoinDataSource getPrefetchJoinDataSource,
        IMemoryChunkProviderPtr memoryChunkProvider,
        TLogger logger)
        : JoinClause_(std::move(joinClause))
        , ExecutePlan_(std::move(executeForeign))
        , ConsumeSubqueryStatistics_(std::move(consumeSubqueryStatistics))
        , GetPrefetchJoinDataSource_(std::move(getPrefetchJoinDataSource))
        , MemoryChunkProvider_(std::move(memoryChunkProvider))
        , Logger(std::move(logger))
    { }

    IJoinRowsProducerPtr Profile() override
    {
        if (auto dataSource = GetPrefetchJoinDataSource_()) {
            dataSource->ObjectId = JoinClause_->ForeignObjectId;
            dataSource->CellId = JoinClause_->ForeignCellId;

            auto joinSubquery = JoinClause_->GetJoinSubquery();
            joinSubquery->InferRanges = false;
            // COMPAT(lukyan): Use ordered read without modification of protocol
            joinSubquery->Limit = OrderedReadWithPrefetchHint;

            YT_LOG_DEBUG("Evaluating remote subquery (SubqueryId: %v)", joinSubquery->Id);

            auto writer = New<TSimpleRowsetWriter>(MemoryChunkProvider_);

            ExecutePlan_(
                TPlanFragment{
                    .Query = std::move(joinSubquery),
                    .DataSource = std::move(*dataSource),
                },
                writer)
                .SubscribeUnique(BIND([this, this_ = MakeStrong(this), writer] (TErrorOr<TQueryStatistics>&& error) {
                    if (!error.IsOK()) {
                        writer->Fail(error);
                    } else {
                        ConsumeSubqueryStatistics_(std::move(error.Value()));
                    }
                }));

            return New<TForeignJoinRowsPrefetcher>(writer->GetResult(), JoinClause_->ForeignKeyPrefix);
        } else {
            return this;
        }
    }

    ISchemafulUnversionedReaderPtr FetchJoinedRows(
        std::vector<TRow> keys,
        TRowBufferPtr permanentBuffer) override
    {
        if (keys.empty()) {
            return ISchemafulUnversionedReaderPtr{};
        }

        auto joinFragment = GetForeignQuery(std::move(keys), std::move(permanentBuffer), *JoinClause_, Logger);

        YT_LOG_DEBUG("Evaluating remote subquery (SubqueryId: %v)", joinFragment.Query->Id);

        auto pipe = New<NTableClient::TSchemafulPipe>(MemoryChunkProvider_);

        ExecutePlan_(joinFragment, pipe->GetWriter())
            .SubscribeUnique(BIND([this, this_ = MakeStrong(this), pipe] (TErrorOr<TQueryStatistics>&& error) {
                if (!error.IsOK()) {
                    pipe->Fail(error);
                } else {
                    ConsumeSubqueryStatistics_(std::move(error.Value()));
                }
            }));


        return pipe->GetReader();
    }

private:
    const TConstJoinClausePtr JoinClause_;
    const TExecutePlan ExecutePlan_;
    const TConsumeSubqueryStatistics ConsumeSubqueryStatistics_;
    const TGetPrefetchJoinDataSource GetPrefetchJoinDataSource_;
    const IMemoryChunkProviderPtr MemoryChunkProvider_;

    const TLogger Logger;

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
};

DEFINE_REFCOUNTED_TYPE(TJoinSubqueryProfiler)

////////////////////////////////////////////////////////////////////////////////

IJoinProfilerPtr CreateJoinSubqueryProfiler(
    TConstJoinClausePtr joinClause,
    TExecutePlan executeForeign,
    TConsumeSubqueryStatistics consumeSubqueryStatistics,
    TGetPrefetchJoinDataSource getPrefetchJoinDataSource,
    IMemoryChunkProviderPtr memoryChunkProvider,
    TLogger logger)
{
    return New<TJoinSubqueryProfiler>(
        std::move(joinClause),
        std::move(executeForeign),
        std::move(consumeSubqueryStatistics),
        std::move(getPrefetchJoinDataSource),
        std::move(memoryChunkProvider),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
