#include "stdafx.h"
#include "coordinator.h"

#include "private.h"
#include "helpers.h"
#include "column_evaluator.h"
#include "plan_helpers.h"
#include "plan_fragment.h"

#ifdef YT_USE_LLVM
#include "folding_profiler.h"
#endif

#include <core/concurrency/scheduler.h>

#include <core/profiling/scoped_timer.h>

#include <core/misc/protobuf_helpers.h>
#include <core/misc/common.h>

#include <core/tracing/trace_context.h>

#include <core/logging/log.h>

#include <ytlib/chunk_client/chunk_replica.h>

#include <ytlib/new_table_client/schemaful_reader.h>
#include <ytlib/new_table_client/writer.h>
#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/unordered_schemaful_reader.h>
#include <ytlib/new_table_client/ordered_schemaful_reader.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/tablet_client/public.h>

#include <cstdlib>

namespace NYT {
namespace NQueryClient {

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

// TODO: replace with lambdas
class TRangeInferrer
{
private:
    class IRangeInferrerImpl;
    class TRangeInferrerLight;
    class TRangeInferrerHeavy;

public:

    // Wrapper for Light and Heavy range inferrers.
    // Use Heavy if we need to enrich ranges with computed columns, otherwise Light.
    TRangeInferrer(
        const TConstExpressionPtr& predicate,
        const TTableSchema& schema,
        const TKeyColumns& keyColumns,
        const TColumnEvaluatorCachePtr& evaluatorCache,
        const IFunctionRegistryPtr functionRegistry,
        i64 rangeExpansionLimit,
        bool verboseLogging)
    {
        if (/*splits.size() == 0 ||*/ !predicate) {
            Impl_ = std::make_unique<TRangeInferrerLight>(predicate, TKeyColumns(), functionRegistry, verboseLogging);
            return;
        }

#ifdef YT_USE_LLVM
        if (!schema.HasComputedColumns()) {
            Impl_ = std::make_unique<TRangeInferrerLight>(predicate, keyColumns, functionRegistry, verboseLogging);
            return;
        }

        Impl_ = std::make_unique<TRangeInferrerHeavy>(predicate, schema, keyColumns, evaluatorCache, functionRegistry, rangeExpansionLimit, verboseLogging);
#else
        Impl_ = std::make_unique<TRangeInferrerLight>(predicate, keyColumns, functionRegistry, verboseLogging);
#endif
    }

    std::vector<TKeyRange> GetRangesWithinRange(const TKeyRange& keyRange)
    {
        return Impl_->GetRangesWithinRange(keyRange);
    }

private:
    std::unique_ptr<IRangeInferrerImpl> Impl_;

    // Range inferrer interface.
    class IRangeInferrerImpl
    {
    public:
        virtual ~IRangeInferrerImpl() = default;
        virtual std::vector<TKeyRange> GetRangesWithinRange(const TKeyRange& keyRange) = 0;
    };

    // Extract ranges from a predicate.
    class TRangeInferrerLight
        : public IRangeInferrerImpl
    {
    public:
        TRangeInferrerLight(
            const TConstExpressionPtr& predicate,
            const TKeyColumns& keyColumns,
            const IFunctionRegistryPtr functionRegistry,
            bool verboseLogging)
        {
            KeyTrie_ = ExtractMultipleConstraints(predicate, keyColumns, &KeyTrieBuffer_, functionRegistry);

            LOG_DEBUG_IF(verboseLogging, "Predicate %Qv defines key constraints %Qv", InferName(predicate), KeyTrie_);
        }

        virtual std::vector<TKeyRange> GetRangesWithinRange(const TKeyRange& keyRange) override
        {
            TRowBuffer rowBuffer;
            auto unversionedRanges = GetRangesFromTrieWithinRange(keyRange, KeyTrie_, &rowBuffer);
            std::vector<TKeyRange> ranges;
            for (auto range : unversionedRanges) {
                ranges.emplace_back(TKey(range.first), TKey(range.second));
            }
            return ranges;
        }

    private:
        TKeyTrieNode KeyTrie_ = TKeyTrieNode::Universal();
        TRowBuffer KeyTrieBuffer_;
    };

    // Extract ranges from a predicate and enrich them with computed column values.
    class TRangeInferrerHeavy
        : public IRangeInferrerImpl
    {
    public:
        TRangeInferrerHeavy(
            const TConstExpressionPtr& predicate,
            const TTableSchema& schema,
            const TKeyColumns& keyColumns,
            const TColumnEvaluatorCachePtr& evaluatorCache,
            const IFunctionRegistryPtr functionRegistry,
            i64 rangeExpansionLimit,
            bool verboseLogging)
            : Schema_(schema)
            , KeySize_(keyColumns.size())
            , RangeExpansionLimit_(rangeExpansionLimit)
            , VerboseLogging_(verboseLogging)
        {
            Evaluator_ = evaluatorCache->Find(Schema_, KeySize_);
            yhash_set<Stroka> references;
            Profile(predicate, schema, nullptr, nullptr, &references, functionRegistry);
            auto depletedKeyColumns = BuildDepletedIdMapping(references);
            KeyTrie_ = ExtractMultipleConstraints(predicate, depletedKeyColumns, &KeyTrieBuffer_, functionRegistry);
            LOG_DEBUG_IF(verboseLogging, "Predicate %Qv defines key constraints %Qv", InferName(predicate), KeyTrie_);
        }

        virtual std::vector<TKeyRange> GetRangesWithinRange(const TKeyRange& keyRange) override
        {
            auto ranges = GetRangesFromTrieWithinRange(keyRange, KeyTrie_, &Buffer_);
            std::vector<std::pair<TRow, TRow>> enrichedRanges;
            bool rebuildRanges = false;
            RangeExpansionLeft_ = (RangeExpansionLimit_ > ranges.size())
                ? RangeExpansionLimit_ - ranges.size()
                : 0;

            for (auto& range : ranges) {
                rebuildRanges |= EnrichKeyRange(range, enrichedRanges);
            }

            if (rebuildRanges) {
                std::sort(enrichedRanges.begin(), enrichedRanges.end());
                int last = 0;
                for (int index = 1; index < enrichedRanges.size(); ++index) {
                    if (enrichedRanges[index].first < enrichedRanges[last].second) {
                        enrichedRanges[last].second = enrichedRanges[index].second;
                    } else {
                        ++last;
                        if (last < index) {
                            enrichedRanges[last] = enrichedRanges[index];
                        }
                    }
                }
                enrichedRanges.resize(last + 1);
            }

            std::vector<TKeyRange> owningRanges;
            for (auto range : enrichedRanges) {
                owningRanges.emplace_back(TKey(range.first), TKey(range.second));
            }

            Buffer_.Clear();
            return owningRanges;
        }

    private:
        class TModRangeGenerator
        {
        public:
            TModRangeGenerator(TUnversionedValue mod)
                : Value_(mod)
            {
                Mod_ = (Value_.Type == EValueType::Uint64)
                    ? mod.Data.Uint64
                    : std::abs(mod.Data.Int64);
                Reset();
            }

            ui64 Count()
            {
                return (Value_.Type == EValueType::Uint64)
                    ? Mod_
                    : (Mod_ - 1) * 2 + 1;
            }

            bool Finished() const
            {
                return Value_.Data.Uint64 == Mod_;
            }

            TUnversionedValue Next()
            {
                YCHECK(!Finished());
                auto result = Value_;
                ++Value_.Data.Uint64;
                return result;
            }

            void Reset()
            {
                Value_.Data.Uint64 = (Value_.Type == EValueType::Uint64)
                    ? 0
                    : (-Mod_ + 1);
            }
        private:
            TUnversionedValue Value_;
            ui64 Mod_;
        };

        TKeyColumns BuildDepletedIdMapping(const yhash_set<Stroka>& references)
        {
            TKeyColumns depletedKeyColumns;
            SchemaToDepletedMapping_.resize(KeySize_ + 1, -1);

            auto addIndexToMapping = [&] (int index) {
                SchemaToDepletedMapping_[index] = DepletedToSchemaMapping_.size();
                DepletedToSchemaMapping_.push_back(index);
            };

            for (int index = 0; index < KeySize_; ++index) {
                auto column = Schema_.Columns()[index];
                if (!column.Expression || references.find(column.Name) != references.end()) {
                    addIndexToMapping(index);
                    depletedKeyColumns.push_back(column.Name);
                } else {
                    ComputedColumnIndexes_.push_back(index);
                }
            }
            addIndexToMapping(KeySize_);

            return depletedKeyColumns;
        }

        bool IsUserColumn(int index, const std::pair<TRow, TRow>& range)
        {
            return SchemaToDepletedMapping_[index] != -1;
        }

        TNullable<int> IsExactColumn(int index, const std::pair<TRow, TRow>& range, int depletedPrefixSize)
        {
            const auto& references = Evaluator_->GetReferences(index);
            int maxReferenceIndex = 0;

            for (const auto& reference : references) {
                int referenceIndex = Schema_.GetColumnIndexOrThrow(reference);
                int depletedIndex = SchemaToDepletedMapping_[referenceIndex];
                maxReferenceIndex = std::max(maxReferenceIndex, referenceIndex);

                if (depletedIndex >= depletedPrefixSize
                    || depletedIndex == -1
                    || IsSentinelType(range.first[depletedIndex].Type)
                    || IsSentinelType(range.second[depletedIndex].Type)
                    || range.first[depletedIndex] != range.second[depletedIndex])
                {
                    return TNullable<int>();
                }
            }
            return maxReferenceIndex;
        }

        TNullable<TModRangeGenerator> IsModColumn(int index)
        {
            auto expr = Evaluator_->GetExpression(index)->As<TBinaryOpExpression>();
            if (expr && expr->Opcode == EBinaryOp::Modulo) {
                if (auto literalExpr = expr->Rhs->As<TLiteralExpression>()) {
                    TUnversionedValue value = literalExpr->Value;
                    if (value.Type == EValueType::Int64 || value.Type == EValueType::Uint64) {
                        value.Id = index;
                        return TModRangeGenerator(value);
                    }
                }
            }
            return TNullable<TModRangeGenerator>();
        }

        int ExpandKey(TRow destination, TRow source, int size)
        {
            for (int index = 0; index < size; ++index) {
                int depletedIndex = SchemaToDepletedMapping_[index];
                if (depletedIndex != -1) {
                    if (depletedIndex < source.GetCount()) {
                        destination[index] = source[depletedIndex];
                    } else {
                        return index;
                    }
                }
            }
            return size;
        }

        TNullable<TUnversionedValue> TrimSentinel(TRow row)
        {
            TNullable<TUnversionedValue> result;
            for (int index = row.GetCount() - 1; index >= 0 && IsSentinelType(row[index].Type); --index) {
                result = row[index];
                row.SetCount(index);
            }
            return result;
        }

        void AppendSentinel(TRow row, TNullable<TUnversionedValue> sentinel)
        {
            if (sentinel) {
                row[row.GetCount()] = sentinel.Get();
                row.SetCount(row.GetCount() + 1);
            }
        }

        TRow Copy(TRow source)
        {
            auto row = TUnversionedRow::Allocate(Buffer_.GetAlignedPool(), source.GetCount());
            for (int index = 0; index < source.GetCount(); ++index) {
                row[index] = source[index];
            }
            return row;
        }

        bool EnrichKeyRange(std::pair<TRow, TRow>& range, std::vector<std::pair<TRow, TRow>>& ranges)
        {
            auto lowerSentinel = TrimSentinel(range.first);
            auto upperSentinel = TrimSentinel(range.second);

            int depletedPrefixSize = 0;
            while (depletedPrefixSize < range.first.GetCount()
                && depletedPrefixSize < range.second.GetCount()
                && range.first[depletedPrefixSize] == range.second[depletedPrefixSize])
            {
                ++depletedPrefixSize;
            }

            int shrinkSize = KeySize_;
            int maxReferenceIndex = 0;
            int rangeCount = 1;
            std::vector<TModRangeGenerator> modGenerators;
            std::vector<int> exactGenerators;

            for (int index = 0; index < KeySize_; ++index) {
                if (IsUserColumn(index, range)) {
                    continue;
                } else if (auto lastReference = IsExactColumn(index, range, depletedPrefixSize)) {
                    maxReferenceIndex = std::max(maxReferenceIndex, lastReference.Get());
                    exactGenerators.push_back(index);
                    continue;
                } else if (auto generator = IsModColumn(index)) {
                    auto count = generator.Get().Count();
                    if (count < RangeExpansionLeft_ && rangeCount * count < RangeExpansionLeft_) {
                        rangeCount *= count;
                        modGenerators.push_back(generator.Get());
                        continue;
                    }
                }
                shrinkSize = index;
                break;
            }

            RangeExpansionLeft_ -= rangeCount;

            auto lowerRow = TUnversionedRow::Allocate(Buffer_.GetAlignedPool(), KeySize_ + 1);
            auto upperRow = TUnversionedRow::Allocate(Buffer_.GetAlignedPool(), KeySize_ + 1);

            int lowerSize = ExpandKey(lowerRow, range.first, std::max(shrinkSize, maxReferenceIndex + 1));
            int upperSize = ExpandKey(upperRow, range.second, std::max(shrinkSize, maxReferenceIndex + 1));

            for (int index : exactGenerators) {
                Evaluator_->EvaluateKey(lowerRow, Buffer_, index);
                upperRow[index] = lowerRow[index];
            }

            bool shrinked;
            if (shrinkSize < DepletedToSchemaMapping_[range.second.GetCount()]) {
                upperRow[shrinkSize].Type = EValueType::Max;
                upperRow.SetCount(shrinkSize + 1);
                lowerRow.SetCount(shrinkSize);
                shrinked = true;
            } else {
                upperRow.SetCount(upperSize);
                lowerRow.SetCount(lowerSize);
                AppendSentinel(upperRow, upperSentinel);
                AppendSentinel(lowerRow, lowerSentinel);
                shrinked = false;
            }

            if (modGenerators.empty()) {
                ranges.push_back(std::make_pair(lowerRow, upperRow));
            } else {
                auto yield = [&] () {
                    ranges.push_back(std::make_pair(Copy(lowerRow), Copy(upperRow)));
                };
                auto setValue = [&] (TUnversionedValue value) {
                    lowerRow[value.Id] = value;
                    upperRow[value.Id] = value;
                };

                for (auto& generator : modGenerators) {
                    setValue(generator.Next());
                }
                yield();

                int generatorIndex = modGenerators.size() - 1;
                while (generatorIndex >= 0) {
                    if (modGenerators[generatorIndex].Finished()) {
                        --generatorIndex;
                    } else {
                        setValue(modGenerators[generatorIndex].Next());
                        while (generatorIndex + 1 < modGenerators.size()) {
                            ++generatorIndex;
                            modGenerators[generatorIndex].Reset();
                            setValue(modGenerators[generatorIndex].Next());
                        }
                        yield();
                    }
                }
            }
            return shrinked;
        }

        TColumnEvaluatorPtr Evaluator_;
        TKeyTrieNode KeyTrie_ = TKeyTrieNode::Universal();
        TRowBuffer KeyTrieBuffer_;
        TRowBuffer Buffer_;
        std::vector<int> DepletedToSchemaMapping_;
        std::vector<int> ComputedColumnIndexes_;
        std::vector<int> SchemaToDepletedMapping_;
        TTableSchema Schema_;
        int KeySize_;
        i64 RangeExpansionLimit_;
        i64 RangeExpansionLeft_;
        bool VerboseLogging_;
    };
};

std::pair<TConstQueryPtr, std::vector<TConstQueryPtr>> CoordinateQuery(
    const TConstQueryPtr& query,
    const std::vector<TRefiner>& refiners)
{
    auto Logger = BuildLogger(query);

    std::vector<TConstQueryPtr> subqueries;

    auto subqueryInputRowLimit = refiners.empty()
        ? 0
        : 2 * std::min(query->InputRowLimit, std::numeric_limits<i64>::max() / 2) / refiners.size();

    auto subqueryOutputRowLimit = query->OutputRowLimit;

    for (const auto& refiner : refiners) {
        // Set initial schema and key columns
        auto subquery = New<TQuery>(
            subqueryInputRowLimit,
            subqueryOutputRowLimit);

        subquery->TableSchema = query->TableSchema;
        subquery->KeyColumns = query->KeyColumns;
        subquery->JoinClause = query->JoinClause;

        if (query->WhereClause) {
            subquery->WhereClause = refiner(query->WhereClause, subquery->TableSchema, subquery->KeyColumns);
        }

        if (query->GroupClause) {
            subquery->GroupClause = query->GroupClause;
        } else {
            if (query->OrderClause) {
                subquery->OrderClause = query->OrderClause;            
            } else {            
                subquery->ProjectClause = query->ProjectClause;
            }
            subquery->Limit = query->Limit;
        }

        subqueries.push_back(subquery);
    }

    auto topQuery = New<TQuery>(
        query->InputRowLimit,
        query->OutputRowLimit);

    topQuery->OrderClause = query->OrderClause;
    topQuery->Limit = query->Limit;
    
    if (query->GroupClause) {
        topQuery->TableSchema = query->GroupClause->GetTableSchema();
        if (subqueries.size() > 1) {
            auto groupClause = New<TGroupClause>();
            groupClause->GroupedTableSchema = query->GroupClause->GroupedTableSchema;

            auto& finalGroupItems = groupClause->GroupItems;
            for (const auto& groupItem : query->GroupClause->GroupItems) {
                auto referenceExpr = New<TReferenceExpression>(
                    NullSourceLocation,
                    groupItem.Expression->Type,
                    groupItem.Name);
                finalGroupItems.emplace_back(std::move(referenceExpr), groupItem.Name);
            }

            auto& finalAggregateItems = groupClause->AggregateItems;
            for (const auto& aggregateItem : query->GroupClause->AggregateItems) {
                auto referenceExpr = New<TReferenceExpression>(
                    NullSourceLocation,
                    aggregateItem.Expression->Type,
                    aggregateItem.Name);
                finalAggregateItems.emplace_back(
                    std::move(referenceExpr),
                    aggregateItem.AggregateFunction,
                    aggregateItem.Name);
            }

            topQuery->GroupClause = groupClause;
        }

        topQuery->ProjectClause = query->ProjectClause;
    } else {
        topQuery->TableSchema = query->GetTableSchema();

        if (query->OrderClause) {
            topQuery->ProjectClause = query->ProjectClause;
        }
    }

    return std::make_pair(topQuery, subqueries);
}

TDataSources GetPrunedSources(
    const TConstExpressionPtr& predicate,
    const TTableSchema& tableSchema,
    const TKeyColumns& keyColumns,
    const TDataSources& sources,
    const TColumnEvaluatorCachePtr& evaluatorCache,
    const IFunctionRegistryPtr functionRegistry,
    i64 rangeExpansionLimit,
    bool verboseLogging)
{
    LOG_DEBUG("Infering ranges from predicate");

    TRangeInferrer rangeInferrer(predicate, tableSchema, keyColumns, evaluatorCache, functionRegistry, rangeExpansionLimit, verboseLogging);

    auto keyRangeFormatter = [] (const TKeyRange& range) -> Stroka {
        return Format("[%v .. %v]",
            range.first,
            range.second);
    };

    LOG_DEBUG("Splitting %v sources according to ranges", sources.size());

    TDataSources prunedSources;
    for (const auto& source : sources) {
        const auto& originalRange = source.Range;
        auto ranges = rangeInferrer.GetRangesWithinRange(originalRange);

        for (const auto& range : ranges) {
            auto sourceCopy = source;

            LOG_DEBUG_IF(verboseLogging, "Narrowing source %v key range from %v to %v",
                sourceCopy.Id,
                keyRangeFormatter(originalRange),
                keyRangeFormatter(range));

            sourceCopy.Range = range;

            prunedSources.push_back(std::move(sourceCopy));
        }
    }

    return prunedSources;
}

TDataSources GetPrunedSources(
    const TConstQueryPtr& query,
    const TDataSources& sources,
    const TColumnEvaluatorCachePtr& evaluatorCache,
    const IFunctionRegistryPtr functionRegistry,
    i64 rangeExpansionLimit,
    bool verboseLogging)
{
    return GetPrunedSources(
        query->WhereClause,
        query->TableSchema,
        query->KeyColumns,
        sources,
        evaluatorCache,
        functionRegistry,
        rangeExpansionLimit,
        verboseLogging);
}

TKeyRange GetRange(const TDataSources& sources)
{
    if (sources.empty()) {
        return TKeyRange();
    }

    auto keyRange = sources[0].Range;
    for (int index = 1; index < sources.size(); ++index) {
        keyRange = Unite(keyRange, sources[index].Range);
    }
    return keyRange;
}

std::vector<TKeyRange> GetRanges(const std::vector<TDataSources>& groupedSplits)
{
    std::vector<TKeyRange> ranges(groupedSplits.size());
    for (int index = 0; index < groupedSplits.size(); ++index) {
        ranges[index] = GetRange(groupedSplits[index]);
    }
    return ranges;
}

TQueryStatistics CoordinateAndExecute(
    const TPlanFragmentPtr& fragment,
    ISchemafulWriterPtr writer,
    const std::vector<TRefiner>& refiners,
    bool isOrdered,
    std::function<TEvaluateResult(const TConstQueryPtr&, int)> evaluateSubquery,
    std::function<TQueryStatistics(const TConstQueryPtr&, ISchemafulReaderPtr, ISchemafulWriterPtr)> evaluateTop)
{
    auto nodeDirectory = fragment->NodeDirectory;
    auto query = fragment->Query;
    auto Logger = BuildLogger(query);

    LOG_DEBUG("Begin coordinating query");

    TConstQueryPtr topQuery;
    std::vector<TConstQueryPtr> subqueries;
    std::tie(topQuery, subqueries) = CoordinateQuery(query, refiners);

    LOG_DEBUG("Finished coordinating query");

    std::vector<ISchemafulReaderPtr> splitReaders;

    ISchemafulReaderPtr topReader;
    // Use TFutureHolder to prevent leaking subqueries.
    std::vector<TFutureHolder<TQueryStatistics>> subqueryHolders;

    if (isOrdered) {
        int index = 0;

        topReader = CreateOrderedSchemafulReader([&, index] () mutable -> ISchemafulReaderPtr {
            if (index >= subqueries.size()) {
                return nullptr;
            }

            const auto& subquery = subqueries[index];

            ISchemafulReaderPtr reader;
            TFuture <TQueryStatistics> statistics;
            std::tie(reader, statistics) = evaluateSubquery(subquery, index);

            subqueryHolders.push_back(MakeHolder(statistics, false));

            ++index;

            return reader;
        });
    } else {
        for (int index = 0; index < subqueries.size(); ++index) {
            auto subquery = subqueries[index];

            ISchemafulReaderPtr reader;
            TFuture<TQueryStatistics> statistics;
            std::tie(reader, statistics) = evaluateSubquery(subquery, index);

            splitReaders.push_back(reader);
            subqueryHolders.push_back(statistics);
        }

        topReader = CreateUnorderedSchemafulReader(splitReaders);
    }

    auto queryStatistics = evaluateTop(topQuery, std::move(topReader), std::move(writer));

    for (auto const& holder : subqueryHolders) {
        auto subfragmentStatistics = WaitFor(holder.Get()).ValueOrThrow();
        LOG_DEBUG("Subfragment statistics (%v)", subfragmentStatistics);
        queryStatistics += subfragmentStatistics;
    }

    return queryStatistics;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

