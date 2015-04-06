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

        yhash_set<Stroka> references;
        Profile(predicate, schema, nullptr, nullptr, &references, functionRegistry);

        for (const auto& reference : references) {
            if (schema.GetColumnOrThrow(reference).Expression) {
                Impl_ = std::make_unique<TRangeInferrerLight>(predicate, keyColumns, functionRegistry, verboseLogging);
                return;
            }
        }

        Impl_ = std::make_unique<TRangeInferrerHeavy>(predicate, schema, keyColumns, evaluatorCache, functionRegistry, verboseLogging);
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
            return GetRangesFromTrieWithinRange(keyRange, KeyTrie_);
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
            bool verboseLogging)
            : Schema_(schema)
            , KeySize_(keyColumns.size())
            , VerboseLogging_(verboseLogging)
        {
            SchemaToDepletedMapping_.resize(KeySize_, -1);
            Evaluator_ = evaluatorCache->Find(Schema_, KeySize_);
            TKeyColumns depletedKeyColumns;

            for (int index = 0; index < KeySize_; ++index) {
                if (!Schema_.Columns()[index].Expression) {
                    SchemaToDepletedMapping_[index] = DepletedToSchemaMapping_.size();
                    DepletedToSchemaMapping_.push_back(index);
                    depletedKeyColumns.push_back(keyColumns[index]);
                } else {
                    ComputedColumnIndexes_.push_back(index);
                }
            }

            KeyTrie_ = ExtractMultipleConstraints(predicate, depletedKeyColumns, &KeyTrieBuffer_, functionRegistry);

            LOG_DEBUG_IF(verboseLogging, "Predicate %Qv defines key constraints %Qv", InferName(predicate), KeyTrie_);
        }

        virtual std::vector<TKeyRange> GetRangesWithinRange(const TKeyRange& keyRange) override
        {
            auto ranges = GetRangesFromTrieWithinRange(keyRange, KeyTrie_);

            bool rebuildRanges = false;

            for (auto& range : ranges) {
                rebuildRanges |= EnrichKeyRange(range);
            }

            if (rebuildRanges) {
                TKeyTrieNode trie = TKeyTrieNode::Empty();

                for (const auto& range : ranges) {
                    trie.Unite(TKeyTrieNode::FromRange(range));
                }

                LOG_DEBUG_IF(VerboseLogging_, "Inferred key constraints %Qv", trie);

                ranges = GetRangesFromTrieWithinRange(keyRange, trie);
            }

            Buffer_.Clear();
            return ranges;
        }

    private:
        int GetEssentialKeySize(const TKey& key)
        {
            int size = key.GetCount();

            while (size > 0 && IsSentinelType(key[size - 1].Type)) {
                --size;
            }

            return size;
        }

        void FixIds(TKey& key)
        {
            for (int index = 0; index < key.GetCount(); ++index) {
                key.Get()[index].Id = index;
            }
        }

        void EnrichBound(TKey& bound, int boundSize, bool shrinked, EValueType boundType)
        {
            YCHECK(boundSize <= KeySize_);
            bool needEvaluation = ComputedColumnIndexes_.size() > 0 && ComputedColumnIndexes_[0] < boundSize;

            if (!shrinked && !needEvaluation) {
                return;
            }

            TUnversionedOwningRowBuilder builder(boundSize + 1);

            for (int index = 0; index < boundSize; ++index) {
                int depletedIndex = SchemaToDepletedMapping_[index];
                builder.AddValue(depletedIndex == -1
                    ? MakeUnversionedSentinelValue(EValueType::Null, index)
                    : bound[SchemaToDepletedMapping_[index]]);
            }

            if (!needEvaluation) {
                if (boundType == EValueType::Max) {
                    builder.AddValue(MakeUnversionedSentinelValue(boundType, boundSize));
                }

                bound = builder.FinishRow();
                FixIds(bound);
                return;
            }

            for (int index = boundSize; index < KeySize_; ++index) {
                int depletedIndex = SchemaToDepletedMapping_[index];
                builder.AddValue(depletedIndex == -1 || depletedIndex >= bound.GetCount()
                    ? MakeUnversionedSentinelValue(EValueType::Null, index)
                    : bound[depletedIndex]);
            }

            auto enrichedKey = builder.FinishRow();

            for (int index : ComputedColumnIndexes_) {
                if (index >= boundSize) {
                    break;
                }

                Evaluator_->EvaluateKey(enrichedKey.Get(), Buffer_, index);
            }

            // NB: Copy all data from Buffer_ into the owning row.
            for (int index = 0; index < boundSize; ++index) {
                builder.AddValue(enrichedKey[index]);
            }

            if (shrinked) {
                if (boundType == EValueType::Max) {
                    builder.AddValue(MakeUnversionedSentinelValue(boundType, boundSize));
                }
            } else {
                int size = GetEssentialKeySize(bound);
                if (size < bound.GetCount()) {
                    builder.AddValue(bound[size]);
                }
            }

            bound = builder.FinishRow();
            FixIds(bound);
        }

        bool EnrichKeyRange(TKeyRange& range)
        {
            auto getEssentialKeySize = [&] (const TKey& key) -> int {
                int size = GetEssentialKeySize(key);
                return size ? DepletedToSchemaMapping_[size - 1] + 1 : 0;
            };

            const int leftSize = getEssentialKeySize(range.first);
            const int rightSize = getEssentialKeySize(range.second);
            int lcpSize = std::min(leftSize, rightSize);

            auto getPrefixSize = [&] () -> int {
                auto validComputedKey = [&] (int computedKey) -> bool {
                    const auto& references = Evaluator_->GetReferences(computedKey);

                    for (const auto& reference : references) {
                        int index = Schema_.GetColumnIndexOrThrow(reference);
                        int depletedIndex = SchemaToDepletedMapping_[index];

                        if (index >= lcpSize
                            || depletedIndex == -1
                            || IsSentinelType(range.first[depletedIndex].Type)
                            || IsSentinelType(range.second[depletedIndex].Type)
                            || range.first[depletedIndex] != range.second[depletedIndex])
                        {
                            return false;
                        }
                    }

                    return true;
                };

                for (int index = 0; index < KeySize_; ++index) {
                    if (Schema_.Columns()[index].Expression) {
                        if (!validComputedKey(index)) {
                            return index;
                        }
                    } else if (index >= lcpSize) {
                        return index;
                    }
                }

                return KeySize_;
            };

            int prefixSize = getPrefixSize();

            auto getEvaluatableKeySize = [&] (int thisKeySize) -> int {
                for (int index = prefixSize; index < thisKeySize; ++index) {
                    if (Schema_.Columns()[index].Expression) {
                        return index;
                    }
                }

                return std::max(thisKeySize, prefixSize);
            };

            int evaluatableLeftSize = getEvaluatableKeySize(leftSize);
            int evaluatableRightSize = getEvaluatableKeySize(rightSize);

            bool leftShrinked = evaluatableLeftSize < leftSize;
            bool rightShrinked = evaluatableRightSize < rightSize;

            EnrichBound(range.first, evaluatableLeftSize, leftShrinked, EValueType::Min);
            EnrichBound(range.second, evaluatableRightSize, rightShrinked, EValueType::Max);

            return leftShrinked || rightShrinked;
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
        subquery->Limit = query->Limit;
        subquery->JoinClause = query->JoinClause;

        if (query->WhereClause) {
            subquery->WhereClause = refiner(query->WhereClause, subquery->TableSchema, subquery->KeyColumns);
        }

        if (query->GroupClause) {
            subquery->GroupClause = query->GroupClause;
        } else {
            subquery->ProjectClause = query->ProjectClause;
        }

        subqueries.push_back(subquery);
    }

    auto topQuery = New<TQuery>(
        query->InputRowLimit,
        query->OutputRowLimit);

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
    bool verboseLogging)
{
    LOG_DEBUG("Infering ranges from predicate");

    TRangeInferrer rangeInferrer(predicate, tableSchema, keyColumns, evaluatorCache, functionRegistry, verboseLogging);

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
    bool verboseLogging)
{
    return GetPrunedSources(
        query->WhereClause,
        query->TableSchema,
        query->KeyColumns,
        sources,
        evaluatorCache,
        functionRegistry,
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

