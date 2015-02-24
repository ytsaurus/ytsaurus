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
       const TDataSplits& splits,
       const TColumnEvaluatorCachePtr& evaluatorCache)
    {
        if (splits.size() == 0 || !predicate) {
            Impl_ = std::make_unique<TRangeInferrerLight>(predicate, TKeyColumns());
            return;
        }

        auto schema = GetTableSchemaFromDataSplit(splits[0]);
        auto keyColumns = GetKeyColumnsFromDataSplit(splits[0]);

#ifdef YT_USE_LLVM
        if (!schema.HasComputedColumns()) {
            Impl_ = std::make_unique<TRangeInferrerLight>(predicate, keyColumns);
            return;
        }

        yhash_set<Stroka> references;
        Profile(predicate, schema, nullptr, nullptr, &references);

        for (const auto& reference : references) {
            if (schema.GetColumnOrThrow(reference).Expression) {
                Impl_ = std::make_unique<TRangeInferrerLight>(predicate, keyColumns);
                return;
            }
        }

        Impl_ = std::make_unique<TRangeInferrerHeavy>(predicate, schema, keyColumns, evaluatorCache);
#else
        Impl_ = std::make_unique<TRangeInferrerLight>(predicate, keyColumns);
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
           const TKeyColumns& keyColumns)
        {
            KeyTrie_ = ExtractMultipleConstraints(predicate, keyColumns, &KeyTrieBuffer_);

            LOG_DEBUG("Predicate %Qv defines key constraints %Qv", InferName(predicate), KeyTrie_);
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
            const TColumnEvaluatorCachePtr& evaluatorCache)
            : Schema_(schema)
            , KeySize_(keyColumns.size())
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

            KeyTrie_ = ExtractMultipleConstraints(predicate, depletedKeyColumns, &KeyTrieBuffer_);

            LOG_DEBUG("Predicate %Qv defines key constraints %Qv", InferName(predicate), KeyTrie_);
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

                LOG_DEBUG("Inferred key constraints %Qv", trie);

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
    };
};

std::pair<TConstQueryPtr, std::vector<TConstQueryPtr>> CoordinateQuery(
    const TConstQueryPtr& query,
    const std::vector<TKeyRange>& ranges,
    bool pushdownGroupClause)
{
    auto Logger = BuildLogger(query);

    std::vector<TConstQueryPtr> subqueries;

    auto subqueryInputRowLimit = ranges.empty()
        ? 0
        : 2 * std::min(query->InputRowLimit, std::numeric_limits<i64>::max() / 2) / ranges.size();

    auto subqueryOutputRowLimit = pushdownGroupClause
        ? query->OutputRowLimit
        : std::numeric_limits<i64>::max();

    for (const auto& keyRange : ranges) {
        // Set initial schema and key columns
        auto subquery = New<TQuery>(
            subqueryInputRowLimit,
            subqueryOutputRowLimit);

        subquery->TableSchema = query->TableSchema;
        subquery->KeyColumns = query->KeyColumns;
        subquery->Limit = query->Limit;
        subquery->JoinClause = query->JoinClause;

        // Set predicate
        int rangeSize = std::min(keyRange.first.GetCount(), keyRange.second.GetCount());

        int commonPrefixSize = 0;
        while (commonPrefixSize < rangeSize) {
            commonPrefixSize++;
            if (keyRange.first[commonPrefixSize - 1] != keyRange.second[commonPrefixSize - 1]) {
                break;
            }
        }

        if (query->Predicate) {
            subquery->Predicate = RefinePredicate(keyRange, commonPrefixSize, query->Predicate, subquery->KeyColumns);
        }

        if (query->GroupClause) {
            if (pushdownGroupClause) {
                subquery->GroupClause = query->GroupClause; 
            }
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
        if (pushdownGroupClause) {
            topQuery->TableSchema = query->GroupClause->GetTableSchema();
            if (subqueries.size() > 1) {
                topQuery->GroupClause.Emplace();

                auto& finalGroupItems = topQuery->GroupClause->GroupItems;
                for (const auto& groupItem : query->GroupClause->GroupItems) {
                    auto referenceExpr = New<TReferenceExpression>(
                        NullSourceLocation,
                        groupItem.Expression->Type,
                        groupItem.Name);
                    finalGroupItems.emplace_back(std::move(referenceExpr), groupItem.Name);
                }

                auto& finalAggregateItems = topQuery->GroupClause->AggregateItems;
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
            }
        } else {
            topQuery->TableSchema = query->TableSchema;
            topQuery->GroupClause = query->GroupClause;
        }

        topQuery->ProjectClause = query->ProjectClause;
    } else {
        topQuery->TableSchema = query->GetTableSchema();
    }

    return std::make_pair(topQuery, subqueries);
}

TDataSplits GetPrunedSplits(
    const TConstQueryPtr& query,
    const TDataSplits& splits,
    const TColumnEvaluatorCachePtr& evaluatorCache)
{
    auto Logger = BuildLogger(query);

    TRangeInferrer rangeInferrer(query->Predicate, splits, evaluatorCache);

    auto keyRangeFormatter = [] (const TKeyRange& range) -> Stroka {
        return Format("[%v .. %v]",
            range.first,
            range.second);
    };

    LOG_DEBUG("Splitting %v splits according to ranges", splits.size());

    TDataSplits prunedSplits;
    for (const auto& split : splits) {
        auto originalRange = GetBothBoundsFromDataSplit(split);

        auto ranges = rangeInferrer.GetRangesWithinRange(originalRange);

        for (const auto& range : ranges) {
            auto splitCopy = split;

            LOG_DEBUG("Narrowing split %v key range from %v to %v",
                    GetObjectIdFromDataSplit(splitCopy),
                    keyRangeFormatter(originalRange),
                    keyRangeFormatter(range));
            SetBothBounds(&splitCopy, range);

            prunedSplits.push_back(std::move(splitCopy));
        }
    }

    return prunedSplits;
}

TKeyRange GetRange(const TDataSplits& splits)
{
    if (splits.empty()) {
        return TKeyRange();
    }

    auto keyRange = GetBothBoundsFromDataSplit(splits[0]);
    for (int index = 1; index < splits.size(); ++index) {
        keyRange = Unite(keyRange, GetBothBoundsFromDataSplit(splits[index]));
    }
    return keyRange;
}

std::vector<TKeyRange> GetRanges(const TGroupedDataSplits& groupedSplits)
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
    bool isOrdered,
    const std::vector<TKeyRange>& ranges,
    std::function<TEvaluateResult(const TConstQueryPtr&, int)> evaluateSubquery,
    std::function<TQueryStatistics(const TConstQueryPtr&, ISchemafulReaderPtr, ISchemafulWriterPtr)> evaluateTop,
    bool pushdownGroupOp)
{
    auto nodeDirectory = fragment->NodeDirectory;
    auto query = fragment->Query;
    auto Logger = BuildLogger(query);

    TConstQueryPtr topQuery;
    std::vector<TConstQueryPtr> subqueries;
    std::tie(topQuery, subqueries) = CoordinateQuery(query, ranges, pushdownGroupOp);

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
            TFuture <TQueryStatistics> asyncStatistics;
            std::tie(reader, asyncStatistics) = evaluateSubquery(subquery, index);

            subqueryHolders.push_back(MakeHolder(asyncStatistics, false));

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
        queryStatistics += WaitFor(holder.Get()).ValueOrThrow();
    }

    return queryStatistics;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

