#include "column_evaluator.h"
#include "cg_fragment_compiler.h"
#include "config.h"
#include "folding_profiler.h"
#include "query_preparer.h"
#include "query_statistics.h"
#include "functions.h"
#include "functions_cg.h"

#include <yt/core/misc/sync_cache.h>

namespace NYT {
namespace NQueryClient {

using namespace NTableClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

TColumnEvaluator::TColumnEvaluator(
    std::vector<TColumn> columns,
    std::unordered_map<int, TCGAggregateCallbacks> aggregates)
    : Columns_(std::move(columns))
    , Aggregates_(std::move(aggregates))
{ }

TColumnEvaluatorPtr TColumnEvaluator::Create(
    const TTableSchema& schema,
    const TConstTypeInferrerMapPtr& typeInferrers,
    const TConstFunctionProfilerMapPtr& profilers)
{
    std::vector<TColumn> columns(schema.GetColumnCount());
    std::unordered_map<int, TCGAggregateCallbacks> aggregates;

    for (int index = 0; index < schema.GetColumnCount(); ++index) {
        auto& column = columns[index];
        if (schema.Columns()[index].Expression) {
            yhash_set<TString> references;

            column.Expression = PrepareExpression(
                schema.Columns()[index].Expression.Get(),
                schema,
                typeInferrers,
                &references);

            column.Evaluator = Profile(
                column.Expression,
                schema,
                nullptr,
                &column.Variables,
                profilers)();

            for (const auto& reference : references) {
                column.ReferenceIds.push_back(schema.GetColumnIndexOrThrow(reference));
            }
            std::sort(column.ReferenceIds.begin(), column.ReferenceIds.end());
        }
    }

    for (int index = schema.GetKeyColumnCount(); index < schema.Columns().size(); ++index) {
        if (schema.Columns()[index].Aggregate) {
            const auto& aggregateName = schema.Columns()[index].Aggregate.Get();
            auto type = schema.Columns()[index].Type;
            aggregates[index] = CodegenAggregate(
                BuiltinAggregateCG->GetAggregate(aggregateName)->Profile(type, type, type, aggregateName));
        }
    }

    return New<TColumnEvaluator>(
        std::move(columns),
        std::move(aggregates));
}

void TColumnEvaluator::EvaluateKey(TMutableRow fullRow, const TRowBufferPtr& buffer, int index) const
{
    YCHECK(index < fullRow.GetCount());
    YCHECK(index < Columns_.size());

    const auto& column = Columns_[index];
    const auto& evaluator = column.Evaluator;
    YCHECK(evaluator);

    // Zeroizing row to avoid garbage after evaluator.
    fullRow[index] = MakeUnversionedSentinelValue(EValueType::Null);

    evaluator(
        column.Variables.GetOpaqueData(),
        &fullRow[index],
        fullRow,
        buffer.Get());

    fullRow[index].Id = index;
}

void TColumnEvaluator::EvaluateKeys(TMutableRow fullRow, const TRowBufferPtr& buffer) const
{
    for (int index = 0; index < Columns_.size(); ++index) {
        if (Columns_[index].Evaluator) {
            EvaluateKey(fullRow, buffer, index);
        }
    }
}

const std::vector<int>& TColumnEvaluator::GetReferenceIds(int index) const
{
    return Columns_[index].ReferenceIds;
}

TConstExpressionPtr TColumnEvaluator::GetExpression(int index) const
{
    return Columns_[index].Expression;
}

bool TColumnEvaluator::IsAggregate(int index) const
{
    return Aggregates_.count(index);
}

void TColumnEvaluator::InitAggregate(
    int index,
    TUnversionedValue* state,
    const TRowBufferPtr& buffer) const
{
    auto found = Aggregates_.find(index);
    YCHECK(found != Aggregates_.end());
    found->second.Init(buffer.Get(), state);
    state->Id = index;
}

void TColumnEvaluator::UpdateAggregate(
    int index,
    TUnversionedValue* result,
    const TUnversionedValue& state,
    const TUnversionedValue& update,
    const TRowBufferPtr& buffer) const
{
    auto found = Aggregates_.find(index);
    YCHECK(found != Aggregates_.end());
    found->second.Update(buffer.Get(), result, &state, &update);
    result->Id = index;
}

void TColumnEvaluator::MergeAggregate(
    int index,
    TUnversionedValue* result,
    const TUnversionedValue& state,
    const TUnversionedValue& mergeeState,
    const TRowBufferPtr& buffer) const
{
    auto found = Aggregates_.find(index);
    YCHECK(found != Aggregates_.end());
    found->second.Merge(buffer.Get(), result, &state, &mergeeState);
    result->Id = index;
}

void TColumnEvaluator::FinalizeAggregate(
    int index,
    TUnversionedValue* result,
    const TUnversionedValue& state,
    const TRowBufferPtr& buffer) const
{
    auto found = Aggregates_.find(index);
    YCHECK(found != Aggregates_.end());
    found->second.Finalize(buffer.Get(), result, &state);
    result->Id = index;
}

////////////////////////////////////////////////////////////////////////////////

class TCachedColumnEvaluator
    : public TSyncCacheValueBase<llvm::FoldingSetNodeID, TCachedColumnEvaluator>
{
public:
    TCachedColumnEvaluator(
        const llvm::FoldingSetNodeID& id,
        TColumnEvaluatorPtr evaluator)
        : TSyncCacheValueBase(id)
        , Evaluator_(std::move(evaluator))
    { }

    const TColumnEvaluatorPtr& GetColumnEvaluator()
    {
        return Evaluator_;
    }

private:
    const TColumnEvaluatorPtr Evaluator_;
};

// TODO(lukyan): Use async cache?
class TColumnEvaluatorCache::TImpl
    : public TSyncSlruCacheBase<llvm::FoldingSetNodeID, TCachedColumnEvaluator>
{
public:
    explicit TImpl(
        TColumnEvaluatorCacheConfigPtr config,
        const TConstTypeInferrerMapPtr& typeInferrers,
        const TConstFunctionProfilerMapPtr& profilers)
        : TSyncSlruCacheBase(config->CGCache)
        , TypeInferers_(typeInferrers)
        , Profilers_(profilers)
    { }

    TColumnEvaluatorPtr Get(const TTableSchema& schema)
    {
        llvm::FoldingSetNodeID id;
        Profile(schema, &id);

        auto cachedEvaluator = Find(id);
        if (!cachedEvaluator) {
            LOG_DEBUG("Codegen cache miss: generating column evaluator (Schema: %v)",
                schema);

            auto evaluator = TColumnEvaluator::Create(
                schema,
                TypeInferers_,
                Profilers_);
            cachedEvaluator = New<TCachedColumnEvaluator>(id, evaluator);

            TryInsert(cachedEvaluator, &cachedEvaluator);
        }

        return cachedEvaluator->GetColumnEvaluator();
    }

private:
    const TConstTypeInferrerMapPtr TypeInferers_;
    const TConstFunctionProfilerMapPtr Profilers_;
};

////////////////////////////////////////////////////////////////////////////////

TColumnEvaluatorCache::TColumnEvaluatorCache(
    TColumnEvaluatorCacheConfigPtr config,
    const TConstTypeInferrerMapPtr& typeInferrers,
    const TConstFunctionProfilerMapPtr& profilers)
    : Impl_(New<TImpl>(
        std::move(config),
        typeInferrers,
        profilers))
{ }

TColumnEvaluatorCache::~TColumnEvaluatorCache() = default;

TColumnEvaluatorPtr TColumnEvaluatorCache::Find(
    const TTableSchema& schema)
{
    return Impl_->Get(schema);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
