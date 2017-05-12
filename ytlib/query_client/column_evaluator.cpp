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

TColumnEvaluator::TColumnEvaluator(std::vector<TColumn> columns)
    : Columns_(std::move(columns))
{ }

TColumnEvaluatorPtr TColumnEvaluator::Create(
    const TTableSchema& schema,
    const TConstTypeInferrerMapPtr& typeInferrers,
    const TConstFunctionProfilerMapPtr& profilers)
{
    std::vector<TColumn> columns(schema.GetColumnCount());

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

        if (schema.Columns()[index].Aggregate) {
            const auto& aggregateName = schema.Columns()[index].Aggregate.Get();
            auto type = schema.Columns()[index].Type;
            column.Aggregate = CodegenAggregate(
                BuiltinAggregateCG->GetAggregate(aggregateName)->Profile(type, type, type, aggregateName));
            column.IsAggregate = true;
        }
    }

    return New<TColumnEvaluator>(std::move(columns));
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
    return Columns_[index].IsAggregate;
}

void TColumnEvaluator::InitAggregate(
    int index,
    TUnversionedValue* state,
    const TRowBufferPtr& buffer) const
{
    Columns_[index].Aggregate.Init(buffer.Get(), state);
    state->Id = index;
}

void TColumnEvaluator::UpdateAggregate(
    int index,
    TUnversionedValue* result,
    const TUnversionedValue& state,
    const TUnversionedValue& update,
    const TRowBufferPtr& buffer) const
{
    Columns_[index].Aggregate.Update(buffer.Get(), result, &state, &update);
    result->Id = index;
}

void TColumnEvaluator::MergeAggregate(
    int index,
    TUnversionedValue* result,
    const TUnversionedValue& state,
    const TUnversionedValue& mergeeState,
    const TRowBufferPtr& buffer) const
{
    Columns_[index].Aggregate.Merge(buffer.Get(), result, &state, &mergeeState);
    result->Id = index;
}

void TColumnEvaluator::FinalizeAggregate(
    int index,
    TUnversionedValue* result,
    const TUnversionedValue& state,
    const TRowBufferPtr& buffer) const
{
    Columns_[index].Aggregate.Finalize(buffer.Get(), result, &state);
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

    TColumnEvaluatorPtr GetColumnEvaluator()
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
    TConstTypeInferrerMapPtr TypeInferers_;
    TConstFunctionProfilerMapPtr Profilers_;
};

////////////////////////////////////////////////////////////////////////////////

TColumnEvaluatorCache::TColumnEvaluatorCache(
    TColumnEvaluatorCacheConfigPtr config,
    const TConstTypeInferrerMapPtr& typeInferrers,
    const TConstFunctionProfilerMapPtr& profilers)
    : Impl_(New<TImpl>(std::move(config), typeInferrers, profilers))
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
