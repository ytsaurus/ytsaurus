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
    std::vector<bool> isAggregate)
    : Columns_(std::move(columns))
    , IsAggregate_(std::move(isAggregate))
{ }

TColumnEvaluatorPtr TColumnEvaluator::Create(
    const TTableSchema& schema,
    const TConstTypeInferrerMapPtr& typeInferrers,
    const TConstFunctionProfilerMapPtr& profilers)
{
    std::vector<TColumn> columns(schema.GetColumnCount());
    std::vector<bool> isAggregate(schema.GetColumnCount());

    for (int index = 0; index < schema.GetColumnCount(); ++index) {
        auto& column = columns[index];
        if (schema.Columns()[index].Expression()) {
            THashSet<TString> references;

            column.Expression = PrepareExpression(
                schema.Columns()[index].Expression().Get(),
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

        if (schema.Columns()[index].Aggregate()) {
            const auto& aggregateName = schema.Columns()[index].Aggregate().Get();
            auto type = schema.Columns()[index].GetPhysicalType();
            column.Aggregate = CodegenAggregate(
                BuiltinAggregateProfilers->GetAggregate(aggregateName)->Profile(type, type, type, aggregateName),
                type, type);
            isAggregate[index] = true;
        }
    }

    return New<TColumnEvaluator>(std::move(columns), std::move(isAggregate));
}

void TColumnEvaluator::EvaluateKey(TMutableRow fullRow, const TRowBufferPtr& buffer, int index) const
{
    YCHECK(index < fullRow.GetCount());
    YCHECK(index < Columns_.size());

    const auto& column = Columns_[index];
    const auto& evaluator = column.Evaluator;
    YCHECK(evaluator);

    // Zero row to avoid garbage after evaluator.
    fullRow[index] = MakeUnversionedSentinelValue(EValueType::Null);

    evaluator(
        column.Variables.GetLiteralValues(),
        column.Variables.GetOpaqueData(),
        &fullRow[index],
        fullRow.Begin(),
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

void TColumnEvaluator::EvaluateKeys(
    TMutableVersionedRow fullRow,
    const TRowBufferPtr& buffer) const
{
    auto row = buffer->Capture(fullRow.BeginKeys(), fullRow.GetKeyCount(), false);
    EvaluateKeys(row, buffer);

    for (int index = 0; index < fullRow.GetKeyCount(); ++index) {
        if (Columns_[index].Evaluator) {
            fullRow.BeginKeys()[index] = row[index];
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
    TUnversionedValue* state,
    const TUnversionedValue& update,
    const TRowBufferPtr& buffer) const
{
    Columns_[index].Aggregate.Update(buffer.Get(), state, &update);
    state->Id = index;
}

void TColumnEvaluator::MergeAggregate(
    int index,
    TUnversionedValue* state,
    const TUnversionedValue& mergeeState,
    const TRowBufferPtr& buffer) const
{
    Columns_[index].Aggregate.Merge(buffer.Get(), state, &mergeeState);
    state->Id = index;
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
