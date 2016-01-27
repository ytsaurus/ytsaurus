#include "column_evaluator.h"
#include "cg_fragment_compiler.h"
#include "config.h"
#include "folding_profiler.h"
#include "query_preparer.h"
#include "query_statistics.h"

#include <yt/core/misc/sync_cache.h>

namespace NYT {
namespace NQueryClient {

using namespace NTableClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TColumnEvaluator::TColumnEvaluator(
    const TTableSchema& schema,
    int keyColumnCount,
    IFunctionRegistryPtr functionRegistry)
    : TableSchema_(schema)
    , KeyColumnCount_(keyColumnCount)
    , FunctionRegistry_(std::move(functionRegistry))
    , Evaluators_(keyColumnCount)
    , Variables_(keyColumnCount)
    , ReferenceIds_(keyColumnCount)
    , Expressions_(keyColumnCount)
    , AllLiteralArgs_(keyColumnCount)
{ }

TColumnEvaluatorPtr TColumnEvaluator::Create(
    const TTableSchema& schema,
    int keyColumnCount,
    IFunctionRegistryPtr functionRegistry)
{
    auto evaluator = New<TColumnEvaluator>(schema, keyColumnCount, std::move(functionRegistry));
    evaluator->Prepare();
    return evaluator;
}

void TColumnEvaluator::Prepare()
{
    for (int index = 0; index < KeyColumnCount_; ++index) {
        if (TableSchema_.Columns()[index].Expression) {
            yhash_set<Stroka> references;
            Expressions_[index] = PrepareExpression(
<<<<<<< HEAD
                TableSchema_.Columns()[index].Expression.Get(),
                TableSchema_,
                FunctionRegistry_);
=======
                Schema_.Columns()[index].Expression.Get(),
                Schema_,
                FunctionRegistry_,
                &references);
>>>>>>> prestable/0.17.4
            Evaluators_[index] = Profile(
                Expressions_[index],
                TableSchema_,
                nullptr,
                &Variables_[index],
                &AllLiteralArgs_[index],
                FunctionRegistry_)();

            for (const auto& reference : references) {
                ReferenceIds_[index].push_back(TableSchema_.GetColumnIndexOrThrow(reference));
            }
            std::sort(ReferenceIds_[index].begin(), ReferenceIds_[index].end());
        }
    }

    for (int index = KeyColumnCount_; index < TableSchema_.Columns().size(); ++index) {
        if (TableSchema_.Columns()[index].Aggregate) {
            const auto& aggregateName = TableSchema_.Columns()[index].Aggregate.Get();
            auto type = TableSchema_.Columns()[index].Type;
            auto aggregate = FunctionRegistry_->GetAggregateFunction(aggregateName);
            Aggregates_[index] = CodegenAggregate(aggregate->MakeCodegenAggregate(type, type, type, aggregateName));
        }
    }
}

void TColumnEvaluator::EvaluateKey(TMutableRow fullRow, const TRowBufferPtr& buffer, int index) const
{
    YCHECK(index < fullRow.GetCount());
    YCHECK(index < KeyColumnCount_);
    YCHECK(TableSchema_.Columns()[index].Expression);

    TQueryStatistics statistics;
    TExecutionContext executionContext;
    executionContext.Schema = &TableSchema_;
    executionContext.LiteralRows = &Variables_[index].LiteralRows;
    executionContext.PermanentBuffer = buffer;
    executionContext.OutputBuffer = buffer;
    executionContext.IntermediateBuffer = buffer;
    executionContext.Statistics = &statistics;
#ifndef NDEBUG
    int dummy;
    executionContext.StackSizeGuardHelper = reinterpret_cast<size_t>(&dummy);
#endif

    std::vector<TFunctionContext*> functionContexts;
    for (auto& literalArgs : AllLiteralArgs_[index]) {
        executionContext.FunctionContexts.emplace_back(std::move(literalArgs));
    }
    for (auto& functionContext : executionContext.FunctionContexts) {
        functionContexts.push_back(&functionContext);
    }

    Evaluators_[index](
        &fullRow[index],
        fullRow,
        const_cast<TRowBuilder&>(Variables_[index].ConstantsRowBuilder).GetRow(),
        &executionContext,
        &functionContexts[0]);

    fullRow[index].Id = index;
}

void TColumnEvaluator::EvaluateKeys(TMutableRow fullRow, const TRowBufferPtr& buffer) const
{
    for (int index = 0; index < KeyColumnCount_; ++index) {
        if (TableSchema_.Columns()[index].Expression) {
            EvaluateKey(fullRow, buffer, index);
        }
    }
}

const std::vector<int>& TColumnEvaluator::GetReferenceIds(int index) const
{
    return ReferenceIds_[index];
}

TConstExpressionPtr TColumnEvaluator::GetExpression(int index) const
{
    return Expressions_[index];
}

void TColumnEvaluator::VerifyAggregate(int index)
{
    YCHECK(index < TableSchema_.Columns().size());
    YCHECK(TableSchema_.Columns()[index].Aggregate);
}

void TColumnEvaluator::InitAggregate(
    int index,
    TUnversionedValue* state,
    const TRowBufferPtr& buffer)
{
    VerifyAggregate(index);

    TExecutionContext executionContext;
    executionContext.PermanentBuffer = buffer;
    executionContext.OutputBuffer = buffer;
    executionContext.IntermediateBuffer = buffer;

    Aggregates_[index].Init(&executionContext, state);
    state->Id = index;
}

void TColumnEvaluator::UpdateAggregate(
    int index,
    TUnversionedValue* result,
    const TUnversionedValue& state,
    const TUnversionedValue& update,
    const TRowBufferPtr& buffer)
{
    VerifyAggregate(index);

    TExecutionContext executionContext;
    executionContext.PermanentBuffer = buffer;
    executionContext.OutputBuffer = buffer;
    executionContext.IntermediateBuffer = buffer;

    Aggregates_[index].Update(&executionContext, result, &state, &update);
    result->Id = index;
}

void TColumnEvaluator::MergeAggregate(
    int index,
    TUnversionedValue* result,
    const TUnversionedValue& state,
    const TUnversionedValue& mergeeState,
    const TRowBufferPtr& buffer)
{
    VerifyAggregate(index);

    TExecutionContext executionContext;
    executionContext.PermanentBuffer = buffer;
    executionContext.OutputBuffer = buffer;
    executionContext.IntermediateBuffer = buffer;

    Aggregates_[index].Merge(&executionContext, result, &state, &mergeeState);
    result->Id = index;
}

void TColumnEvaluator::FinalizeAggregate(
    int index,
    TUnversionedValue* result,
    const TUnversionedValue& state,
    const TRowBufferPtr& buffer)
{
    VerifyAggregate(index);

    TExecutionContext executionContext;
    executionContext.PermanentBuffer = buffer;
    executionContext.OutputBuffer = buffer;
    executionContext.IntermediateBuffer = buffer;

    Aggregates_[index].Finalize(&executionContext, result, &state);
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

class TColumnEvaluatorCache::TImpl
    : public TSyncSlruCacheBase<llvm::FoldingSetNodeID, TCachedColumnEvaluator>
{
public:
    explicit TImpl(
        TColumnEvaluatorCacheConfigPtr config,
        IFunctionRegistryPtr functionRegistry)
        : TSyncSlruCacheBase(config->CGCache)
        , FunctionRegistry_(std::move(functionRegistry))
    { }

    TColumnEvaluatorPtr Get(const TTableSchema& schema, int keyColumnCount)
    {
        llvm::FoldingSetNodeID id;
        Profile(schema, keyColumnCount, &id, FunctionRegistry_);

        auto cachedEvaluator = Find(id);
        if (!cachedEvaluator) {
            auto evaluator = TColumnEvaluator::Create(schema, keyColumnCount, FunctionRegistry_);
            cachedEvaluator = New<TCachedColumnEvaluator>(id, evaluator);

            TryInsert(cachedEvaluator, &cachedEvaluator);
        }

        return cachedEvaluator->GetColumnEvaluator();
    }

private:
    const IFunctionRegistryPtr FunctionRegistry_;
};

////////////////////////////////////////////////////////////////////////////////

TColumnEvaluatorCache::TColumnEvaluatorCache(
    TColumnEvaluatorCacheConfigPtr config,
    IFunctionRegistryPtr functionRegistry)
    : Impl_(New<TImpl>(std::move(config), std::move(functionRegistry)))
{ }

TColumnEvaluatorCache::~TColumnEvaluatorCache() = default;

TColumnEvaluatorPtr TColumnEvaluatorCache::Find(
    const TTableSchema& schema,
    int keyColumnCount)
{
    return Impl_->Get(schema, keyColumnCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
