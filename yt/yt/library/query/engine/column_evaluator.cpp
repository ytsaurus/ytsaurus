
#include "cg_fragment_compiler.h"
#include "folding_profiler.h"
#include "functions_cg.h"

#include <yt/yt/library/query/base/query_preparer.h>
#include <yt/yt/library/query/base/functions.h>
#include <yt/yt/library/query/base/private.h>

#include <yt/yt/library/query/engine_api/builtin_function_profiler.h>
#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/engine_api/config.h>

#include <yt/yt/client/query_client/query_statistics.h>

#include <yt/yt/core/misc/sync_cache.h>

namespace NYT::NQueryClient {

using namespace NTableClient;
using namespace NYTree;

using NCodegen::EExecutionBackend;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

// TODO(dtorilov): Consider enabling WebAssembly for column evaluators.

TColumnEvaluatorPtr TColumnEvaluator::Create(
    const TTableSchemaPtr& schema,
    const TConstTypeInferrerMapPtr& typeInferrers,
    const TConstFunctionProfilerMapPtr& profilers)
{
    std::vector<TColumn> columns(schema->GetColumnCount());
    std::vector<bool> isAggregate(schema->GetColumnCount());

    for (int index = 0; index < schema->GetColumnCount(); ++index) {
        THashSet<std::string> references;
        auto& column = columns[index];

        if (schema->Columns()[index].Expression()) {
            column.Expression = PrepareExpression(
                *schema->Columns()[index].Expression(),
                *schema,
                typeInferrers,
                &references);

            column.EvaluatorImage = Profile(
                column.Expression,
                schema,
                /*id*/ nullptr,
                &column.Variables,
                /*useCanonicalNullRelations*/ false,
                EExecutionBackend::Native,
                profilers)();

            column.EvaluatorInstance = column.EvaluatorImage.Instantiate();

            for (const auto& reference : references) {
                column.ReferenceIds.push_back(schema->GetColumnIndexOrThrow(reference));
            }
            std::sort(column.ReferenceIds.begin(), column.ReferenceIds.end());
        }

        if (schema->Columns()[index].Aggregate()) {
            const auto& aggregateName = *schema->Columns()[index].Aggregate();
            if (auto nested = TryParseNestedAggregate(aggregateName)) {
                continue;
            }
            auto type = schema->Columns()[index].GetWireType();
            column.AggregateImage = CodegenAggregate(
                GetBuiltinAggregateProfilers()->GetAggregate(aggregateName)->Profile(
                    {type},
                    type,
                    type,
                    aggregateName,
                    EExecutionBackend::Native),
                {type},
                type,
                EExecutionBackend::Native);
            column.AggregateInstance = column.AggregateImage.Instantiate();
            isAggregate[index] = true;
        }

        // Perform lazy initialization to make Variables immutable and thread-safe in the future.
        column.Variables.GetLiteralValues();
    }

    return New<TColumnEvaluator>(std::move(columns), std::move(isAggregate));
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
class TColumnEvaluatorCache
    : public TSyncSlruCacheBase<llvm::FoldingSetNodeID, TCachedColumnEvaluator>
    , public IColumnEvaluatorCache
{
public:
    TColumnEvaluatorCache(
        TColumnEvaluatorCacheConfigPtr config,
        const TConstTypeInferrerMapPtr& typeInferrers,
        const TConstFunctionProfilerMapPtr& profilers)
        : TSyncSlruCacheBase(config->CGCache)
        , TypeInferrers_(typeInferrers)
        , Profilers_(profilers)
    { }

    TColumnEvaluatorPtr Find(const TTableSchemaPtr& schema) override
    {
        llvm::FoldingSetNodeID id;
        Profile(schema, &id);

        auto cachedEvaluator = TSyncSlruCacheBase::Find(id);
        if (!cachedEvaluator) {
            YT_LOG_DEBUG("Codegen cache miss: generating column evaluator (Schema: %v)",
                *schema);

            auto evaluator = TColumnEvaluator::Create(
                schema,
                TypeInferrers_,
                Profilers_);
            cachedEvaluator = New<TCachedColumnEvaluator>(id, evaluator);

            TryInsert(cachedEvaluator, &cachedEvaluator);
        }

        return cachedEvaluator->GetColumnEvaluator();
    }

    void Configure(const TColumnEvaluatorCacheDynamicConfigPtr& config) override
    {
        TSyncSlruCacheBase::Reconfigure(config->CGCache);
    }

private:
    const TConstTypeInferrerMapPtr TypeInferrers_;
    const TConstFunctionProfilerMapPtr Profilers_;
};

IColumnEvaluatorCachePtr CreateColumnEvaluatorCache(
    TColumnEvaluatorCacheConfigPtr config,
    TConstTypeInferrerMapPtr typeInferrers,
    TConstFunctionProfilerMapPtr profilers)
{
    return New<TColumnEvaluatorCache>(
        std::move(config),
        std::move(typeInferrers),
        std::move(profilers));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
