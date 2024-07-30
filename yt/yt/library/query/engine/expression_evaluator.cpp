#include "folding_profiler.h"

#include <yt/yt/library/query/base/private.h>
#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/library/query/engine_api/expression_evaluator.h>
#include <yt/yt/library/query/engine_api/config.h>

#include <yt/yt/core/misc/async_slru_cache.h>

namespace NYT::NQueryClient {

using namespace NConcurrency;
using namespace NTableClient;

using TKey = std::pair<TString, llvm::FoldingSetNodeID>;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

TExpressionEvaluatorPtr TExpressionEvaluator::Create(
    const TParsedSource& expression,
    const TTableSchemaPtr& schema,
    const TConstTypeInferrerMapPtr& typeInferrers,
    const TConstFunctionProfilerMapPtr& profilers)
{
    auto typedExpression = PrepareExpression(
        expression,
        *schema,
        typeInferrers);

    auto variables = TCGVariables();

    auto image = Profile(
        typedExpression,
        schema,
        /*id*/ nullptr,
        &variables,
        /*useCanonicalNullRelations*/ false,
        NCodegen::EExecutionBackend::Native,
        profilers)();

    return New<TExpressionEvaluator>(std::move(image), std::move(variables));
}

////////////////////////////////////////////////////////////////////////////////

class TCachedExpressionEvaluator
    : public TAsyncCacheValueBase<TKey, TCachedExpressionEvaluator>
{
public:
    TCachedExpressionEvaluator(
        TKey id,
        TExpressionEvaluatorPtr evaluator)
        : TAsyncCacheValueBase(std::move(id))
        , Evaluator_(std::move(evaluator))
    { }

    const TExpressionEvaluatorPtr& GetExpressionEvaluator()
    {
        return Evaluator_;
    }

private:
    const TExpressionEvaluatorPtr Evaluator_;
};

////////////////////////////////////////////////////////////////////////////////

class TExpressionEvaluatorCache
    : public TAsyncSlruCacheBase<TKey, TCachedExpressionEvaluator>
    , public IExpressionEvaluatorCache
{
public:
    TExpressionEvaluatorCache(
        TExpressionEvaluatorCacheConfigPtr config,
        const TConstTypeInferrerMapPtr& typeInferrers,
        const TConstFunctionProfilerMapPtr& profilers)
        : TAsyncSlruCacheBase(config->CGCache)
        , TypeInferers_(typeInferrers)
        , Profilers_(profilers)
    { }

    TExpressionEvaluatorPtr Find(const TParsedSource& parsedSource, const TTableSchemaPtr& schema) override
    {
        TKey key;
        Profile(schema, &key.second);
        key.first = parsedSource.Source;

        auto cookie = TAsyncSlruCacheBase::BeginInsert(key);
        if (cookie.IsActive()) {
            YT_LOG_DEBUG("Codegen cache miss: generating expression evaluator (Expression: %v, Schema: %v)",
                parsedSource.Source,
                *schema);

            try {
                auto evaluator = TExpressionEvaluator::Create(
                    parsedSource,
                    schema,
                    TypeInferers_,
                    Profilers_);
                cookie.EndInsert(New<TCachedExpressionEvaluator>(std::move(key), std::move(evaluator)));
            } catch (const std::exception& ex) {
                YT_LOG_DEBUG(ex, "Failed to compile an expression");
                cookie.Cancel(TError(ex).Wrap("Failed to compile an expression"));
            }
        }

        return WaitForFast(cookie.GetValue())
            .ValueOrThrow()->GetExpressionEvaluator();
    }

private:
    const TConstTypeInferrerMapPtr TypeInferers_;
    const TConstFunctionProfilerMapPtr Profilers_;
};

IExpressionEvaluatorCachePtr CreateExpressionEvaluatorCache(
    TExpressionEvaluatorCacheConfigPtr config,
    TConstTypeInferrerMapPtr typeInferrers,
    TConstFunctionProfilerMapPtr profilers)
{
    return New<TExpressionEvaluatorCache>(
        std::move(config),
        std::move(typeInferrers),
        std::move(profilers));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
