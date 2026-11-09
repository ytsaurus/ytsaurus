#include <yt/yt/library/query/engine_api/builtin_function_profiler.h>
#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/engine_api/config.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/misc/sync_cache.h>

namespace NYT::NQueryClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TCachedColumnEvaluator
    : public TSyncCacheValueBase<TTableSchema, TCachedColumnEvaluator>
{
public:
    TCachedColumnEvaluator(
        const TTableSchema& schema,
        TColumnEvaluatorPtr evaluator)
        : TSyncCacheValueBase(schema)
        , Evaluator_(std::move(evaluator))
    { }

    const TColumnEvaluatorPtr& GetColumnEvaluator() const
    {
        return Evaluator_;
    }

private:
    const TColumnEvaluatorPtr Evaluator_;
};

////////////////////////////////////////////////////////////////////////////////

class TColumnEvaluatorCache
    : public TSyncSlruCacheBase<TTableSchema, TCachedColumnEvaluator>
    , public IColumnEvaluatorCache
{
public:
    TColumnEvaluatorCache(
        TColumnEvaluatorCacheConfigPtr config,
        TConstTypeInferrerMapPtr typeInferrers,
        TConstFunctionProfilerMapPtr profilers)
        : TSyncSlruCacheBase(config->CGCache)
        , TypeInferrers_(std::move(typeInferrers))
        , Profilers_(std::move(profilers))
    { }

    TColumnEvaluatorPtr Find(const TTableSchemaPtr& schema) override
    {
        const auto& key = *schema;
        auto cachedEvaluator = TSyncSlruCacheBase::Find(key);
        if (!cachedEvaluator) {
            auto evaluator = TColumnEvaluator::Create(schema, TypeInferrers_, Profilers_);
            cachedEvaluator = New<TCachedColumnEvaluator>(key, std::move(evaluator));
            TryInsert(cachedEvaluator, &cachedEvaluator);
        }

        return cachedEvaluator->GetColumnEvaluator();
    }

    void Configure(const TColumnEvaluatorCacheDynamicConfigPtr& config) override
    {
        TSyncSlruCacheBase::Reconfigure(config->CGCache);
    }

    i64 GetSize() const override
    {
        return TSyncSlruCacheBase::GetSize();
    }

private:
    const TConstTypeInferrerMapPtr TypeInferrers_;
    const TConstFunctionProfilerMapPtr Profilers_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

IColumnEvaluatorCachePtr CreateColumnEvaluatorCache(
    TColumnEvaluatorCacheConfigPtr config,
    TConstTypeInferrerMapPtr typeInferrers,
    TConstFunctionProfilerMapPtr profilers)
{
    if (profilers && !profilers->empty()) {
        THROW_ERROR_EXCEPTION("Portable column evaluator cache does not support custom function profilers");
    }

    return New<TColumnEvaluatorCache>(
        std::move(config),
        std::move(typeInferrers),
        std::move(profilers));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
