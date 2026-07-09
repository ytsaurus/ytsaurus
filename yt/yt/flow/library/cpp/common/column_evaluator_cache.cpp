#include "column_evaluator_cache.h"

#include <yt/yt/library/query/engine_api/column_evaluator.h>
#include <yt/yt/library/query/engine_api/config.h>

#include <library/cpp/containers/insert_only_concurrent_cache/cache.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

using namespace NQueryClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TFastColumnEvaluatorCache
    : public IColumnEvaluatorCache
{
public:
    explicit TFastColumnEvaluatorCache(IColumnEvaluatorCachePtr underlying)
        : Underlying_(std::move(underlying))
    { }

    TColumnEvaluatorPtr Find(const TTableSchemaPtr& schema) override
    {
        return Cache_.FindOrInsert(schema, [&] {
            return Underlying_->Find(schema);
        });
    }

    void Configure(const TColumnEvaluatorCacheDynamicConfigPtr& config) override
    {
        Underlying_->Configure(config);
    }

    i64 GetSize() const override
    {
        return Underlying_->GetSize();
    }

private:
    const IColumnEvaluatorCachePtr Underlying_;

    TInsertOnlyConcurrentCache<TTableSchemaPtr, TColumnEvaluatorPtr> Cache_;
};

////////////////////////////////////////////////////////////////////////////////

struct TFastColumnEvaluatorCacheHolder
{
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock);
    TWeakPtr<IColumnEvaluatorCache> Cache;
};

////////////////////////////////////////////////////////////////////////////////

IColumnEvaluatorCachePtr CreateFastColumnEvaluatorCache()
{
    static TFastColumnEvaluatorCacheHolder Holder;

    {
        auto guard = Guard(Holder.Lock);
        if (auto cache = Holder.Cache.Lock()) {
            return cache;
        }
    }

    IColumnEvaluatorCachePtr cache = New<TFastColumnEvaluatorCache>(
        CreateColumnEvaluatorCache(New<TColumnEvaluatorCacheConfig>()));

    {
        auto guard = Guard(Holder.Lock);
        Holder.Cache = cache;
    }

    return cache;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
