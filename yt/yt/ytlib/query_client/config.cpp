#include "config.h"

#include <yt/yt/core/misc/cache_config.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TExecutorConfig::TExecutorConfig()
{
    RegisterParameter("cg_cache", CGCache)
        .DefaultNew();

    RegisterPreprocessor([&] () {
        CGCache->Capacity = 512;
        CGCache->ShardCount = 1;
    });
}

////////////////////////////////////////////////////////////////////////////////

TColumnEvaluatorCacheConfig::TColumnEvaluatorCacheConfig()
{
    RegisterParameter("cg_cache", CGCache)
        .DefaultNew();

    RegisterPreprocessor([&] () {
        CGCache->Capacity = 512;
        CGCache->ShardCount = 1;
    });
}

////////////////////////////////////////////////////////////////////////////////

TColumnEvaluatorCacheDynamicConfig::TColumnEvaluatorCacheDynamicConfig()
{
    RegisterParameter("cg_cache", CGCache)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
