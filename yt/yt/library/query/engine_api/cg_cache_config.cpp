#include "cg_cache_config.h"

#include <library/cpp/yt/memory/leaky_ref_counted_singleton.h>

namespace NYT::NQueryClient {

void TCodegenCacheConfig::Register(TRegistrar registrar)
{
    registrar.Preprocessor([] (TThis* config) {
        config->Capacity = 512;
        config->ShardCount = 1;
    });
}

void TCodegenCacheDynamicConfig::Register(TRegistrar /*registrar*/)
{ }

} // namespace NYT::NQueryClient
