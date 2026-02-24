#pragma once

#include "public.h"

#include <yt/yt/library/query/engine_api/evaluation_helpers.h>

#include <yt/yt/core/misc/async_slru_cache.h>
#include <yt/yt/core/misc/configurable_singleton_decl.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct TCodegenCacheConfig
    : public TSlruCacheConfig
{
    REGISTER_YSON_STRUCT(TCodegenCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCodegenCacheConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCodegenCacheDynamicConfig
    : public TSlruCacheDynamicConfig
{
    REGISTER_YSON_STRUCT(TCodegenCacheDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCodegenCacheDynamicConfig)

} // namespace NYT::NQueryClient
