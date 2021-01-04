#pragma once

#include "public.h"

#include <yt/core/misc/cache_config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TExecutorConfig
    : public NYTree::TYsonSerializable
{
public:
    TSlruCacheConfigPtr CGCache;

    TExecutorConfig()
    {
        RegisterParameter("cg_cache", CGCache)
            .DefaultNew();

        RegisterPreprocessor([&] () {
            CGCache->Capacity = 512;
            CGCache->ShardCount = 1;
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TExecutorConfig)

////////////////////////////////////////////////////////////////////////////////

class TColumnEvaluatorCacheConfig
    : public NYTree::TYsonSerializable
{
public:
    TSlruCacheConfigPtr CGCache;

    TColumnEvaluatorCacheConfig()
    {
        RegisterParameter("cg_cache", CGCache)
            .DefaultNew();

        RegisterPreprocessor([&] () {
            CGCache->Capacity = 512;
            CGCache->ShardCount = 1;
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TColumnEvaluatorCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TColumnEvaluatorCacheDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    TSlruCacheDynamicConfigPtr CGCache;

    TColumnEvaluatorCacheDynamicConfig()
    {
        RegisterParameter("cg_cache", CGCache)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TColumnEvaluatorCacheDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
