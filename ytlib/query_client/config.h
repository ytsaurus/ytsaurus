#pragma once

#include "public.h"

#include <yt/core/misc/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NQueryClient {

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

} // namespace NQueryClient
} // namespace NYT
