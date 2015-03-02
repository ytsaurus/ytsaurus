#pragma once

#include "public.h"

#include <core/misc/config.h>

#include <core/ytree/yson_serializable.h>

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

        RegisterInitializer([&] () {
            CGCache->Capacity = 100;
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

        RegisterInitializer([&] () {
            CGCache->Capacity = 100;
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TColumnEvaluatorCacheConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
