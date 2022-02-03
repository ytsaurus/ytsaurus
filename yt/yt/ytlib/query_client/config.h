#pragma once

#include "public.h"

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TExecutorConfig
    : public NYTree::TYsonSerializable
{
public:
    TSlruCacheConfigPtr CGCache;

    TExecutorConfig();
};

DEFINE_REFCOUNTED_TYPE(TExecutorConfig)

////////////////////////////////////////////////////////////////////////////////

class TColumnEvaluatorCacheConfig
    : public NYTree::TYsonSerializable
{
public:
    TSlruCacheConfigPtr CGCache;

    TColumnEvaluatorCacheConfig();
};

DEFINE_REFCOUNTED_TYPE(TColumnEvaluatorCacheConfig)

////////////////////////////////////////////////////////////////////////////////

class TColumnEvaluatorCacheDynamicConfig
    : public NYTree::TYsonSerializable
{
public:
    TSlruCacheDynamicConfigPtr CGCache;

    TColumnEvaluatorCacheDynamicConfig();
};

DEFINE_REFCOUNTED_TYPE(TColumnEvaluatorCacheDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
