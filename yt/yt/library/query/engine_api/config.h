#pragma once

#include "public.h"

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct TExecutorConfig
    : public NYTree::TYsonStruct
{
    TSlruCacheConfigPtr CGCache;

    REGISTER_YSON_STRUCT(TExecutorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TExecutorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TColumnEvaluatorCacheConfig
    : public NYTree::TYsonStruct
{
    TSlruCacheConfigPtr CGCache;

    REGISTER_YSON_STRUCT(TColumnEvaluatorCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TColumnEvaluatorCacheConfig)

////////////////////////////////////////////////////////////////////////////////

struct TColumnEvaluatorCacheDynamicConfig
    : public NYTree::TYsonStruct
{
    TSlruCacheDynamicConfigPtr CGCache;

    REGISTER_YSON_STRUCT(TColumnEvaluatorCacheDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TColumnEvaluatorCacheDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TExpressionEvaluatorCacheConfig
    : public NYTree::TYsonStruct
{
    TSlruCacheConfigPtr CGCache;

    REGISTER_YSON_STRUCT(TExpressionEvaluatorCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TExpressionEvaluatorCacheConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
