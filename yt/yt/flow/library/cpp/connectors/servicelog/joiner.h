#pragma once

#include "fetcher.h"
#include "provider.h"
#include "public.h"

#include <yt/yt/flow/library/cpp/common/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TFetcherInJoinerSpec);

struct TFetcherInJoinerSpec
    : public TTableFetcherSpec
{
    std::string Prefix;

    REGISTER_YSON_STRUCT(TFetcherInJoinerSpec);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("prefix", &TFetcherInJoinerSpec::Prefix)
            .Default("");
    }
};

DEFINE_REFCOUNTED_TYPE(TFetcherInJoinerSpec);

////////////////////////////////////////////////////////////////////////////////

struct TTableJoinerSpec
    : public NYTree::TYsonStruct
{
    std::vector<TFetcherInJoinerSpecPtr> Fetchers;

    REGISTER_YSON_STRUCT(TTableJoinerSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTableJoinerSpec);

////////////////////////////////////////////////////////////////////////////////

IServiceLogRowsProviderPtr CreateTableJoiner(
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler,
    std::vector<std::pair<std::string, IServiceLogRowsProviderPtr>> secondaryFetchers);

IServiceLogRowsProviderPtr CreateTableJoiner(
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler,
    IStatusProfilerPtr statusProfiler,
    NClient::NCache::IClientsCachePtr clientsCache,
    TTableJoinerSpecPtr spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
