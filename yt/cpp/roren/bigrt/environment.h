#pragma once

#include "fwd.h"

#include <ads/bsyeti/libs/ytex/transaction_keeper/public.h>
#include <ads/core/library/cpp/persqueue/public.h>
#include <bigrt/lib/utility/liveness_checker/public.h>
#include <grut/libs/client/factory/interface/provider.h>
#include <yt/yt/client/cache/public.h>
#include <yt/yt/core/actions/public.h>
#include <yt/yt/core/concurrency/public.h>
#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/tvm/service/public.h>

namespace NRoren {

struct TBigRtEnvironment final
{
    TBigRtConfigPtr Config;
    NYT::TCancelableContextPtr CancelableContext;

    NYT::NConcurrency::ITwoLevelFairShareThreadPoolPtr ThreadPool;

    NYT::NClient::NCache::IClientsCachePtr YtClients;
    NYT::NAuth::ITvmServicePtr TvmManager;
    NPersQueue::TPQLib* PQLib = nullptr;
    NGrut::NClient::IClientProviderPtr GrutClientProvider;

    NBigRT::TKnownHostsUpdaterPtr KnownHostsUpdater;
    NBigRT::TLivenessCheckerPtr LivenessChecker;

    NYT::NProfiling::TProfiler Profiler = NYT::NProfiling::TProfiler("roren", "");
    NYT::NProfiling::TTagSet ProfilerTags;
    std::function<std::pair<TString, TString>(const TString&)> ConsumingSystemTagFactory =
        [] (const TString& name) { return std::pair{"computation", name}; };

    static TBigRtEnvironmentPtr FromProtoConfig(
        const TBigRtProgramProtoConfig& config,
        NYT::NConcurrency::ITwoLevelFairShareThreadPoolPtr threadPool,
        NYT::TCancelableContextPtr cancelableContext);

    static TBigRtEnvironmentPtr FromYsonConfig(
        TBigRtProgramConfigPtr config,
        NYT::TCancelableContextPtr cancelableContext);

    TBigRtEnvironment() = default;

    ~TBigRtEnvironment();

    void ExpandConfig();
};

} // namespace NRoren
