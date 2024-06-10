#include "environment.h"

#include <yt/cpp/roren/bigrt/config/config.h>
#include <yt/cpp/roren/bigrt/proto/config.pb.h>
#include <yt/cpp/roren/library/logger/logger.h>

#include <ads/bsyeti/libs/stop_token/stop_token.h>
#include <ads/bsyeti/libs/tvm_manager/tvm_manager.h>
#include <ads/bsyeti/libs/ytex/transaction_keeper/transaction_keeper.h>
#include <bigrt/lib/queue/qyt/queue.h>
#include <bigrt/lib/supplier/logbroker/interface/supplier.h>
#include <bigrt/lib/utility/liveness_checker/known_hosts_updater.h>
#include <bigrt/lib/utility/liveness_checker/liveness_checker.h>
#include <bigrt/lib/utility/profiling/vcpu_factor/vcpu_factor.h>
#include <bigrt/lib/utility/retry/retry.h>
#include <grut/libs/client/factory/interface/provider.h>
#include <quality/user_sessions/rt/lib/common/yt_client.h>
#include <util/charset/utf8.h>
#include <yt/yt/core/concurrency/new_fair_share_thread_pool.h>
#include <yt/yt/core/concurrency/two_level_fair_share_thread_pool.h>

#include "pool_weight_provider.h"

USE_ROREN_LOGGER();

namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NRoren;

TBigRtConfigPtr ProtoToYsonConfig(const TBigRtProgramProtoConfig& protoConfig)
{
    auto ysonConfig = NYT::New<TBigRtConfig>();
    ysonConfig->MainYtCluster = protoConfig.GetMainYtCluster();
    ysonConfig->MainYtPath = protoConfig.GetMainYtPath();

    for (const auto& [name, input] : protoConfig.GetEasyInputs()) {
        auto [it, success] = ysonConfig->Computations.emplace(name, NYT::New<TComputationConfig>());
        THROW_ERROR_EXCEPTION_UNLESS(success, "Duplicating input tag %v", name);
        auto& computation = it->second;
        computation->Input = input;
    }
    for (const auto& [name, inputs] : protoConfig.GetInputs()) {
        auto [it, success] = ysonConfig->Computations.emplace(name, NYT::New<TComputationConfig>());
        THROW_ERROR_EXCEPTION_UNLESS(success, "Duplicating input tag %v", name);
        auto& computation = *it->second;
        for (const auto& input : inputs.GetSuppliers()) {
            computation.Inputs.emplace(input.GetAlias(), input);
        }
    }

    for (const auto& [name, config] : protoConfig.GetConsumers()) {
        auto& computation = *ysonConfig->Computations.at(name);
        computation.StatelessShardProcessorConfig.CopyFrom(config.GetStatelessShardProcessorConfig());
        computation.ConsumingSystemConfig.CopyFrom(config.GetConsumingSystemConfig());
        if (config.HasFallbackShardProcessorConfig()) {
            computation.FallbackShardProcessorConfig = NBigRT::TFallbackShardProcessorConfig{};
            computation.FallbackShardProcessorConfig->CopyFrom(config.GetFallbackShardProcessorConfig());
        }
        if (config.HasThrottlerQuota()) {
            computation.ThrottlerQuota = config.GetThrottlerQuota();
        }
    }

    for (const auto& [name, config] : protoConfig.GetDestinations()) {
        auto& destination = *ysonConfig->Destinations.emplace(name, NYT::New<TDestinationConfig>()).first->second;
        destination.ShardCount = config.GetShardsCount();
        if (config.HasSwiftWriter()) {
            destination.SwiftWriter = config.GetSwiftWriter();
        } else if (config.HasQytWriter()) {
            destination.YtQueueWriter = config.GetQytWriter();
        }
    }

    ysonConfig->MaxInFlightBytes = protoConfig.GetMaxInFlightBytes();
    ysonConfig->EnableWaitProcessStarted = protoConfig.GetParallelizationConfig().GetEnableWaitProcessStarted();
    ysonConfig->AutoLaunchBalancerInternalMaster = protoConfig.GetAutoLaunchBalancerInternalMaster();
    ysonConfig->UseProcessorV3 = protoConfig.GetUseProcessorV3();
    if (protoConfig.HasUserThreadCount()) {
        ysonConfig->LegacyThreadCount = protoConfig.GetUserThreadCount();
    }
    ysonConfig->Postprocess();
    return ysonConfig;
}

TString Slugify(TString str)
{
    for (auto& i: str) {
        if (!std::isalnum(i)) {
            i = '_';
        }
    }
    return str;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

TBigRtEnvironment::~TBigRtEnvironment()
{
    if (LivenessChecker) {
        LivenessChecker->Stop();
    }
    if (KnownHostsUpdater) {
        KnownHostsUpdater->Stop();
    }
}

void TBigRtEnvironment::ExpandConfig()
{
    // Expand LB Dcs.
    THashMap<TString, TComputationConfigPtr> computations;
    for (auto& [name, computation] : Config->Computations) {
        if (!computation->Input.has_value()) {
            computations.emplace(name, computation);
            continue;
        }
        auto& input = *computation->Input;
        if (!input.HasLbSupplier() || !input.GetLbSupplier().GetRorenOuterExpand()) {
            computations.emplace(name, computation);
            continue;
        }
        input.SetAlias(name);
        for (auto expandedSupplierConfig : NBigRT::ExpandLbAutoDcs(input)) {
            expandedSupplierConfig.MutableLbSupplier()->ClearRorenOuterExpand();
            auto expandedComputation = NYT::NYTree::CloneYsonStruct(computation);
            expandedComputation->Input = expandedSupplierConfig;
            computations.emplace(expandedSupplierConfig.GetAlias(), expandedComputation);
        }
    }
    Config->Computations = std::move(computations);

    // Set Computation defaults.
    if (Config->DefaultComputationSettings != nullptr) {
        const auto& defaults = Config->DefaultComputationSettings;
        THROW_ERROR_EXCEPTION_UNLESS(
            !defaults->ConsumingSystemConfig.HasMainPath()
            && !defaults->ConsumingSystemConfig.HasCluster(),
            "You shouldn't set defaults for consuming system yt params, use MainYtCluster and MainYtPath");
        for (auto& [_, config] : Config->Computations) {
            auto patch = NYT::NYTree::ConvertToNode(config);
            config = NYT::NYTree::UpdateYsonStruct(defaults, patch);
        }
    }

    // Set MainYt* params.
    THashMap<TString, TString> normalizedNames;
    for (auto& [name, computation] : Config->Computations) {
        if (computation->ConsumingSystemConfig.GetCluster().empty()) {
            THROW_ERROR_EXCEPTION_IF(Config->MainYtCluster.empty(), "Can't set consuming system cluster without MainYtCluster, computation=%v", name);
            computation->ConsumingSystemConfig.SetCluster(Config->MainYtCluster);
        }
        if (computation->ConsumingSystemConfig.GetMainPath().empty()) {
            THROW_ERROR_EXCEPTION_IF(Config->MainYtPath.empty(), "Can't set consuming system path without MainYtPath, computation=%v", name);
            auto normalizedName = ToLowerUTF8(Slugify(name));
            THROW_ERROR_EXCEPTION_IF(normalizedNames.contains(normalizedName),
                    "Topic names must have unique lowered-slugified names: %v, %v -> %v", normalizedNames[normalizedName], name, normalizedName);
                normalizedNames.emplace(normalizedName, name);
            computation->ConsumingSystemConfig.SetMainPath(NYT::Format("%v/cs/%v", Config->MainYtPath, normalizedName));
        }
        if (!computation->ConsumingSystemConfig.GetEnableFakeBalancer() && computation->ConsumingSystemConfig.GetMasterBalancing().GetMasterPath().empty() && !Config->MainYtPath.empty()) {
            computation->ConsumingSystemConfig.MutableMasterBalancing()->SetMasterPath(NYT::Format("%v/balancer", Config->MainYtPath));
        }
        if (computation->Input.has_value()) {
            THROW_ERROR_EXCEPTION_UNLESS(computation->Inputs.empty(), "computation=%v has both Input and Inputs", name);
            computation->Inputs.emplace(name, *std::move(computation->Input));
            computation->Input = std::nullopt;
        }
        for (auto& [supplierName, supplierConfig] : computation->Inputs) {
            if (supplierConfig.GetAlias().empty()) {
                supplierConfig.SetAlias(supplierName);
            }
        }
    }

    // Fetch destination shards.
    auto stopToken = NBSYeti::TStopToken(CancelableContext);
    for (auto& [name, config] : Config->Destinations) {
        if (config->SwiftWriter.has_value()) {
            THROW_ERROR_EXCEPTION_IF(config->YtQueueWriter.has_value(), "destination=%v has both YtQueue and Swift", name);
            auto& swiftWriter = *config->SwiftWriter->MutableNativeConfig();
            if (swiftWriter.GetYtCluster().empty()) {
                THROW_ERROR_EXCEPTION_IF(Config->MainYtCluster.empty(), "Can't set cluster for destination %v without MainYtCluster", name);
                swiftWriter.SetYtCluster(Config->MainYtCluster);
            }
            if (0 == swiftWriter.GetShardsCount()) {
                swiftWriter.SetShardsCount(NBigRT::RetryUntilStopped(
                    [&] () {
                        return NBigRT::TYtQueue(
                            swiftWriter.GetQueuePath(),
                            YtClients->GetClient(swiftWriter.GetYtCluster())
                        ).GetShardCount();
                    },
                    stopToken,
                    NYT::Format("Error getting shards count of swift writer queue %v", swiftWriter.GetQueuePath())));
            }
            if (config->ShardCount == 0) {
                config->ShardCount = swiftWriter.GetShardsCount();
            }
        } else if (config->YtQueueWriter.has_value()) {
           auto& qytWriterConfig = *config->YtQueueWriter;
            if (0 == qytWriterConfig.GetShardsCount()) {
                THROW_ERROR_EXCEPTION_IF(Config->MainYtCluster.empty(), "Can't fetch shards count for destination %v when main cluster is unknown");
                qytWriterConfig.SetShardsCount(NBigRT::RetryUntilStopped(
                    [&] () {
                        return NBigRT::TYtQueue(
                            qytWriterConfig.GetPath(),
                            YtClients->GetClient(Config->MainYtCluster)
                        ).GetShardCount();
                    },
                    stopToken,
                    NYT::Format("Error getting shards count for qyt %v", name)));
            }
            if (config->ShardCount == 0) {
                config->ShardCount = qytWriterConfig.GetShardsCount();
            }
        } else {
            YT_LOG_FATAL("No writer config provided for destination %v", name);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TBigRtEnvironmentPtr TBigRtEnvironment::FromProtoConfig(
    const TBigRtProgramProtoConfig& config,
    NYT::NConcurrency::ITwoLevelFairShareThreadPoolPtr threadPool,
    NYT::TCancelableContextPtr cancelableContext)
{
    auto env = NYT::New<TBigRtEnvironment>();

    env->Config = ProtoToYsonConfig(config);
    env->CancelableContext = std::move(cancelableContext);
    env->ThreadPool = std::move(threadPool);
    env->YtClients = NUserSessions::NRT::CreateTransactionKeeper(config.GetYtClientConfig());
    if (config.HasTvmConfig()) {
        env->TvmManager = NBSYeti::CreateTvmManager(config.GetTvmConfig());
    }
    env->GrutClientProvider = NGrut::NClient::CreateClientProvider(config.GetGrutConnection());

    if (config.GetKnownHostsUpdaterConfig().GetEnabled()) {
        env->KnownHostsUpdater = NBigRT::CreateKnownHostsUpdater(
            config.GetKnownHostsUpdaterConfig(),
            env->YtClients,
            env->ThreadPool->GetInvoker(NPrivate::POOL_SYSTEM, "hosts_updater"));
        env->KnownHostsUpdater->Start();
    }

    if (config.GetLivenessCheckerConfig().GetEnabled()) {
        env->LivenessChecker = NBigRT::CreateLivenessChecker(
            config.GetLivenessCheckerConfig(),
            env->YtClients,
            env->ThreadPool->GetInvoker(NPrivate::POOL_SYSTEM, "liveness_checker"));
        env->LivenessChecker->Start();
    }
    return env;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
