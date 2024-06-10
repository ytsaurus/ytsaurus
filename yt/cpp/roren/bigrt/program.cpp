#include "bigrt.h"
#include "pipeline_executor.h"
#include "pool_weight_provider.h"
#include "program.h"

#include <ads/bsyeti/libs/profiling/solomon/exporter.h>
#include <ads/bsyeti/libs/stop_token/stop_token.h>
#include <ads/bsyeti/libs/ytex/http/server.h>
#include <ads/bsyeti/libs/ytex/http/std_handlers.h>
#include <ads/bsyeti/libs/ytex/program/program.h>
#include <bigrt/lib/queue/qyt/queue.h>
#include <bigrt/lib/supplier/logbroker/interface/supplier.h>
#include <bigrt/lib/utility/profiling/vcpu_factor/vcpu_factor.h>
#include <bigrt/lib/utility/retry/retry.h>

#include <library/cpp/string_utils/parse_size/parse_size.h>
#include <quality/user_sessions/rt/lib/common/yt_client.h>
#include <util/charset/utf8.h>
#include <util/generic/ptr.h>
#include <yt/cpp/roren/library/logger/logger.h>
#include <yt/yt/core/concurrency/new_fair_share_thread_pool.h>
#include <yt/yt/core/http/config.h>
#include <yt/yt/core/http/server.h>

#include <memory>
#include <cctype>

USE_ROREN_LOGGER();

namespace {

////////////////////////////////////////////////////////////////////////////////

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

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

struct TBigRtProgramBaseInternal::TInternal
{
    NYT::NProfiling::TProfiler Profiler = NYT::NProfiling::TProfiler("roren", "");
    NYT::NProfiling::TTagSet ProfilerTags;
    NYT::NHttp::IRequestPathMatcherPtr HttpHandlers = NYT::New<NYT::NHttp::TRequestPathMatcher>();
    std::function<std::pair<TString, TString>(const TString&)> ConsumingSystemTagFactory =
        [] (const TString& inputName) -> std::pair<TString, TString> {
            return {"computation", inputName};
        };
    NYT::NConcurrency::ITwoLevelFairShareThreadPoolPtr ThreadPool;
};

////////////////////////////////////////////////////////////////////////////////

TBigRtProgramBaseInternal::TBigRtProgramBaseInternal(NYT::TCancelableContextPtr cancelableContext, std::function<TBigRtProgramProtoConfig&()> configGetter)
    : TProgram(std::move(cancelableContext))
    , TBigRtConfigBuilderOps(std::move(configGetter))
    , Internal_(std::make_shared<TInternal>())
{
}

NYT::NHttp::IRequestPathMatcherPtr TBigRtProgramBaseInternal::GetHttpHandlers()
{
    return Internal_->HttpHandlers;
}

void TBigRtProgramBaseInternal::Run(std::function<void(TPipeline&)> pipelineConstructor)
{
    const TString programName = "roren";
    NYTEx::TProgramBase::StaticInitialize(GetUserConfig(), programName);
    Prepare();
    NYT::NProfiling::TSolomonExporterPtr SolomonExporter = NBSYeti::NProfiling::CreateSolomonExporter(GetNativeConfig().GetSolomonExporterConfig());
    SolomonExporter->Start();
    //  Prepare HTTP handlers
    if (GetNativeConfig().GetEnableDirectSolomonExporterHandler()) {
        NYTEx::NHttp::AddStandardHandlers(
            GetHttpHandlers(),
            GetCancelableContext(),
            GetOriginalConfig(),
            SolomonExporter
        );
    } else {
        NYTEx::NHttp::AddStandardHandlers(
            GetHttpHandlers(),
            GetCancelableContext(),
            GetOriginalConfig(),
            NBSYeti::NProfiling::GetSensorsGetter(SolomonExporter)
        );
    }
    // Start HTTP server
    const auto threads = 1;
    NYT::NHttp::TServerConfigPtr httpConfig = NYTEx::NHttp::CreateServerConfig(GetNativeConfig().GetHttpServerConfig());
    NYT::NHttp::IServerPtr HttpServer_ = NYT::NHttp::CreateServer(httpConfig, threads);
    HttpServer_->SetPathMatcher(GetHttpHandlers());
    HttpServer_->Start();

    auto env = TBigRtEnvironment::FromProtoConfig(GetNativeConfig(), GetThreadPool(), GetCancelableContext());
    env->Profiler = Internal_->Profiler;
    env->ProfilerTags = Internal_->ProfilerTags;
    env->ConsumingSystemTagFactory = Internal_->ConsumingSystemTagFactory;

    Start();
    auto dummyExecutor = ::MakeIntrusive<TExecutorStub>();
    auto pipeline = NPrivate::MakePipeline(dummyExecutor);
    pipelineConstructor(pipeline);

    auto executor = ::MakeIntrusive<TBigRtPipelineExecutor>(env);
    executor->Run(pipeline);
    Finish();
}

void TBigRtProgramBaseInternal::Run(std::function<void(TPipeline&, const TBigRtProgramProtoConfig&)> pipelineConstructor)
{
    Run(
        [this, pipelineConstructor=std::move(pipelineConstructor)] (TPipeline& pipeline) -> void {
            InternalInvokePipelineConstructor(pipelineConstructor, pipeline);
        }
    );
}

void TBigRtProgramBaseInternal::NormalizeInputs(TBigRtProgramProtoConfig& config)
{
    if (!config.GetEasyInputs().empty()) {
        for (auto&& [name, supplierConfig] : std::move(*config.MutableEasyInputs())) {
            auto addInput = [&] (TString name, NBigRT::TSupplierConfig supplierConfig) {
                Y_ENSURE(!config.GetInputs().contains(name), NYT::Format("Conflicting inputs with name %Qv", name));
                TRorenInputConfig inputConfig;
                *inputConfig.AddSuppliers() = std::move(supplierConfig);
                (*config.MutableInputs())[name] = std::move(inputConfig);
            };
            Y_ENSURE(!supplierConfig.HasAlias(), NYT::Format("Supplier alias set for easy input %Qv", name));
            supplierConfig.SetAlias(name);
            TRorenInputConfig inputConfig;
            if (!supplierConfig.HasLbSupplier() || !supplierConfig.GetLbSupplier().GetRorenOuterExpand()) {
                addInput(std::move(name), std::move(supplierConfig));
                continue;
            }
            for (auto expandedSupplierConfig : NBigRT::ExpandLbAutoDcs(supplierConfig)) {
                expandedSupplierConfig.MutableLbSupplier()->ClearRorenOuterExpand();
                addInput(expandedSupplierConfig.GetAlias(), std::move(expandedSupplierConfig));
            }
        }
        config.MutableEasyInputs()->clear();
    }

    for (const auto& [name, _]: config.GetInputs()) {
        if (!config.GetConsumers().contains(name)) {
            Y_ENSURE(config.HasMainYtCluster() && config.HasMainYtPath(), NYT::Format("No defaults provided to determine supplier config for input %v", name));
            (*config.MutableConsumers())[name] = {};
        }
    }

    if (config.HasConsumerDefaults()) {
        const auto& consumerDefaults = config.GetConsumerDefaults();
        Y_ENSURE(
            !consumerDefaults.GetConsumingSystemConfig().HasMainPath()
            && !consumerDefaults.GetConsumingSystemConfig().HasCluster(),
            "You shouldn't set defaults for consuming system path, use MainYtCluster and MainYtPath.");
        for (auto& [_, consumerConfig] : *config.MutableConsumers()) {
            if (config.GetAutoLaunchBalancerInternalMaster()) {
                Y_ENSURE(!consumerConfig.GetConsumingSystemConfig().HasMasterBalancing(), "You can't override consumer-local MasterBalancing settings when using AutoLaunchBalancerInternalMaster");
            }
            NRoren::TConsumerConfig mergedConfig;
            mergedConfig.CopyFrom(consumerDefaults);
            mergedConfig.MergeFrom(consumerConfig);
            consumerConfig = std::move(mergedConfig);
        }
    }
}

void TBigRtProgramBaseInternal::NormalizeMainYtParams(TBigRtProgramProtoConfig& config)
{
    if (config.HasMainYtCluster()) {
        const auto& clusterName = config.GetMainYtCluster();
        for (auto& [_, consumerConfig] : *config.MutableConsumers()) {
            auto& csConfig = *consumerConfig.MutableConsumingSystemConfig();
            if (!csConfig.HasCluster()) {
                csConfig.SetCluster(clusterName);
            }
        }
        if (!config.GetYtClientConfig().GetUnderlyingClientConfig().HasClusterName()) {
            config.MutableYtClientConfig()->MutableUnderlyingClientConfig()->SetClusterName(clusterName);
        }
    }

    if (config.HasMainYtPath()) {
        const auto& mainYtPath = config.GetMainYtPath();
        THashMap<TString, TString> normalizedNames;
        for (auto& [topicName, consumerConfig] : *config.MutableConsumers()) {
            auto& csConfig = *consumerConfig.MutableConsumingSystemConfig();
            if (!csConfig.HasMainPath()) {
                auto normalizedName = ToLowerUTF8(Slugify(topicName));
                Y_ENSURE(!normalizedNames.contains(normalizedName),
                    NYT::Format("Topic names must have unique lowered-slugified names: %v, %v -> %v", normalizedNames[normalizedName], topicName, normalizedName));
                normalizedNames.emplace(normalizedName, topicName);
                csConfig.SetMainPath(NYT::Format("%v/cs/%v", mainYtPath, normalizedName));
            }
            if (auto& masterBalancing = *csConfig.MutableMasterBalancing(); !masterBalancing.HasMasterPath()) {
                masterBalancing.SetMasterPath(NYT::Format("%v/balancer", mainYtPath));
            }
        }
    }
}

void TBigRtProgramBaseInternal::NormalizeTvmConfig(TBigRtProgramProtoConfig& config)
{
    if (!config.HasTvmConfig()) {
        return;
    }
    auto& dstServices = *config.MutableTvmConfig()->MutableDstServices();
    THashSet<TString> presentAliases;
    for (const auto& dstService : dstServices) {
        presentAliases.insert(dstService.GetAlias());
    }
    for (const auto& [alias, id] : TVector<std::pair<TString, ui32>>{
        {"logbroker_tvm_alias", 2001059}, // hardcoded in users sessions lb writer
        {"logbroker", 2001059},
        {"lbkx", 2001059},
        {"messenger", 2001059},
        {"logbroker-prestable", 2001147},
        {"lbkxt", 2001147},
    }) {
        if (presentAliases.contains(alias)) {
            continue;
        }
        NBSYeti::TTvmGlobalConfig::TDstService service;
        service.SetAlias(alias);
        service.SetId(id);
        dstServices.Add()->CopyFrom(service);
    }
}

void TBigRtProgramBaseInternal::NormalizeDestinations(TBigRtProgramProtoConfig& config, NYT::TCancelableContextPtr cancelableContext)
{
    auto ytClientsPool = NUserSessions::NRT::CreateTransactionKeeper(config.GetYtClientConfig());
    const auto& cluster = config.GetYtClientConfig().GetUnderlyingClientConfig().GetClusterName();
    auto stopToken = NBSYeti::TStopToken(cancelableContext);
    for (auto& [name, destinationConfig] : *config.MutableDestinations()) {
        ui64 shardsCount = 0;
        switch (destinationConfig.GetWriterCase()) {
            case TProcessingDestinationConfig::kSwiftWriter: {
                auto& swiftWriterConfig = *destinationConfig.MutableSwiftWriter()->MutableNativeConfig();
                if (0 == swiftWriterConfig.GetShardsCount()) {
                    swiftWriterConfig.SetShardsCount(NBigRT::RetryUntilStopped(
                        [&] () {
                            return NBigRT::TYtQueue(
                                swiftWriterConfig.GetQueuePath(),
                                ytClientsPool->GetClient(swiftWriterConfig.GetYtCluster())
                            ).GetShardCount();
                        },
                        stopToken,
                        NYT::Format("Error getting shards count of swift writer queue %v", swiftWriterConfig.GetQueuePath())));
                }
                shardsCount = swiftWriterConfig.GetShardsCount();
                break;
            }
            case TProcessingDestinationConfig::kQytWriter: {
                auto& qytWriterConfig = *destinationConfig.MutableQytWriter();
                if (0 == qytWriterConfig.GetShardsCount()) {
                    qytWriterConfig.SetShardsCount(NBigRT::RetryUntilStopped(
                        [&] () {
                            return NBigRT::TYtQueue(
                                qytWriterConfig.GetPath(),
                                ytClientsPool->GetClient(cluster)
                            ).GetShardCount();
                        },
                        stopToken,
                        NYT::Format("Error getting shards count for qyt %v", name)));
                }
                shardsCount = qytWriterConfig.GetShardsCount();
                break;
            }
            case TProcessingDestinationConfig::WRITER_NOT_SET: {
                break;
            }
        }
        if (0 == destinationConfig.GetShardsCount()) {
            destinationConfig.SetShardsCount(shardsCount);
        }
    }
}

void TBigRtProgramBaseInternal::NormalizeConfigBase(TBigRtProgramProtoConfig& config, NYT::TCancelableContextPtr cancelableContext)
{
    NormalizeInputs(config);
    NormalizeMainYtParams(config);
    NormalizeTvmConfig(config);
    NormalizeDestinations(config, cancelableContext);
}

void TBigRtProgramBaseInternal::NormalizeConfig(TBigRtProgramProtoConfig& config)
{
    NormalizeConfigBase(config, GetCancelableContext());
}

TBigRtProgramBaseInternal& TBigRtProgramBaseInternal::SetProfiler(NYT::NProfiling::TProfiler profiler)
{
    Internal_->Profiler = profiler;
    return *this;
}

TBigRtProgramBaseInternal& TBigRtProgramBaseInternal::SetProfilerTags(NYT::NProfiling::TTagSet tagSet)
{
    Internal_->ProfilerTags = tagSet;
    return *this;
}

TBigRtProgramBaseInternal& TBigRtProgramBaseInternal::SetConsumingSystemTagFactory(
    std::function<std::pair<TString, TString>(const TString&)> factory)
{
    Internal_->ConsumingSystemTagFactory = std::move(factory);
    return *this;
}

const NYT::NConcurrency::ITwoLevelFairShareThreadPoolPtr& TBigRtProgramBaseInternal::GetThreadPool()
{
    if (Internal_->ThreadPool == nullptr) {
        Internal_->ThreadPool = CreateThreadPool(GetNativeConfig());
    }
    return Internal_->ThreadPool;
}

NYT::NConcurrency::ITwoLevelFairShareThreadPoolPtr TBigRtProgramBaseInternal::CreateThreadPool(const TBigRtProgramProtoConfig& config)
{
    int threadCount = 0;
    THashMap<TString, double> poolWeights;
    if (config.HasParallelizationConfig()) {
        const auto& parallelizationConfig = config.GetParallelizationConfig();
        threadCount = parallelizationConfig.GetThreadCount();
        poolWeights = THashMap<TString, double>(parallelizationConfig.GetPoolWeights().begin(), parallelizationConfig.GetPoolWeights().end());
    }
    if (threadCount == 0) {
        if (config.GetUserThreadCount() != 0) {
            threadCount = config.GetUserThreadCount();
        } else if (auto cores = static_cast<int>(NBigRT::GetVCpuCores()); cores != 0) {
            threadCount = cores;
        } else {
            YT_LOG_WARNING("Can't detect threads count. Use 1 thread.");
            threadCount = 1;
        }
    }

    YT_LOG_INFO("Starting thread pool; ThreadCount: %v", threadCount);
    NYT::NConcurrency::TNewTwoLevelFairShareThreadPoolOptions options;
    options.PoolWeightProvider = NYT::New<NPrivate::TMappedPoolWeightProvider>(std::move(poolWeights));
    return NYT::NConcurrency::CreateNewTwoLevelFairShareThreadPool(
        threadCount,
        "2level-fairshare",
        std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate

