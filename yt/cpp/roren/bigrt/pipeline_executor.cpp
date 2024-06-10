#include "pipeline_executor.h"

#include "bigrt_executor.h"
#include "composite_bigrt_writer.h"
#include "destination_writer.h"
#include "parse_graph_v3.h"
#include "pool_weight_provider.h"
#include "processor_v2.h"
#include "processor_v3.h"
#include "stateful_impl/state_manager_registry.h"
#include "supplier.h"

#include <ads/bsyeti/libs/backtrace/backtrace.h>
#include <ads/bsyeti/libs/ytex/common/await.h>
#include <bigrt/lib/consuming_system/consuming_system.h>
#include <bigrt/lib/processing/shard_processor/fallback/processor.h>
#include <bigrt/lib/processing/shard_processor/stateless/processor.h>
#include <bigrt/lib/writer/swift/factory.h>
#include <bigrt/lib/writer/yt_queue/factory.h>
#include <bigrt/lib/utility/inflight/inflight.h>
#include <bigrt/lib/utility/logging/logging.h>  // MainLogger
#include <bigrt/lib/utility/throttler/throttler.h>
#include <yt/cpp/roren/bigrt/config/config.h>
#include <yt/cpp/roren/bigrt/parse_graph.h>
#include <yt/cpp/roren/bigrt/table_poller.h>
#include <yt/cpp/roren/library/logger/logger.h>
#include <yt/yt/core/concurrency/two_level_fair_share_thread_pool.h>

#include <deque>

USE_ROREN_LOGGER();

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

bool HasTimers(const NPrivate::IExecutionBlockPtr& firstBlock)
{
    std::deque<NPrivate::IExecutionBlockPtr> blocks;
    blocks.push_back(firstBlock);
    while (!blocks.empty()) {
        const auto current = blocks.front();
        if (!current->GetTimersCallbacks().empty()) {
            return true;
        }
        blocks.insert(blocks.end(), current->GetOutputBlocks().begin(), current->GetOutputBlocks().end());
        blocks.pop_front();
    }
    return false;
};

NBigRT::TSupplierFactoriesProvider GetSupplierFactoryWithTimers(std::shared_ptr<THashMap<ui64, std::shared_ptr<NPrivate::TTimers>>>& timersMap, const TString& csMainPath, const NBigRT::TSupplierFactoriesData& supplierFactoriesData)
{
    return NPrivate::CreateExSupplierFactoriesProvider(
        supplierFactoriesData,
        [timersMap, csMainPath](const ui64 shardId, const NBigRT::TSupplierFactoryContext& context) -> NPrivate::TTimers& {
            auto it = timersMap->find(shardId);
            if (it == timersMap->end()) {
                auto result = timersMap->emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(shardId),
                    std::forward_as_tuple(
                        std::make_shared<NPrivate::TTimers>(
                            context.YtClients->GetClient(context.DefaultMainCluster),
                            csMainPath,
                            shardId,
                            [shardId](const TString&) { return shardId; }
                        )
                    )
                );
                it = result.first;
            }
            return *it->second;
        }
    );
}

////////////////////////////////////////////////////////////////////////////////

TBigRtPipelineExecutor::TBigRtPipelineExecutor(TBigRtEnvironmentPtr env)
    : Env_(std::move(env))
{}

void TBigRtPipelineExecutor::Start(const TPipeline& pipeline)
{
    auto parsedPipeline = std::make_shared<NPrivate::TParsedPipelineV3>(pipeline, Env_->Config->UseProcessorV3);
    YT_LOG_INFO("Graph Dump (DOT):\n%v", parsedPipeline->GetDebugDescription());

    auto parseResultList = NPrivate::ParseBigRtPipeline(pipeline, [] (const NPrivate::IExecutionBlockPtr&) { }, {});

    auto getPoolName = [] (const TString& inputTag) {
        return "input-" + inputTag;
    };
    auto getFairInvoker = [getPoolName, threadPool=Env_->ThreadPool] (const TString& inputTag, NBigRT::TConsumingSystem::IConsumer& consumer) {
        const TString bucketName = "shard-" + ToString(consumer.GetShard());
        return threadPool->GetInvoker(getPoolName(inputTag), bucketName);
    };

    THashMap<TString, NYTEx::ITransactionKeeperPtr> transactionKeepersPool;
    auto getTransactionKeeper = [&](const TComputationConfig& computation) {
        auto& transactionKeeper = transactionKeepersPool[computation.TransactionKeeperGroup];
        if (nullptr == transactionKeeper) {
            transactionKeeper = NYTEx::CreateTransactionKeeper(Env_->YtClients, Env_->Config->SharedTransactionPeriod);
        }
        return transactionKeeper;
    };

    auto inflightLimiter = NBigRT::CreateProfiledInflightLimiter(Env_->ProfilerTags, Env_->Config->MaxInFlightBytes);

    auto sharedState = MakeIntrusive<TSharedRorenProcessorState>(Env_->CancelableContext);

    auto stopToken = NBSYeti::TStopToken(Env_->CancelableContext);
    THashMap<TString, NBigRT::ISwiftQueueWriterFactoryPtr> swiftWriterFactories;
    THashMap<TString, NBigRT::IYtQueueWriterFactoryPtr> qytWriterFactories;
    for (const auto& [tag, config] : Env_->Config->Destinations) {
        if (config->SwiftWriter.has_value()) {
            swiftWriterFactories.emplace(tag, NBigRT::CreateSwiftQueueWriterFactory(
                *config->SwiftWriter,
                Env_->YtClients,
                Env_->ThreadPool->GetInvoker(NPrivate::POOL_SWIFT, tag),
                tag,
                stopToken,
                Env_->ProfilerTags.WithTag({"destination", tag})));
        } else if (config->YtQueueWriter.has_value()) {
            qytWriterFactories.emplace(tag, NBigRT::CreateYtQueueWriterFactory(*config->YtQueueWriter));
        } else {
            THROW_ERROR_EXCEPTION("Writer is not specified for destination %v", tag);
        }
    }

    auto writerFactory = NYT::New<NPrivate::TCompositeBigRtWriterFactory>();
    for (const auto& registrator : parsedPipeline->GetWriterRegistrators()) {
        registrator(*writerFactory, Env_->TvmManager);
    }
    for (const auto& [tag, factory] : swiftWriterFactories) {
        writerFactory->Add(TTypeTag<NPrivate::IDestinationWriter>{tag},
            NPrivate::CreateDestinationWriterFactory(factory));
    }
    for (const auto& [tag, factory] : qytWriterFactories) {
        writerFactory->Add(TTypeTag<NPrivate::IDestinationWriter>{tag},
            NPrivate::CreateDestinationWriterFactory(factory));
    }

    THashSet<TString> launchedBalancerInternalMasters;

    for (size_t i = 0; i < parseResultList.size(); ++i) {
        auto& executionBlock = parseResultList[i];
        const auto& inputTag = executionBlock.InputTag;
        THROW_ERROR_EXCEPTION_UNLESS(Env_->Config->Computations.contains(inputTag), "Unknown input tag %v", inputTag);
        auto& computationConfig = *Env_->Config->Computations.at(inputTag);
        auto consumingSystemConfig = computationConfig.ConsumingSystemConfig;
        if (Env_->Config->AutoLaunchBalancerInternalMaster) {
            auto& balancerConfig = *consumingSystemConfig.MutableMasterBalancing();
            THROW_ERROR_EXCEPTION_UNLESS(!balancerConfig.GetLaunchInternalMaster() || !balancerConfig.HasLaunchInternalMaster(), "You can't set LaunchInternalMaster when using AutoLaunchBalancerInternalMaster");
            if (launchedBalancerInternalMasters.insert(balancerConfig.GetMasterPath()).second) {
                balancerConfig.SetLaunchInternalMaster(true);
                YT_LOG_DEBUG("Launching balancer internal master on %v", inputTag);
            } else {
                YT_LOG_DEBUG("Skip launching balancer internal master on %v", inputTag);
            }
        }
        auto csProfilerTags = Env_->ProfilerTags.WithTag(Env_->ConsumingSystemTagFactory(inputTag));

        std::shared_ptr<THashMap<ui64, std::shared_ptr<NPrivate::TTimers>>> timersMap = std::make_shared<THashMap<ui64, std::shared_ptr<NPrivate::TTimers>>>();

        NBigRT::TSupplierFactoriesData supplierFactoriesData{
            .Configs = [&] () {
                TVector<NBigRT::TSupplierConfig> supplierConfigs;
                for (const auto& [alias, config] : computationConfig.Inputs) {
                    supplierConfigs.push_back(config);
                    if (alias.empty()) {
                        supplierConfigs.back().SetAlias(inputTag);
                    } else {
                        supplierConfigs.back().SetAlias(alias);
                    }
                }
                return supplierConfigs;
            }(),
            .Context = {
                .GrutClientProvider = Env_->GrutClientProvider,
            },
            .TvmManager = Env_->TvmManager,
            .LbPqLib = Env_->PQLib,
        };
        if (Env_->Config->UseProcessorV3 || !Env_->Config->LegacyThreadCount.has_value()) {
            supplierFactoriesData.HeavyInvoker = Env_->ThreadPool->GetInvoker(getPoolName(inputTag), "supplier");
        }

        NBigRT::TSupplierFactoriesProvider supplierFactory = HasTimers(executionBlock.ExecutionBlock)
            ? GetSupplierFactoryWithTimers(timersMap, consumingSystemConfig.GetMainPath(), supplierFactoriesData)
            : NBigRT::CreateSupplierFactoriesProvider(supplierFactoriesData);

        std::optional<NBigRT::TThrottlerQuota> throttlerQuota = computationConfig.ThrottlerQuota;

        auto makeStatelessProcessorConstructionArgs = [
                =,
                config=computationConfig.StatelessShardProcessorConfig,
                transactionKeeper=getTransactionKeeper(computationConfig)
                                                      ] (NBigRT::TConsumingSystem::IConsumer& consumer) {
            return NBigRT::TStatelessShardProcessor::TConstructionArgs{
                .Consumer = consumer,
                .Config = config,
                .TransactionKeeper = transactionKeeper,
                .InflightLimiter = inflightLimiter,
                .Place = inputTag,
                .ProfilerTags = csProfilerTags,
                .WriterFactory = writerFactory,
            };
        };
        NBigRT::TConsumingSystem::TShardsProcessor shardProcessorV2 =
            [
                config=Env_->Config,
                makeStatelessProcessorConstructionArgs,
                inputTag,
                timersMap,
                pipeline,
                i,
                profiler = Env_->Profiler,
                sharedState,
                getFairInvoker,
                throttlerQuota
            ] (NBigRT::TConsumingSystem::IConsumer& consumer) {
                YT_LOG_INFO("Starting roren processor; InputTag=%v", inputTag);
                NYT::NConcurrency::IThreadPoolPtr localThreadPool;
                NYT::IInvokerPtr invoker;
                if (!config->LegacyThreadCount.has_value()) {
                    invoker = getFairInvoker(inputTag, consumer);
                } else {
                    const ui64 threadCount = Max(1UL, *config->LegacyThreadCount);
                    localThreadPool = NYT::NConcurrency::CreateThreadPool(threadCount, "roren-threadpool");
                    invoker = localThreadPool->GetInvoker();
                }
                bool waitProcessStarted = config->EnableWaitProcessStarted;
                auto processor = std::make_unique<TRorenProcessorV2>(
                    makeStatelessProcessorConstructionArgs(consumer),
                    timersMap,
                    pipeline,
                    i,
                    profiler,
                    sharedState,
                    invoker,
                    waitProcessStarted,
                    throttlerQuota);
                processor->Run();
            };

        const int fibers = Env_->ThreadPool->GetThreadCount();  //TODO: multiplier from config
        NBigRT::TConsumingSystem::TShardsProcessor shardProcessorV3 =
            [
                makeStatelessProcessorConstructionArgs,
                fibers,
                inputTag,
                timersMap,
                getFairInvoker,
                parsedPipeline,
                profiler = Env_->Profiler,
                sharedState,
                throttlerQuota
            ] (NBigRT::TConsumingSystem::IConsumer& consumer) {
                YT_LOG_INFO("Starting roren processor; InputTag=%v", inputTag);
                auto invoker = getFairInvoker(inputTag, consumer);
                auto processor = std::make_unique<NPrivate::TBigRtProcessorV3>(
                    makeStatelessProcessorConstructionArgs(consumer),
                    fibers,
                    invoker,
                    timersMap->contains(consumer.GetShard()) ? timersMap->at(consumer.GetShard()) : nullptr,
                    *parsedPipeline,
                    inputTag,
                    profiler,
                    sharedState,
                    throttlerQuota);
                processor->Run();
            };

        auto consSystemData = NBigRT::TConsumingSystemData{
            .Config = std::move(consumingSystemConfig),
            .SuppliersProvider = supplierFactory,
            .YtClients = Env_->YtClients,
            .ShardsProcessor = Env_->Config->UseProcessorV3 ? shardProcessorV3 : shardProcessorV2,
            .ProfilerTags = csProfilerTags,
        };
        if (computationConfig.FallbackShardProcessorConfig.has_value()) {
            consSystemData.FallbackShardsProcessor =
                [
                    config = *computationConfig.FallbackShardProcessorConfig,
                    transactionKeeper=getTransactionKeeper(computationConfig),
                    inflightLimiter
                ] (NBigRT::TConsumingSystem::IConsumer& consumer) {
                    NBigRT::TFallbackShardProcessor{
                        NBigRT::TStatelessShardProcessor::TConstructionArgs{
                            consumer,
                            config.GetStatelessShardProcessorConfig(),
                            transactionKeeper,
                            inflightLimiter,
                        },
                        config.GetFallbackProcessingConfig(),
                    }.Run();
                };
        }
        auto consSystem = NBigRT::CreateConsumingSystem(std::move(consSystemData));

        consSystem->Run();
        ConsSystems_.push_back(std::move(consSystem));
    }
}

void TBigRtPipelineExecutor::Finish()
{
    NYTEx::WaitFor(Env_->CancelableContext);
    YT_LOG_INFO("Stopping consuming system");
    for (const auto& i : ConsSystems_) {
        NYT::NConcurrency::WaitUntilSet(i->Stop());
    }
}

void TBigRtPipelineExecutor::Run(const TPipeline& pipeline)
{
    Start(pipeline);
    Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
