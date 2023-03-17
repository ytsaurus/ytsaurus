#include "pipeline_executor.h"

#include "bigrt_execution_context.h"
#include "bigrt_executor.h"
#include "composite_bigrt_writer.h"
#include "stateful_impl/state_manager_registry.h"

#include <ads/bsyeti/libs/tvm_manager/tvm_manager.h>
#include <ads/bsyeti/libs/ytex/common/await.h>
#include <ads/bsyeti/libs/ytex/http/proto/config.pb.h>
#include <bigrt/lib/consuming_system/consuming_system.h>
#include <bigrt/lib/processing/shard_processor/stateless/processor.h>
#include <bigrt/lib/supplier/supplier.h>
#include <bigrt/lib/utility/inflight/inflight.h>
#include <bigrt/lib/utility/profiling/safe_stats_over_yt.h>
#include <library/cpp/logger/global/global.h>
#include <yt/cpp/roren/bigrt/parse_graph.h>
#include <yt/cpp/roren/bigrt/table_poller.h>
#include <quality/user_sessions/rt/lib/common/protos/yt_client_config.pb.h>
#include <quality/user_sessions/rt/lib/common/yt_client.h>
#include <quality/user_sessions/rt/lib/yt_profiling/solomon_server.h>

#include <yt/yt/core/concurrency/thread_pool.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

static int GetDefaultThreadCount()
{
    return 1;
}

////////////////////////////////////////////////////////////////////////////////

// The same (unix) process might run several TRorenProcessors for different shards.
// This class incapsulates state that is shared between such TRorenProcessors.
struct TSharedRorenProcessorState: public TThrRefBase
{
    NYT::NConcurrency::IThreadPoolPtr BackgroundThreadPool_;
    NYT::IInvokerPtr BackgroundInvoker_;
    ::TIntrusivePtr<TTablePoller> TablePoller_;

    explicit TSharedRorenProcessorState(const NYT::TCancelableContextPtr& cancelableContext)
    {
        // Constant for now can be made configurable at some point.
        constexpr int backgroundThreadCount = 1;

        INFO_LOG << "Starting bg-thread-pool; ThreadCount: " << backgroundThreadCount;
        BackgroundThreadPool_ = NYT::NConcurrency::CreateThreadPool(backgroundThreadCount, "bg-thread-pool");
        BackgroundInvoker_ = cancelableContext->CreateInvoker(BackgroundThreadPool_->GetInvoker());
        TablePoller_ = ::MakeIntrusive<TTablePoller>(BackgroundInvoker_);
    }
};
using TSharedRorenProcessorStatePtr = ::TIntrusivePtr<TSharedRorenProcessorState>;

////////////////////////////////////////////////////////////////////////////////

class TRorenProcessor: public NBigRT::TStatelessShardProcessor
{
private:
    using TBase = NBigRT::TStatelessShardProcessor;

    struct TEpochContext
    {
        std::deque<NYT::TFuture<void>> UserComputationFutureQueue;
    };

public:
    TRorenProcessor(
        TBase::TConstructionArgs args,
        NRoren::TBigRtExecutorPoolPtr resharderExecutorPool,
        const NYT::NProfiling::TProfiler& profiler, TSharedRorenProcessorStatePtr sharedState, int userThreadCount)
        : TBase(args), ResharderExecutorPool_(std::move(resharderExecutorPool)), SharedState_(std::move(sharedState))
    {
        if (userThreadCount == 0) {
            userThreadCount = GetDefaultThreadCount();
        }

        INFO_LOG << "Starting user-thread-pool; ThreadCount: " << userThreadCount;
        UserThreadPool_ = NYT::NConcurrency::CreateThreadPool(userThreadCount, "user-thread-pool");
        UserInvoker_ = UserThreadPool_->GetInvoker();

        RorenContextArgsTemplate_.TransactionKeeper = TransactionKeeper;
        RorenContextArgsTemplate_.Shard = Shard, RorenContextArgsTemplate_.TablePoller = SharedState_->TablePoller_;
        RorenContextArgsTemplate_.Profiler = profiler;
        RorenContextArgsTemplate_.MainCluster = Cluster;
    }

    void Process(TString /*dataSource*/, NBigRT::TMessageBatch dataMessage) override
    {
        auto contextArgs = RorenContextArgsTemplate_;
        contextArgs.Writer = NYT::StaticPointerCast<NPrivate::TCompositeBigRtWriter>(GetWriter());
        auto context = CreateBigRtExecutionContext(std::move(contextArgs));

        auto userComputationFuture =
            BIND([pool = ResharderExecutorPool_, context = context, dataMessage = std::move(dataMessage)] {
                RunUserComputation(pool, context, dataMessage);
            })
                .AsyncVia(UserInvoker_)
                .Run();

        auto stopToken = GetStopToken();
        stopToken.GetCancelableContext()->PropagateTo(userComputationFuture);

        EpochContext_.UserComputationFutureQueue.emplace_back(std::move(userComputationFuture));
    }

    NYT::TFuture<TPrepareForAsyncWriteResult> FinishEpoch(TCommitContext& commitContext) override final
    {
        while (!EpochContext_.UserComputationFutureQueue.empty()) {
            NYT::NConcurrency::WaitFor(EpochContext_.UserComputationFutureQueue.front()).ThrowOnError();
            EpochContext_.UserComputationFutureQueue.pop_front();
        }
        return TStatelessShardProcessor::FinishEpoch(commitContext);
    }

private:
    static void RunUserComputation(
        const NRoren::TBigRtExecutorPoolPtr& pool,
        const IBigRtExecutionContextPtr& context, const NBigRT::TMessageBatch& dataMessage)
    {
        auto executor = pool->AcquireExecutor();
        executor.Start(std::move(context));
        executor.Do(dataMessage);
        executor.Finish();
    }

private:
    NRoren::TBigRtExecutorPoolPtr ResharderExecutorPool_;
    NYT::NProfiling::TProfiler Profiler_;
    TEpochContext EpochContext_;

    NYT::NConcurrency::IThreadPoolPtr UserThreadPool_;
    NYT::IInvokerPtr UserInvoker_;

    TSharedRorenProcessorStatePtr SharedState_;
    TBigRtExecutionContextArgs RorenContextArgsTemplate_;
};

class TRorenProcessorV2: public NBigRT::TStatelessShardProcessor
{
private:
    using TBase = NBigRT::TStatelessShardProcessor;

    struct TEpochContext
    {
        IBigRtExecutionContextPtr ExecutionContext;
        NYT::TFuture<void> ExecutionBlockFinished;
    };

public:
    TRorenProcessorV2(
        TBase::TConstructionArgs args,
        NPrivate::IExecutionBlockPtr executionBlock,
        std::vector<std::pair<TString, NPrivate::TCreateBaseStateManagerFunction>> stateManagerFactoryFunctionList,
        const NYT::NProfiling::TProfiler& profiler, TSharedRorenProcessorStatePtr sharedState, int userThreadCount)
        : TBase(args), ExecutionBlock_(std::move(executionBlock)), SharedState_(std::move(sharedState))
    {
        if (userThreadCount == 0) {
            userThreadCount = GetDefaultThreadCount();
        }

        INFO_LOG << "Starting user-thread-pool; ThreadCount: " << userThreadCount;
        UserThreadPool_ = NYT::NConcurrency::CreateThreadPool(userThreadCount, "user-thread-pool");
        UserInvoker_ = UserThreadPool_->GetInvoker();
        ExecutionBlock_->BindToInvoker(UserInvoker_);
        NPrivate::TStateManagerRegistryPtr stateManagerRegistry;
        if (!stateManagerFactoryFunctionList.empty()) {
            stateManagerRegistry = MakeIntrusive<NPrivate::TStateManagerRegistry>();
            for (const auto& [id, factory] : stateManagerFactoryFunctionList) {
                auto stateManager = factory(Shard, SensorsContext);
                stateManagerRegistry->Add(id, std::move(stateManager));
            }
        }

        RorenContextArgsTemplate_.TransactionKeeper = TransactionKeeper;
        RorenContextArgsTemplate_.Shard = Shard, RorenContextArgsTemplate_.TablePoller = SharedState_->TablePoller_;
        RorenContextArgsTemplate_.Profiler = profiler;
        RorenContextArgsTemplate_.MainCluster = Cluster;
        RorenContextArgsTemplate_.StateManagerRegistry = stateManagerRegistry;
    }

    void Process(TString /*dataSource*/, NBigRT::TMessageBatch dataMessage) override
    {
        if (!EpochContext_) {
            auto contextArgs = RorenContextArgsTemplate_;
            contextArgs.Writer = NYT::StaticPointerCast<NPrivate::TCompositeBigRtWriter>(GetWriter());
            EpochContext_ = TEpochContext{};
            EpochContext_->ExecutionContext = CreateBigRtExecutionContext(std::move(contextArgs));

            EpochContext_->ExecutionBlockFinished = ExecutionBlock_->Start(EpochContext_->ExecutionContext);
            auto stopToken = GetStopToken();
            stopToken.GetCancelableContext()->PropagateTo(EpochContext_->ExecutionBlockFinished);
        }

        NYT::TSharedRef data;
        {
            auto sharedDataMessage = std::make_shared<NBigRT::TMessageBatch>(std::move(dataMessage));
            data = NYT::TSharedRef{
                sharedDataMessage.get(), sizeof(NBigRT::TMessageBatch), NYT::MakeSharedRangeHolder(sharedDataMessage)};
        }

        ExecutionBlock_->AsyncDo(data);
    }

    NYT::TFuture<TPrepareForAsyncWriteResult> FinishEpoch(TCommitContext& commitContext) override final
    {
        ExecutionBlock_->AsyncFinish();
        NYT::NConcurrency::WaitFor(EpochContext_->ExecutionBlockFinished).ThrowOnError();

        auto transactionWriterList = NPrivate::GetTransactionWriterList(EpochContext_->ExecutionContext);
        auto onCommitCallbackList = NPrivate::GetOnCommitCallbackList(EpochContext_->ExecutionContext);

        EpochContext_.reset();

        return TStatelessShardProcessor::FinishEpoch(commitContext)
            .Apply(BIND(
                [transactionWriterList = std::move(transactionWriterList),
                 onCommitCallbackList = std::move(onCommitCallbackList)](TPrepareForAsyncWriteResult prepareResult) {
                    return TPrepareForAsyncWriteResult{
                        .AsyncWriter =
                            [asyncWriter = prepareResult.AsyncWriter,
                             transactionWriterList = std::move(transactionWriterList)](
                                NYT::NApi::ITransactionPtr tx, NYTEx::ITransactionContextPtr context) {
                                for (const auto& writer : transactionWriterList) {
                                    writer(tx, context);
                                }
                                if (asyncWriter) {
                                    asyncWriter(tx, context);
                                }
                            },
                        .OnCommitCallback =
                            [onCommitCallback = prepareResult.OnCommitCallback,
                             onCommitCallbackList = std::move(onCommitCallbackList)](const TCommitContext& context) {
                                NPrivate::TProcessorCommitContext rorenContext = {
                                    .Success = context.Success,
                                    .TransactionCommitResult = context.TransactionCommitResult,
                                };

                                for (const auto& callback : onCommitCallbackList) {
                                    callback(rorenContext);
                                }

                                if (onCommitCallback) {
                                    onCommitCallback(context);
                                }
                            },
                    };
                }));
    }

private:
    static void RunUserComputation(
        const NRoren::TBigRtExecutorPoolPtr& pool,
        const IBigRtExecutionContextPtr& context, const NBigRT::TMessageBatch& dataMessage)
    {
        auto executor = pool->AcquireExecutor();
        executor.Start(std::move(context));
        executor.Do(dataMessage);
        executor.Finish();
    }

private:
    NPrivate::IExecutionBlockPtr ExecutionBlock_;
    std::optional<TEpochContext> EpochContext_;

    NYT::NConcurrency::IThreadPoolPtr UserThreadPool_;
    NYT::IInvokerPtr UserInvoker_;

    TSharedRorenProcessorStatePtr SharedState_;
    TBigRtExecutionContextArgs RorenContextArgsTemplate_;
};

////////////////////////////////////////////////////////////////////////////////

static THashMap<TString, TConsumerConfig> ExtractConsumers(TVector<TConsumerConfig> configs)
{
    Y_VERIFY(!configs.empty(), "Must be at least one consumer");
    THashMap<TString, TConsumerConfig> consumers;
    for (auto& i : configs) {
        const auto& tag = i.GetInputTag();
        Y_VERIFY(!consumers.contains(tag), "Conflicting consumer input tag %s", tag.c_str());
        consumers[tag] = i;
    }
    return consumers;
}

static THashMap<TString, TVector<NBigRT::TSupplierConfig>> ExtractInputs(const TBigRtProgramConfig& config)
{
    THashMap<TString, TVector<NBigRT::TSupplierConfig>> suppliers;
    if (!config.GetEasyInputs().empty()) {
        // Extract inputs from EasyInputs, tag = supplier.GetAlias()
        Y_VERIFY(config.GetInputs().empty(), "EasyInputs and Inputs are alternatives");
        for (const auto& input : config.GetEasyInputs()) {
            const auto& tag = input.GetAlias();
            Y_VERIFY(!suppliers.contains(tag), "Conflicting supplier input tag %s", tag.c_str());
            suppliers[tag] = {input};
        }
        return suppliers;
    }
    // Extract inputs from Inputs
    Y_VERIFY(!config.GetInputs().empty(), "EasyInputs and Inputs are alternatives, one of them must be not empty");
    for (const auto& input : config.GetInputs()) {
        const auto& tag = input.GetInputTag();
        Y_VERIFY(!suppliers.contains(tag), "Conflicting supplier input tag %s", tag.c_str());
        suppliers[tag] = TVector<NBigRT::TSupplierConfig>(input.GetSuppliers().begin(), input.GetSuppliers().end());
    }
    return suppliers;
}

////////////////////////////////////////////////////////////////////////////////

TBigRtPipelineExecutor::TBigRtPipelineExecutor(
    const TBigRtProgramConfig& config,
    const NYT::NProfiling::TProfiler& profiler,
    const NYT::NProfiling::TTagSet& profilerTags, const NYT::TCancelableContextPtr& cancelableContext)
    : Config_(config), Profiler_(profiler), ProfilerTags_(profilerTags), CancelableContext_(cancelableContext)
{}

void TBigRtPipelineExecutor::Run(const TPipeline& pipeline)
{
    auto consumers =
        ExtractConsumers(TVector<TConsumerConfig>(Config_.GetConsumers().begin(), Config_.GetConsumers().end()));
    auto inputs = ExtractInputs(Config_);
    // Checking that every consumer has some input and vice versa.
    THashSet<TString> usedTags;
    for (const auto& [tag, suppliers] : inputs) {
        Y_VERIFY(consumers.contains(tag), "No consumer is linked to supplier with alias: %s", tag.c_str());
        usedTags.insert(tag);
    }
    for (const auto& [tag, config] : consumers) {
        Y_VERIFY(usedTags.contains(tag), "Unused input tag %s", tag.c_str());
        Y_VERIFY(usedTags.contains(tag), "Consumer %s refers to unknown input %s",
            config.GetConsumingSystemConfig().GetMainPath().c_str(),
            tag.c_str());
    }

    struct TProcessorArgs
    {
        TString InputTag;
        std::vector<NPrivate::TRegisterWriterFunction> RegisterWriterFunctionList;
        std::variant<TBigRtExecutorPoolPtr, NPrivate::IExecutionBlockPtr> UserCodeExecutor;
        std::vector<std::pair<TString, NPrivate::TCreateBaseStateManagerFunction>> StateManagerFactoryFunctionList;
    };

    std::vector<TProcessorArgs> processorArgsList;

    // TODO: once V2GraphParsing is stable we should remove old parsing.
    if (Config_.GetEnableV2GraphParsing()) {
        auto parseResultList = NPrivate::ParseBigRtPipeline(pipeline);

        for (const auto& parseResult : parseResultList) {
            auto& args = processorArgsList.emplace_back();
            args.RegisterWriterFunctionList = parseResult.RegisterWriterFunctionList;
            args.InputTag = parseResult.InputTag;
            args.UserCodeExecutor = parseResult.ExecutionBlock;
            args.StateManagerFactoryFunctionList = parseResult.CreateStateManagerFunctionList;
        }
    } else {
        auto parsedPipeline = NRoren::NPrivate::ParseBigRtResharder(pipeline);
        Y_VERIFY(parsedPipeline.size() > 0);
        for (auto& pipelineDescription : parsedPipeline) {
            auto& args = processorArgsList.emplace_back();

            args.InputTag = pipelineDescription.InputTag;
            args.RegisterWriterFunctionList = pipelineDescription.WriterRegistratorList;
            args.UserCodeExecutor = NRoren::MakeExecutorPool(pipelineDescription);
        }
    }

    NBSYeti::TTvmManagerPtr tvmManager;

    if (Config_.HasTvmConfig()) {
        tvmManager = NBSYeti::CreateTvmManager(Config_.GetTvmConfig());
    }

    auto ytClientsPool = NUserSessions::NRT::CreateTransactionKeeper(Config_.GetYtClientConfig());

    auto inflightLimiter = NBigRT::CreateProfiledInflightLimiter(ProfilerTags_, Config_.GetMaxInFlightBytes());

    auto sharedState = MakeIntrusive<TSharedRorenProcessorState>(CancelableContext_);

    TVector<NBigRT::TConsumingSystemPtr> consSystems;

    for (auto& args : processorArgsList) {
        const auto& inputTag = args.InputTag;
        Y_VERIFY(inputs.contains(inputTag), "Unknown input tag %s", inputTag.c_str());
        const auto& supplierConfigs = inputs[inputTag];
        const auto& consSystemConfig = consumers[inputTag];

        auto writerFactory = NYT::New<NPrivate::TCompositeBigRtWriterFactory>();
        for (const auto& registrator : args.RegisterWriterFunctionList) {
            registrator(*writerFactory, tvmManager);
        }

        auto consSystem = NBigRT::CreateConsumingSystem({
            .Config = consSystemConfig.GetConsumingSystemConfig(),
            .SuppliersProvider = NBigRT::CreateSupplierFactoriesProvider({
                .Configs = supplierConfigs,
                .TvmManager = tvmManager,
            }),
            .YtClients = ytClientsPool,
            .ShardsProcessor =
                [ytClientsPool,
                 inflightLimiter,
                 sharedState,
                 userCodeExecutor = args.UserCodeExecutor,
                 stateManagerFactoryFunctionList = args.StateManagerFactoryFunctionList,
                 writerFactory,
                 config = &Config_, consSystemConfig, profiler = Profiler_, profilerTags = ProfilerTags_](
                    NBigRT::TConsumingSystem::IConsumer& consumer) {
                    auto processor = std::visit(
                        [&](const auto& userCodeExecutor) -> std::unique_ptr<NBigRT::TStatelessShardProcessor> {
                            using T = std::decay_t<decltype(userCodeExecutor)>;
                            if constexpr (std::is_same_v<T, TBigRtExecutorPoolPtr>) {
                                return std::make_unique<TRorenProcessor>(
                                    TRorenProcessor::TConstructionArgs{
                                        .Consumer = consumer,
                                        .Config = consSystemConfig.GetStatelessShardProcessorConfig(),
                                        .TransactionKeeper = ytClientsPool,
                                        .InflightLimiter = inflightLimiter,
                                        .ProfilerTags = profilerTags,
                                        .WriterFactory = writerFactory,
                                    }, userCodeExecutor, profiler, sharedState, config->GetUserThreadCount());
                            } else if constexpr (std::is_same_v<T, NPrivate::IExecutionBlockPtr>) {
                                return std::make_unique<TRorenProcessorV2>(
                                    TRorenProcessor::TConstructionArgs{
                                        .Consumer = consumer,
                                        .Config = consSystemConfig.GetStatelessShardProcessorConfig(),
                                        .TransactionKeeper = ytClientsPool,
                                        .InflightLimiter = inflightLimiter,
                                        .ProfilerTags = profilerTags,
                                        .WriterFactory = writerFactory,
                                    },
                                    userCodeExecutor->Clone(),
                                    std::move(stateManagerFactoryFunctionList),
                                    profiler, sharedState, config->GetUserThreadCount());
                            }
                        }, userCodeExecutor);
                    processor->Run();
                },
            .ProfilerTags = ProfilerTags_,
        });

        consSystem->Run();
        consSystems.push_back(std::move(consSystem));
    }

    NYTEx::WaitFor(CancelableContext_);
    INFO_LOG << "Stopping consuming system\n";
    for (const auto& i : consSystems) {
        NYT::NConcurrency::WaitUntilSet(i->Stop());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
