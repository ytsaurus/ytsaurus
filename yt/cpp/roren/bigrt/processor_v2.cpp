#include "processor_v2.h"

#include <ads/bsyeti/libs/backtrace/backtrace.h>
#include <bigrt/lib/utility/profiling/continuous_time_guard.h>
#include <bigrt/lib/utility/timers/timers.h>
#include "table_poller.h"
#include "execution_block.h"
#include "parse_graph.h"
#include "supplier.h"
#include "bigrt_executor.h"

USE_ROREN_LOGGER();

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

TSharedRorenProcessorState::TSharedRorenProcessorState(const NYT::TCancelableContextPtr& cancelableContext)
{
    // Constant for now can be made configurable at some point.
    constexpr int backgroundThreadCount = 1;

    YT_LOG_INFO("Starting bg-thread-pool; ThreadCount: %v", backgroundThreadCount);
    BackgroundThreadPool_ = NYT::NConcurrency::CreateThreadPool(backgroundThreadCount, "bg-thread-pool");
    BackgroundInvoker_ = cancelableContext->CreateInvoker(BackgroundThreadPool_->GetInvoker());
    TablePoller_ = ::MakeIntrusive<TTablePoller>(BackgroundInvoker_);
}

////////////////////////////////////////////////////////////////////////////////

TRorenProcessorV2::TRorenProcessorV2(
    TBase::TConstructionArgs args,
    std::shared_ptr<THashMap<ui64, std::shared_ptr<NPrivate::TTimers>>> timers,
    const TPipeline& pipeline,
    size_t executionBlockNumber,
    const NYT::NProfiling::TProfiler& profiler,
    TSharedRorenProcessorStatePtr sharedState,
    NYT::IInvokerPtr userInvoker,
    bool waitProcessStarted,
    std::optional<NBigRT::TThrottlerQuota> throttlerQuota)
    : TBase(args)
    , WaitProcessStarted_(waitProcessStarted)
    , SharedState_(std::move(sharedState))
    , Throttler_(throttlerQuota ? std::make_optional(NBigRT::TAmortizedThrottler(*throttlerQuota, ShardsCount)) : std::nullopt)
    , Metrics_{profiler.WithTags(ProfilerTags)}
    , VCpuMetrics_(MakeAtomicShared<TVCpuMetrics>())
{
    if (timers->contains(Shard)) {
        Timers_ = timers->at(Shard);
    }
    auto parsedPipeline = NPrivate::ParseBigRtPipeline(pipeline,
        [this] (const NPrivate::IExecutionBlockPtr executionBlock) {
            for (const auto& [callbackId, timerWrapper] : executionBlock->GetTimersCallbacks()) {
                Y_UNUSED(timerWrapper);
                TimersCallbacks_[callbackId] = executionBlock;
            }
        },
        VCpuMetrics_
    );
    auto myParsedBlock = parsedPipeline[executionBlockNumber];
    ExecutionBlock_ = myParsedBlock.ExecutionBlock;
    const auto& stateManagerFactoryFunctionList = myParsedBlock.CreateStateManagerFunctionList;

    UserInvoker_ = std::move(userInvoker);
    ExecutionBlock_->BindToInvoker(UserInvoker_);
    NPrivate::TStateManagerRegistryPtr stateManagerRegistry;
    if (!stateManagerFactoryFunctionList.empty()) {
        stateManagerRegistry = MakeIntrusive<NPrivate::TStateManagerRegistry>();
        for (const auto& [id, factory] : stateManagerFactoryFunctionList) {
            auto stateManager = factory(Shard, SensorsContext);
            stateManagerRegistry->Add(id, std::move(stateManager));
        }
    }

    RorenContextArgsTemplate_.Timers = Timers_;
    RorenContextArgsTemplate_.TransactionKeeper = TransactionKeeper;
    RorenContextArgsTemplate_.Shard = Shard;
    RorenContextArgsTemplate_.TablePoller = SharedState_->TablePoller_;
    RorenContextArgsTemplate_.Profiler = Metrics_.Profiler;
    RorenContextArgsTemplate_.MainCluster = Cluster;
    RorenContextArgsTemplate_.StateManagerRegistry = stateManagerRegistry;
    RorenContextArgsTemplate_.ThrottlerReport = std::make_shared<std::atomic<ui64>>(0);

    if (Timers_) {
        Timers_->ReInit();
    }
}

TRorenProcessorV2::~TRorenProcessorV2()
{
    static const auto& Logger = NBigRT::MainLogger;

    if (!EpochContext_ || !EpochContext_->ExecutionBlockFinished) {
        return;
    }

    try {
        auto executionBlockFinished = EpochContext_->ExecutionBlockFinished;

        executionBlockFinished.Cancel(NYT::TError("Stopping roren shard processor"));
        NYT::NConcurrency::WaitUntilSet(executionBlockFinished);
    } catch (...) {
        YT_LOG_FATAL("Unexpected exception: %v", NBSYeti::CurrentExceptionWithBacktrace());
    }
}

void TRorenProcessorV2::Process(TString dataSource, NBigRT::TMessageBatch messageBatch)
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
    if (dataSource == NPrivate::SYSTEM_TIMERS_SUPPLIER_NAME) {
        ProcessTimers(std::move(dataSource), std::move(messageBatch));
    } else {
        ProcessInput(std::move(dataSource), std::move(messageBatch));
    }
    ThrottleIfNecessary();
}

void TRorenProcessorV2::ThrottleIfNecessary()
{
    if (Throttler_) {
        auto stopToken = GetStopToken();
        auto& throttler = *Throttler_;
        throttler.ReportProcessed(RorenContextArgsTemplate_.ThrottlerReport->exchange(0));
        TDuration sleepTime = throttler.GetThrottleTime();
        if (!sleepTime) {
            return;
        }
        auto deadline = sleepTime.ToDeadLine();
        TDuration currentMaxTimeout = TDuration::Seconds(10);
        while (!stopToken.IsSet() && TInstant::Now() < deadline) {
            auto timeout = std::min(deadline - TInstant::Now(), currentMaxTimeout);
            if (timeout >= TDuration::Seconds(10)) {
                YT_LOG_WARNING("Throttling takes too much time: %v", timeout);
            }
            {
                auto timeGuard = NBigRT::TSimpleTimerGuard(Metrics_.ThrottleTotalTime);
                stopToken.Wait(timeout);
            }
            currentMaxTimeout = std::min(2 * currentMaxTimeout, TDuration::Seconds(3600));
        }
    }
}

void TRorenProcessorV2::ProcessInput(TString /*inputAlias*/, NBigRT::TMessageBatch messageBatch)
{
    NYT::TSharedRef data;
    {
        auto sharedMessageBatch = std::make_shared<NBigRT::TMessageBatch>(std::move(messageBatch));
        data = NYT::TSharedRef{
            sharedMessageBatch.get(), sizeof(NBigRT::TMessageBatch), NYT::MakeSharedRangeHolder(sharedMessageBatch)};
    }

    auto doStartedFuture = ExecutionBlock_->AsyncDo(data);
    if (WaitProcessStarted_) {
        GetStopToken().WaitAny(doStartedFuture);
    }
    ThrottleIfNecessary();
}

void TRorenProcessorV2::ProcessTimers(TString dataSource, NBigRT::TMessageBatch messageBatch)
{
    THashMap<TString, TVector<TTimer>> timersMap;
    Y_ABORT_UNLESS(dataSource == NPrivate::SYSTEM_TIMERS_SUPPLIER_NAME);
    for (auto& rawMessage : messageBatch.Messages) {
        NPrivate::TTimerProto message;
        Y_ABORT_UNLESS(message.ParseFromString(rawMessage.UnpackedData()));
        timersMap[message.GetKey().GetCallbackId()].emplace_back(std::move(message));
    }
    for (auto& [callbackId, readyTimers] : timersMap) {
        this->TimersCallbacks_.at(callbackId)->AsyncOnTimer(callbackId, std::move(readyTimers));
    }
}

NYT::TFuture<TRorenProcessorV2::TPrepareForAsyncWriteResult> TRorenProcessorV2::FinishEpoch(TCommitContext& commitContext)
{
    NBigRT::TContinuousTimeCounterGuard guard(WaitInfo.Process);
    std::vector<NBigRT::TWriterCallback> transactionWriterList;
    NPrivate::TTimers::TTimersHashMap timersUpdates;
    std::vector<NPrivate::TOnCommitCallback> onCommitCallbackList;

    if (EpochContext_) {
        ExecutionBlock_->AsyncFinish();
        NYT::NConcurrency::WaitFor(EpochContext_->ExecutionBlockFinished).ThrowOnError();
        ThrottleIfNecessary();
        EpochContext_->ExecutionBlockFinished.Reset();
        transactionWriterList = NPrivate::TBigRtExecutionContextOps::GetTransactionWriterList(EpochContext_->ExecutionContext);
        timersUpdates = NPrivate::TBigRtExecutionContextOps::GetTimersHashMap(EpochContext_->ExecutionContext);
        onCommitCallbackList = NPrivate::TBigRtExecutionContextOps::GetOnCommitCallbackList(EpochContext_->ExecutionContext);
    }

    EpochContext_.reset();

    return TStatelessShardProcessor::FinishEpoch(commitContext)
        .Apply(BIND(
            [this,
             transactionWriterList = std::move(transactionWriterList),
             timersUpdates = std::move(timersUpdates),
             onCommitCallbackList = std::move(onCommitCallbackList)
            ](TPrepareForAsyncWriteResult prepareResult) mutable {
                return TPrepareForAsyncWriteResult{
                    .AsyncWriter =
                        [this, asyncWriter = prepareResult.AsyncWriter,
                        timersUpdates = std::move(timersUpdates),
                        transactionWriterList = std::move(transactionWriterList)
                        ](NYT::NApi::ITransactionPtr tx, NYTEx::ITransactionContextPtr context) {
                            if (this->Timers_) {
                                this->Timers_->Commit(tx, timersUpdates);
                            }
                            for (const auto& writer : transactionWriterList) {
                                writer(tx, context);
                            }
                            if (asyncWriter) {
                                asyncWriter(tx, context);
                            }
                        },
                    .OnCommitCallback =
                        [this, onCommitCallback = prepareResult.OnCommitCallback,
                         onCommitCallbackList = std::move(onCommitCallbackList)](const TCommitContext& context) {
                            NPrivate::TProcessorCommitContext rorenContext = {
                                .Success = context.Success,
                                .TransactionCommitResult = context.TransactionCommitResult,
                            };

                            for (const auto& callback : onCommitCallbackList) {
                                callback(rorenContext);
                            }

                            if (context.Success && this->Timers_) {
                                this->Timers_->OnCommit();
                            }

                            if (onCommitCallback) {
                                onCommitCallback(context);
                            }
                        },
                };
            }));
}

void TRorenProcessorV2::RunUserComputation(
    const NRoren::TBigRtExecutorPoolPtr& pool,
    const IBigRtExecutionContextPtr& context, const NBigRT::TMessageBatch& messageBatch)
{
    auto executor = pool->AcquireExecutor();
    executor.Start(std::move(context));
    executor.Do(messageBatch);
    executor.Finish();
}

NBigRT::TConsumingSystem::TVCpu TRorenProcessorV2::GetEstimatedVCpuMetrics() const
{
    return VCpuMetrics_->Get();
}

////////////////////////////////////////////////////////////////////////////////

}
