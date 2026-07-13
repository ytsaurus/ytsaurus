#include "computation_base.h"

#include "computation_tracer.h"
#include "event_timestamp_assigner.h"
#include "meta_setter.h"
#include "stores/compact_output_store.h"
#include "stores/input_store.h"
#include "stores/output_store.h"
#include "stores/timer_store.h"

#include "job_state/state_manager.h"

#include <yt/yt/flow/library/cpp/common/lag.h>

#include <yt/yt/flow/library/cpp/connectors/common/ordered_source.h>

#include <yt/yt/flow/library/cpp/distributed_throttler/factory.h>

#include <yt/yt/flow/library/cpp/misc/retryable_client.h>
#include <yt/yt/flow/library/cpp/misc/retryable_transaction.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/flow/library/cpp/tables/compact_output_messages.h>
#include <yt/yt/flow/library/cpp/tables/compact_partition_output_messages.h>
#include <yt/yt/flow/library/cpp/tables/context.h>
#include <yt/yt/flow/library/cpp/tables/input_messages.h>
#include <yt/yt/flow/library/cpp/tables/timers.h>
#include <yt/yt/flow/library/cpp/tables/transaction_manager.h>

#include <yt/yt/flow/library/cpp/common/external_state_manager.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/inflight_tracker.h>
#include <yt/yt/flow/library/cpp/common/input_context.h>
#include <yt/yt/flow/library/cpp/common/message_batcher.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/sink.h>
#include <yt/yt/flow/library/cpp/common/source.h>
#include <yt/yt/flow/library/cpp/common/state_cache.h>
#include <yt/yt/flow/library/cpp/common/time_provider.h>

#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>


#include <yt/yt/flow/library/cpp/tables/context.h>
#include <yt/yt/flow/library/cpp/tables/key_states.h>
#include <yt/yt/flow/library/cpp/tables/key_visitor_states.h>
#include <yt/yt/flow/library/cpp/tables/partition_states.h>

#include <yt/yt/flow/lib/client/public.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/topological_ordering.h>

#include <util/string/join.h>

namespace NYT::NFlow {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

TDynamicSourceSpecPtr GetPatchedDynamicSourceSpec(const TDynamicComputationSpecPtr& dynamicSpec, const TStreamId& streamId)
{
    auto patchedDynamicSourceSpec = CloneYsonStruct(GetOrDefault(dynamicSpec->SourceStreams, streamId, New<TDynamicSourceSpec>()));
    patchedDynamicSourceSpec->Draining = dynamicSpec->Draining;
    return patchedDynamicSourceSpec;
}

// Convert the flow-side strong-typedef throttler map into the factory's
// module-local std::string-keyed map.
THashMap<NDistributedThrottler::TThrottlerId, TDynamicThrottlerSpecPtr> ToFactoryThrottlers(
    const THashMap<TThrottlerId, TDynamicThrottlerSpecPtr>& throttlers)
{
    THashMap<NDistributedThrottler::TThrottlerId, TDynamicThrottlerSpecPtr> result;
    result.reserve(throttlers.size());
    for (const auto& [id, spec] : throttlers) {
        result.emplace(id.Underlying(), spec);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TComputationBase::TComputationBase(
    TComputationContextPtr context,
    TDynamicComputationContextPtr dynamicContext)
    : Logger(context->Logger)
    , Context_(std::move(context))
    , Parameters_(DynamicPointerCast<IComputation::TParameters>(
        TRegistry::Get()->ParseComputationParameters(Context_->ComputationSpec)))
    , TopologicalStreamOrder_(BuildTopologicalStreamOrder(Context_->ComputationSpec))
    , RetryableClient_(CreateRetryableClient(Context_->GetClient(), Context_->SerializedInvoker, Context_->StatusProfiler->WithPrefix("/retryable_client"), Logger))
    , TimeProvider_(CreateTimeProvider(RetryableClient_->GetTimestampProvider(), Context_->ClockClusterTag))
    , TransactionManager_(CreateTransactionManager())
    , DynamicContext_(std::move(dynamicContext))
    , DynamicParameters_(DynamicPointerCast<IComputation::TDynamicParameters>(
        TRegistry::Get()->ParseDynamicComputationParameters(Context_->ComputationSpec, DynamicContext_->DynamicComputationSpec)))
    , DynamicPartitionSpec_(DynamicPointerCast<IComputation::TDynamicPartitionSpec>(
        TRegistry::Get()->ParseDynamicComputationPartitionSpec(Context_->ComputationSpec, DynamicContext_->DynamicPartitionSpec)))
    , ThrottlerFactory_(NDistributedThrottler::CreateDistributedThrottlerFactory(
        Context_->DistributedThrottlerControllerChannelProvider,
        ToString(Context_->Job->JobId),
        ToFactoryThrottlers(DynamicContext_->Throttlers),
        Context_->StatusProfiler->WithPrefix("/throttlers"),
        Logger.WithTag("ThrottlerFactory"),
        Context_->Profiler.WithPrefix("/throttlers")))
{
    YT_VERIFY(Parameters_);
    YT_VERIFY(DynamicParameters_);
    YT_VERIFY(DynamicPartitionSpec_);
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);

    // TODO(mikari): move dynamic spec to ctor.
    RetryableClient_->Reconfigure(GetDynamicSpec()->RetryableRequest);
    TransactionManager_->Reconfigure(GetDynamicSpec()->RetryableRequest);

    // Subscribe to reconfigure to update CurrentDynamic* fields and call ReconfigureCallbacks_.
    SubscribeOnReconfigure(BIND(
        [this] () {
            // RetryableClient and TransactionManager reconfiguration.
            RetryableClient_->Reconfigure(GetDynamicSpec()->RetryableRequest);
            TransactionManager_->Reconfigure(GetDynamicSpec()->RetryableRequest);
            // Cheap when specs haven't changed — the factory diffs them.
            ThrottlerFactory_->Reconfigure(ToFactoryThrottlers(GetDynamicContext()->Throttlers));
        }),
        EWatchReconfigure::DynamicComputationSpec);
}

void TComputationBase::Reconfigure(TDynamicComputationContextPtr dynamicContext)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    PendingDynamicContext_ = std::move(dynamicContext);
}

void TComputationBase::UpdateWatermarkState(TWatermarkStatePtr watermarkState)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    PendingWatermarkState_ = std::move(watermarkState);
}

void TComputationBase::SetInputTraverse(THashMap<TStreamId, TStreamTraverseDataPtr> inputStreams)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    PendingInputStreams_.Store(std::optional(std::move(inputStreams)));
}

const TComputationContextPtr& TComputationBase::GetContext() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();
    YT_VERIFY(Context_);
    return Context_;
}

const ITimeProviderPtr& TComputationBase::GetTimeProvider() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();
    return TimeProvider_;
}

const TComputationSpecPtr& TComputationBase::GetSpec() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();
    return GetContext()->ComputationSpec;
}

TDynamicComputationContextPtr TComputationBase::GetDynamicContext() const
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);
    YT_VERIFY(DynamicContext_);
    return DynamicContext_;
}

TDynamicComputationSpecPtr TComputationBase::GetDynamicSpec() const
{
    return GetDynamicContext()->DynamicComputationSpec;
}

IComputation::TParametersPtr TComputationBase::GetParametersBase() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();
    YT_VERIFY(Parameters_);
    return Parameters_;
}

IComputation::TDynamicParametersPtr TComputationBase::GetDynamicParametersBase() const
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);
    YT_VERIFY(DynamicParameters_);
    return DynamicParameters_;
}

IComputation::TDynamicPartitionSpecPtr TComputationBase::GetDynamicPartitionSpecBase() const
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);
    YT_VERIFY(DynamicPartitionSpec_);
    return DynamicPartitionSpec_;
}

i64 TComputationBase::GetSpecGeneration() const
{
    return GetDynamicContext()->SpecGeneration;
}

const NTableClient::TTableSchemaPtr& TComputationBase::GetKeySchema() const
{
    return GetContext()->ComputationSpec->GroupBySchema;
}

TWatermarkStatePtr TComputationBase::GetWatermarkState() const
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);
    YT_VERIFY(WatermarkState_);
    return WatermarkState_;
}

NConcurrency::IThroughputThrottlerPtr TComputationBase::GetThrottler(const TThrottlerId& throttlerId)
{
    YT_VERIFY(ThrottlerFactory_);
    return ThrottlerFactory_->GetClient(throttlerId.Underlying());
}

const THashMap<TStreamId, TStreamTraverseDataPtr>& TComputationBase::GetInputTraverse() const
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);
    YT_VERIFY(InputStreams_);
    return *InputStreams_;
}

EPartitionState TComputationBase::GetPartitionState() const
{
    return GetContext()->Partition->State;
}

TJobId TComputationBase::GetJobId() const
{
    return GetContext()->Job->JobId;
}

const TComputationId& TComputationBase::GetComputationId() const
{
    return GetContext()->Partition->ComputationId;
}

TPartitionId TComputationBase::GetPartitionId() const
{
    return GetContext()->Partition->PartitionId;
}

IRetryableClientPtr TComputationBase::GetRetryableClient() const
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);
    YT_VERIFY(RetryableClient_);
    return RetryableClient_;
}

NTables::TTransactionManagerPtr TComputationBase::GetTransactionManager() const
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);
    YT_VERIFY(TransactionManager_);
    return TransactionManager_;
}

TMessageBuilder TComputationBase::MakeOutputMessageBuilder(std::optional<TStreamId> streamId, TMessageBuilder::TInitFunction init) const
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);

    if (!streamId) {
        if (GetSpec()->OutputStreamIds.size() == 1) {
            streamId = *GetSpec()->OutputStreamIds.begin();
        } else {
            THROW_ERROR_EXCEPTION("Impossible to guess output stream");
        }
    }

    return TMessageBuilder(
        *streamId,
        GetContext()->StreamSpecStorage->GetSchema(*streamId),
        std::move(init));
}

TMessage TComputationBase::ConvertToOutputMessage(const TMessage& message, std::optional<TStreamId> streamId) const
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);

    if (!streamId) {
        if (GetSpec()->OutputStreamIds.size() == 1) {
            streamId = *GetSpec()->OutputStreamIds.begin();
        } else {
            THROW_ERROR_EXCEPTION("Impossible to guess output stream");
        }
    }

    auto spec = GetContext()->StreamSpecStorage->GetSpec(*streamId);
    auto outputMessage = ConvertMessageToNewSchema(message, spec->Schema, GetContext()->ConverterCache);
    outputMessage.StreamId = *streamId;
    return outputMessage;
}

TTimer TComputationBase::MakeTimer(
    const TKey& key,
    const TStreamId& streamId,
    const TSystemTimestamp& triggerTimestamp,
    const TSystemTimestamp& eventTimestamp) const
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);

    TTimer timer;
    timer.Key = key;
    timer.KeySchema = GetKeySchema();
    timer.StreamId = streamId;
    timer.TriggerTimestamp = triggerTimestamp;
    timer.EventTimestamp = eventTimestamp;
    return timer;
}

TNodeTraverseDataPtr TComputationBase::GetNodeTraverse()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return NodeTraverse_.Acquire();
}

void TComputationBase::ApplyPendingStates()
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);
    if (auto pending = PendingWatermarkState_.Exchange(nullptr)) {
        WatermarkState_ = std::move(pending);
    }
    if (auto pending = PendingInputStreams_.Exchange(std::nullopt)) {
        InputStreams_ = std::move(*pending);
    }
    if (auto pending = PendingDynamicContext_.Exchange(nullptr)) {
        EWatchReconfigure mask = EWatchReconfigure::Never;
        if (pending->SpecGeneration != DynamicContext_->SpecGeneration) {
            mask |= EWatchReconfigure::SpecGeneration;
        }
        if (pending->DynamicComputationSpec != DynamicContext_->DynamicComputationSpec) {
            mask |= EWatchReconfigure::DynamicComputationSpec;
            DynamicParameters_ = DynamicPointerCast<IComputation::TDynamicParameters>(
                TRegistry::Get()->ParseDynamicComputationParameters(GetSpec(), pending->DynamicComputationSpec));
            YT_VERIFY(DynamicParameters_);
        }
        if (pending->DynamicPartitionSpec != DynamicContext_->DynamicPartitionSpec) {
            mask |= EWatchReconfigure::DynamicPartitionSpec;
            DynamicPartitionSpec_ = DynamicPointerCast<IComputation::TDynamicPartitionSpec>(
                TRegistry::Get()->ParseDynamicComputationPartitionSpec(GetSpec(), pending->DynamicPartitionSpec));
            YT_VERIFY(DynamicPartitionSpec_);
        }

        DynamicContext_ = std::move(pending);

        if (mask != EWatchReconfigure::Never) {
            for (const auto& [callback, callbackMask] : ReconfigureCallbacks_) {
                if ((mask & callbackMask) != EWatchReconfigure::Never) {
                    callback();
                }
            }
        }
    }
}

void TComputationBase::SubscribeOnReconfigure(NYT::TCallback<void()> callback, IComputation::EWatchReconfigure watchReconfigureMask)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);

    ReconfigureCallbacks_.push_back(std::pair(std::move(callback), watchReconfigureMask));
}

bool TComputationBase::UpdateTraverse(
    TSystemTimestamp reportTime,
    TSystemTimestamp systemWatermark,
    const THashMap<TStreamId, TInflightStreamTraverseDataPtr>& inflights)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);

    YT_LOG_INFO("Building traverse (Inflights: %v)",
        ConvertToYsonString(inflights, NYson::EYsonFormat::Text));

    ValidateComputationInflights(inflights, GetSpec());

    auto traverseData = New<TNodeTraverseData>();
    traverseData->ReportTime = reportTime;

    // Deep copy because traverseData->Streams will be mutated.
    for (const auto& [streamId, streamTraverseData] : GetInputTraverse()) {
        traverseData->Streams[streamId] = CloneYsonStruct(streamTraverseData);
    }

    for (const auto& streamId : TopologicalStreamOrder_) {
        if (!inflights.contains(streamId)) {
            continue;
        }

        std::vector<TStreamTraverseDataPtr> input;
        input.push_back(
            GetOrDefault(
                traverseData->Streams,
                streamId,
                MakeCompletedStreamTraverseData(GetSpecGeneration(), systemWatermark)));

        // Completing and Interrupting partitions do not consume input messages anymore.
        if (GetPartitionState() == EPartitionState::Executing) {
            auto parents = GetOrDefault(GetSpec()->StreamsDependency, streamId, {});
            for (const auto& parentStreamId : parents) {
                if (streamId == parentStreamId && GetSpec()->TimerStreams.contains(streamId) && GetSpec()->AllowTimerSelfDependency) {
                    continue;
                }
                input.push_back(GetOrCrash(traverseData->Streams, parentStreamId));
            }
        }

        auto parentStream = MergeStreamTraverseData(input, EInflightMerge::None);
        // Parent system watermark is ignored due to new messages system timestamp independent from parent messages.
        parentStream->SystemWatermark = systemWatermark;

        traverseData->Streams[streamId] = ApplyInflightTraverseData(
            parentStream,
            GetOrCrash(inflights, streamId),
            systemWatermark);
    }

    bool isFinished = true;
    for (const auto& [streamId, streamTraverse] : traverseData->Streams) {
        isFinished &= streamTraverse->State == EStreamState::Completed;
    }

    YT_LOG_INFO("Built traverse (TraverseData: %v)",
        ConvertToYsonString(traverseData, NYson::EYsonFormat::Text));
    NodeTraverse_.Store(traverseData);
    YT_LOG_INFO("Traverse updated (IsFinished: %v)", isFinished);

    return isFinished;
}

NTables::TTransactionManagerPtr TComputationBase::CreateTransactionManager() const
{
    auto transactionContext = New<NTables::TTransactionManagerContext>();
    transactionContext->Client = Context_->GetClient();
    transactionContext->PipelinePath = Context_->PipelinePath;
    transactionContext->LoadThroughputThrottler = Context_->LoadThroughputThrottler;
    transactionContext->Logger = Logger;
    transactionContext->Profiler = GetContext()->Profiler;
    transactionContext->LeaseId = Context_->Job->LeaseId;
    transactionContext->PartitionId = Context_->Partition->PartitionId;
    transactionContext->StatusProfiler = Context_->StatusProfiler;
    return New<NTables::TTransactionManager>(transactionContext, New<TDynamicRetryableRequestSpec>());
}

std::vector<TStreamId> TComputationBase::BuildTopologicalStreamOrder(TComputationSpecPtr spec)
{
    TIncrementalTopologicalOrdering<TStreamId> graph;
    for (auto& streamId : spec->InputStreamIds) {
        graph.AddVertex(streamId);
    }
    for (auto& streamId : GetKeys(spec->SourceStreams)) {
        graph.AddVertex(streamId);
    }
    for (auto& streamId : GetKeys(spec->KeyVisitorStreams)) {
        graph.AddVertex(streamId);
    }
    for (const auto& [streamId, parentStreams] : spec->StreamsDependency) {
        for (const auto& parentStreamId : parentStreams) {
            if (streamId == parentStreamId && spec->TimerStreams.contains(streamId) && spec->AllowTimerSelfDependency) {
                continue;
            }
            graph.AddEdge(parentStreamId, streamId);
        }
    }
    return graph.GetOrdering();
}

////////////////////////////////////////////////////////////////////////////////

TRootOutputCollector::TRootOutputCollector(TComputationSpecPtr spec, IMetaSetterPtr metaSetter, bool supportsDistribute)
    : Spec_(std::move(spec))
    , MetaSetter_(std::move(metaSetter))
    , SupportsDistribute_(supportsDistribute)
{ }

IOutputCollectorPtr TRootOutputCollector::SetParents(
    const std::vector<TInputMessageConstPtr>& messages,
    const std::vector<TInputTimerConstPtr>& timers,
    const std::vector<TInputVisitConstPtr>& visits)
{
    return New<TOutputCollector>(MakeStrong(this), New<TMessageParents>(messages, timers, visits));
}

void TRootOutputCollector::AddMessage(TMessage&& message, const TMessageParentsConstPtr& parents, bool distribute)
{
    if (!distribute && !SupportsDistribute_) {
        // Non-source computations cannot advance the watermark from a dropped message, so a
        // |distribute| = false message is simply not emitted. (Source computations keep it
        // out of the output but still let it advance the watermark — handled below.)
        return;
    }
    auto setterResult = MetaSetter_->Fill(message, parents);
    Result_.OutputMessages.push_back(std::move(message));
    if (SupportsDistribute_) {
        Result_.OutputMessagesDistribute.push_back(distribute);
    }
    Result_.OutputMessagesParentMessageIds.push_back(std::move(setterResult.ActualParentMessageIds));
}

void TRootOutputCollector::AddTimer(TTimer&& timer, const TMessageParentsConstPtr& parents)
{
    auto setterResult = MetaSetter_->Fill(timer, parents);
    Result_.OutputTimers.push_back(std::move(timer));
    Result_.OutputTimersParentMessageIds.push_back(std::move(setterResult.ActualParentMessageIds));
}

TRootOutputCollector::TTransformResult TRootOutputCollector::CollectResult()
{
    YT_VERIFY(Result_.OutputMessages.size() == Result_.OutputMessagesParentMessageIds.size());
    YT_VERIFY(!SupportsDistribute_ || Result_.OutputMessages.size() == Result_.OutputMessagesDistribute.size());
    YT_VERIFY(Result_.OutputTimers.size() == Result_.OutputTimersParentMessageIds.size());
    return std::exchange(Result_, TTransformResult{});
}

////////////////////////////////////////////////////////////////////////////////

TOutputCollector::TOutputCollector(TRootOutputCollectorPtr rootCollector, const TMessageParentsConstPtr& parents)
    : RootCollector_(std::move(rootCollector))
    , Parents_(parents)
{ }

IOutputCollectorPtr TOutputCollector::SetParents(
    const std::vector<TInputMessageConstPtr>& messages,
    const std::vector<TInputTimerConstPtr>& timers,
    const std::vector<TInputVisitConstPtr>& visits)
{
    return RootCollector_->SetParents(messages, timers, visits);
}

void TOutputCollector::AddMessage(TMessage&& message, bool distribute)
{
    RootCollector_->AddMessage(std::move(message), Parents_, distribute);
}

void TOutputCollector::AddTimer(TSystemTimestamp triggerTimestamp, std::optional<TSystemTimestamp> eventTimestamp)
{
    TTimer timer;
    timer.TriggerTimestamp = triggerTimestamp;
    if (eventTimestamp) {
        timer.EventTimestamp = *eventTimestamp;
    }
    AddTimer(std::move(timer));
}

void TOutputCollector::AddTimer(const TStreamId& streamId, TSystemTimestamp triggerTimestamp, std::optional<TSystemTimestamp> eventTimestamp)
{
    TTimer timer;
    timer.StreamId = streamId;
    timer.TriggerTimestamp = triggerTimestamp;
    if (eventTimestamp) {
        timer.EventTimestamp = *eventTimestamp;
    }
    AddTimer(std::move(timer));
}

void TOutputCollector::AddTimer(TTimer&& timer)
{
    RootCollector_->AddTimer(std::move(timer), Parents_);
}

////////////////////////////////////////////////////////////////////////////////

void TOutputStoreStreamOrchidState::Register(TRegistrar registrar)
{
    registrar.Parameter("used_count", &TThis::UsedCount)
        .Default();
    registrar.Parameter("limit_count", &TThis::LimitCount)
        .Default();

    registrar.Parameter("used_bytes", &TThis::UsedBytes)
        .Default();
    registrar.Parameter("limit_bytes", &TThis::LimitBytes)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TUniversalComputationOrchidState::Register(TRegistrar registrar)
{
    registrar.Parameter("partition_description", &TThis::PartitionDescription)
        .Default();
    registrar.Parameter("epoch_parts_wall_time", &TThis::EpochPartsWallTime)
        .Default();
    registrar.Parameter("output_store", &TThis::OutputStore)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TUniversalComputationDynamicPartitionSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("active_source", &TThis::ActiveSource)
        .Default();
    registrar.Parameter("blocked_output_streams", &TThis::BlockedOutputStreams)
        .Default();
    registrar.Parameter("finish_after_current_epoch", &TThis::FinishAfterCurrentEpoch)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TUniversalComputationPartitionStatus::Register(TRegistrar registrar)
{
    registrar.Parameter("active_source_status", &TThis::ActiveSourceStatus)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TPendingDistributedOutputs::PushNormal(TOutputMessageConstPtr&& message)
{
    auto guard = Guard(Lock_);
    if (Finished_) {
        return;
    }
    Normal_.emplace_back(std::move(message));
}

void TPendingDistributedOutputs::PushInit(TOutputMessageConstPtr&& message)
{
    auto guard = Guard(Lock_);
    if (Finished_) {
        return;
    }
    Init_.emplace_back(std::move(message));
}

std::pair<TPendingDistributedOutputs::TDeque, TPendingDistributedOutputs::TDeque>
TPendingDistributedOutputs::Extract(bool finish)
{
    auto guard = Guard(Lock_);
    Finished_ |= finish;
    return {std::exchange(Normal_, {}), std::exchange(Init_, {})};
}

////////////////////////////////////////////////////////////////////////////////

TUniversalComputationBase::TUniversalComputationBase(
    TComputationContextPtr context,
    TDynamicComputationContextPtr dynamicContext)
    : TComputationBase(std::move(context), std::move(dynamicContext))
    , StateManager_(CreateStateManager())
    , HasVisitorDrivenJoiners_(ComputeHasVisitorDrivenJoiners())
    , ActiveSourceStreamId_(CreateActiveSourceStreamId())
    , ActiveSource_(CreateActiveSource())
    , Sinks_()
    , InputStore_(CreateInputStore())
    , TimerStore_(CreateTimerStore())
    , OutputStore_(CreateOutputStore())
    , KeyVisitors_(CreateKeyVisitors())
    , Tracer_(CreateComputationTracer(GetContext(), GetSpec(), GetDynamicSpec()->Tracer))
    , EventTimestampAssigner_(CreateEventTimestampAssigner(GetSpec()->WatermarkStrategy->EventTimestampAssigner))
    , StartTime_(TInstant::Now())
    , RunIterationStartPromise_(NewPromise<void>())
    , BeforeCommitInIterationPromise_(NewPromise<void>())
    , RunIterationFinishPromise_(NewPromise<void>())
    , EpochTimer_(GetContext()->Profiler.Timer("/epoch_time"))
    , EpochCounter_(GetContext()->Profiler.Counter("/epoch"))
    , StreamMessageCounters_(CreateStreamMessageCounters(GetContext()->Profiler.WithPrefix("/watermark_info"), GetSpec()))
    , InputEventLagObserver_(GetContext()->Profiler.WithPrefix("/event_lag/input"), GetInputEventLagStreamIds(GetSpec()))
    , OutputEventLagObserver_(GetContext()->Profiler.WithPrefix("/event_lag/output"), GetOutputEventLagStreamIds(GetSpec()))
    , PendingProcessedOutputs_(New<TPendingDistributedOutputs>())
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);

    SubscribeOnReconfigure(BIND(
        [this] () {
            for (const auto& [sinkId, key, sink] : GetAllSinks()) {
                auto dynamicSinkContext = New<TDynamicSinkContext>();
                dynamicSinkContext->DynamicSinkSpec = GetOrDefault(GetDynamicSpec()->Sinks, sinkId, New<TDynamicSinkSpec>());
                sink->Reconfigure(dynamicSinkContext);
            }
            if (TimerStore_) {
                auto dynamicTimerStoreContext = New<TDynamicTimerStoreContext>();
                dynamicTimerStoreContext->DynamicTimerStoreSpec = GetDynamicSpec()->TimerStore;
                dynamicTimerStoreContext->Draining = GetDynamicSpec()->Draining;
                TimerStore_->Reconfigure(std::move(dynamicTimerStoreContext));
            }
            if (InputStore_) {
                InputStore_->Reconfigure(GetDynamicSpec()->InputStore);
            }
            for (const auto& [streamId, visitor] : KeyVisitors_) {
                auto dynamicContext = New<TDynamicKeyVisitorContext>();
                dynamicContext->DynamicSpec = GetOrDefault(
                    GetDynamicSpec()->KeyVisitorStreams,
                    streamId,
                    New<TDynamicKeyVisitorStreamSpec>());
                dynamicContext->Draining = GetDynamicSpec()->Draining;
                visitor->Reconfigure(std::move(dynamicContext));
            }
            OutputStore_->Reconfigure(GetDynamicSpec()->OutputStore);
            Tracer_->Reconfigure(GetDynamicSpec()->Tracer);
            auto dynamicManagerContext = New<TDynamicJobStateManagerContext>();
            dynamicManagerContext->StateManager = GetDynamicSpec()->StateManager;
            dynamicManagerContext->ExternalStateManagers = GetDynamicSpec()->ExternalStateManagers;
            dynamicManagerContext->ExternalStateJoiners = GetDynamicSpec()->ExternalStateJoiners;
            dynamicManagerContext->StateJoiners = GetDynamicSpec()->StateJoiners;
            StateManager_->Reconfigure(std::move(dynamicManagerContext));
        }),
        EWatchReconfigure::DynamicComputationSpec);

    SubscribeOnReconfigure(BIND(
        [this] () {
            if (ActiveSource_) {
                YT_VERIFY(ActiveSourceStreamId_);
                auto dynamicSourceContext = New<TDynamicSourceContext>();
                dynamicSourceContext->DynamicSourceSpec = GetPatchedDynamicSourceSpec(GetDynamicSpec(), *ActiveSourceStreamId_);
                dynamicSourceContext->DynamicPartitionSpec = GetDynamicPartitionSpec()->ActiveSource;
                ActiveSource_->Reconfigure(dynamicSourceContext);
            }
        }),
        EWatchReconfigure::AnySpec);
}

TUniversalComputationBase::~TUniversalComputationBase()
{
    PendingProcessedOutputs_->Extract(/*finish*/ true);
    if (ActiveSource_) {
        ActiveSource_->Terminate();
    }
    for (const auto& [_, visitor] : KeyVisitors_) {
        visitor->Stop();
    }
}

TComputationOrchidStatePtr TUniversalComputationBase::GetOrchidState()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto state = New<TUniversalComputationOrchidState>();
    for (const auto& [partName, partState] : Tracer_->GetPartStates()) {
        state->EpochPartsWallTime[partName] = partState.WallTimeEma;
    }

    const auto outputStoreCountAndByteSize = OutputStore_->GetCountAndByteSizes();
    for (const auto& streamId : GetSpec()->OutputStreamIds) {
        auto [count, byteSize] = outputStoreCountAndByteSize.at(streamId);
        auto streamState = New<TOutputStoreStreamOrchidState>();
        streamState->UsedCount = count;
        streamState->UsedBytes = byteSize;
        state->OutputStore[streamId] = streamState;
    }

    state->PartitionDescription = Format("Range: %v-%v", GetContext()->Partition->LowerKey, GetContext()->Partition->UpperKey);
    return state;
}

TComputationStatusPtr TUniversalComputationBase::GetStatus()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto status = New<TComputationStatus>();

    status->NodeTraverse = GetNodeTraverse();

    {
        auto guard = Guard(LimitsLock_);
        status->InputLimits = InputLimits_;
        status->OutputLimits = OutputLimits_;
    }

    {
        auto partitionStatus = New<TUniversalComputationPartitionStatus>();
        if (ActiveSource_) {
            partitionStatus->ActiveSourceStatus = ActiveSource_->GetPartitionStatus();
        }
        status->PartitionStatus = ConvertTo<IMapNodePtr>(partitionStatus);
    }

    for (const auto& [partName, partState] : Tracer_->GetPartStates()) {
        status->EpochPartTimes[partName] = partState.WallTimeEma.SecondsFloat();
    }

    return status;
}

THashMap<std::string, THashMap<TStreamId, TJobEntityLimitStatus>> TUniversalComputationBase::GetExtraInputLimits()
{
    return {};
}

bool TUniversalComputationBase::UpdateStatus(
    TSystemTimestamp reportTime,
    TSystemTimestamp systemWatermark,
    const THashMap<TStreamId, TInflightStreamTraverseDataPtr>& inflights)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);

    NTracing::TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Accounting"));

    bool isFinished = UpdateTraverse(reportTime, systemWatermark, inflights);

    auto inputLimits = GetExtraInputLimits();

    const auto outputStoreCountAndByteSize = OutputStore_->GetCountAndByteSizes();
    THashMap<std::string, THashMap<TStreamId, TJobEntityLimitStatus>> outputLimits;
    for (const auto& streamId : GetSpec()->OutputStreamIds) {
        auto [count, byteSize] = GetOrCrash(outputStoreCountAndByteSize, streamId);
        {
            auto& entityLimitStatus = outputLimits["output_store_bytes"][streamId];
            entityLimitStatus.Limit = GetDynamicSpec()->OutputStoreByteSizeLimit;
            entityLimitStatus.Used = byteSize;
        }
        {
            auto& entityLimitStatus = outputLimits["output_store_count"][streamId];
            entityLimitStatus.Limit = GetDynamicSpec()->OutputStoreCountLimit;
            entityLimitStatus.Used = count;
        }
    }
    {
        auto guard = Guard(LimitsLock_);
        InputLimits_ = std::move(inputLimits);
        OutputLimits_ = std::move(outputLimits);
    }

    return isFinished;
}

THashMap<TStreamId, TKeyVisitorPtr> TUniversalComputationBase::CreateKeyVisitors() const
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);

    if (GetPartitionState() != EPartitionState::Executing) {
        return {};
    }
    if (GetSpec()->KeyVisitorStreams.empty()) {
        return {};
    }

    auto tableContext = New<NTables::TContext>();
    tableContext->Client = GetRetryableClient();
    tableContext->PipelinePath = GetContext()->PipelinePath;
    tableContext->LoadThroughputThrottler = GetContext()->LoadThroughputThrottler;
    tableContext->Logger = Logger;
    tableContext->Profiler = GetContext()->Profiler.WithPrefix("/tables");

    auto keyStatesDynamicSpec = New<TDynamicTableRequestSpec>();
    auto keyStates = New<NTables::TKeyStates>(tableContext, keyStatesDynamicSpec);
    auto stateTable = New<NTables::TKeyVisitorStates>(tableContext, keyStatesDynamicSpec);

    const auto partitionRange = TKeyRange{
        GetContext()->Partition->LowerKey.value_or(MinKey()),
        GetContext()->Partition->UpperKey.value_or(MaxKey()),
    };

    THashMap<TStreamId, TKeyVisitorPtr> result;
    for (const auto& [streamId, streamSpec] : GetSpec()->KeyVisitorStreams) {
        auto context = New<TKeyVisitorContext>();
        context->ComputationId = GetContext()->Partition->ComputationId;
        context->StreamId = streamId;
        context->Spec = streamSpec;
        context->PartitionRange = partitionRange;
        context->KeyStates = keyStates;
        context->StateManager = StateManager_;
        context->KeyVisitorStates = stateTable;
        context->TimeProvider = GetTimeProvider();
        context->SerializedInvoker = GetContext()->SerializedInvoker;
        context->Logger = Logger.WithTag("KeyVisitorStreamId: %v", streamId);
        context->Profiler = GetContext()->Profiler.WithPrefix("/key_visitor_streams").WithTag("stream_id", streamId.Underlying());
        context->StatusProfiler = GetContext()->StatusProfiler->WithPrefix(Format("/key_visitor/%v", streamId));

        auto dynamicContext = New<TDynamicKeyVisitorContext>();
        dynamicContext->DynamicSpec = GetOrDefault(
            GetDynamicSpec()->KeyVisitorStreams,
            streamId,
            New<TDynamicKeyVisitorStreamSpec>());
        dynamicContext->Draining = GetDynamicSpec()->Draining;

        result[streamId] = New<TKeyVisitor>(std::move(context), std::move(dynamicContext));
    }
    return result;
}

TJobStateManagerPtr TUniversalComputationBase::CreateStateManager() const
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);

    auto tableContext = New<NTables::TContext>();
    tableContext->Client = GetRetryableClient();
    tableContext->PipelinePath = GetContext()->PipelinePath;
    tableContext->LoadThroughputThrottler = GetContext()->LoadThroughputThrottler;
    tableContext->Logger = Logger;
    tableContext->Profiler = GetContext()->Profiler.WithPrefix("/tables");

    auto managerContext = New<TJobStateManagerContext>();
    managerContext->ComputationId = GetContext()->Partition->ComputationId;
    managerContext->PartitionId = GetContext()->Partition->PartitionId;
    managerContext->StateCache = GetContext()->JobStateCache;
    managerContext->LoadThroughputThrottler = GetContext()->LoadThroughputThrottler;
    managerContext->Logger = Logger;
    managerContext->Profiler = GetContext()->Profiler;
    managerContext->StatusProfiler = GetContext()->StatusProfiler;
    managerContext->SerializedInvoker = GetContext()->SerializedInvoker;
    managerContext->KeyStates = New<NTables::TKeyStates>(tableContext, New<TDynamicTableRequestSpec>());
    managerContext->PartitionStates = New<NTables::TPartitionStates>(tableContext, New<TDynamicTableRequestSpec>());
    managerContext->ClientsCache = GetContext()->ClientsCache;
    managerContext->PipelinePath = GetContext()->PipelinePath;
    managerContext->KeySchema = GetKeySchema();
    managerContext->ConverterCache = GetContext()->ConverterCache;
    managerContext->StaticResources = GetContext()->StaticResources;
    managerContext->ExternalStateManagers = GetSpec()->ExternalStateManagers;
    managerContext->ExternalStateJoiners = GetSpec()->ExternalStateJoiners;
    managerContext->StateJoiners = GetSpec()->StateJoiners;

    auto dynamicManagerContext = New<TDynamicJobStateManagerContext>();
    dynamicManagerContext->StateManager = GetDynamicSpec()->StateManager;
    dynamicManagerContext->ExternalStateManagers = GetDynamicSpec()->ExternalStateManagers;
    dynamicManagerContext->ExternalStateJoiners = GetDynamicSpec()->ExternalStateJoiners;
    dynamicManagerContext->StateJoiners = GetDynamicSpec()->StateJoiners;

    return New<TJobStateManager>(std::move(managerContext), std::move(dynamicManagerContext));
}

std::optional<TStreamId> TUniversalComputationBase::CreateActiveSourceStreamId()
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);

    if (GetSpec()->SourceStreams.size() > 0 && GetPartitionState() == EPartitionState::Executing) {
        THROW_ERROR_EXCEPTION_UNLESS(GetContext()->Partition->SourceKey, "Expected SourceKey with source stream id");
        auto [streamId, sourceKey] = SplitUniversalPartitionKey(*GetContext()->Partition->SourceKey);
        return streamId;
    }
    return std::nullopt;
}

ISourcePtr TUniversalComputationBase::CreateActiveSource()
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);

    if (GetSpec()->SourceStreams.size() > 0 && GetPartitionState() == EPartitionState::Executing) {
        THROW_ERROR_EXCEPTION_UNLESS(GetContext()->Partition->SourceKey, "Expected SourceKey with source stream id");
        auto [streamId, sourceKey] = SplitUniversalPartitionKey(*GetContext()->Partition->SourceKey);
        auto context = New<TSourceContext>();
        static_cast<TComputationContextBase&>(*context) = *GetContext();
        context->Logger = context->Logger.WithTag("SourceStreamId: %v", streamId.Underlying());
        context->Profiler = context->Profiler.WithPrefix("/source_streams").WithTag("stream_id", streamId.Underlying());
        context->StatusProfiler = context->StatusProfiler->WithPrefix(Format("/sources/%v", streamId));
        context->SourceStreamId = streamId;
        context->SourceKey = sourceKey;
        context->SourceSpec = GetSpec()->SourceStreams.at(streamId);
        context->TimeProvider = GetTimeProvider();
        auto dynamicSourceContext = New<TDynamicSourceContext>();
        dynamicSourceContext->DynamicSourceSpec = GetPatchedDynamicSourceSpec(GetDynamicSpec(), *ActiveSourceStreamId_);
        dynamicSourceContext->DynamicPartitionSpec = GetDynamicPartitionSpec()->ActiveSource;
        return TRegistry::Get()->CreateSource(context, dynamicSourceContext);
    }
    return nullptr;
}

IInputStorePtr TUniversalComputationBase::CreateInputStore()
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);

    if (GetPartitionState() != EPartitionState::Executing) {
        return nullptr;
    }
    auto context = New<TInputStoreContext>();
    context->Logger = Logger;
    context->Profiler = GetContext()->Profiler;

    context->Partition = GetContext()->Partition;
    context->InputStreamIds = GetSpec()->InputStreamIds;
    {
        auto tableContext = New<NTables::TContext>();
        tableContext->Client = GetRetryableClient()->WithErrorComponent("/input_messages");
        tableContext->PipelinePath = GetContext()->PipelinePath;
        tableContext->LoadThroughputThrottler = GetContext()->LoadThroughputThrottler;
        tableContext->Logger = Logger;
        tableContext->Profiler = GetContext()->Profiler.WithPrefix("/tables");
        if (ResolveUseCompactInputMessages(GetSpec())) {
            tableContext->Tag = Format("%s:compact_input_messages", GetContext()->Partition->ComputationId);
            context->InputMessagesTable = New<NTables::TCompactInputMessages>(tableContext);
        } else {
            tableContext->Tag = Format("%s:input_messages", GetContext()->Partition->ComputationId);
            context->InputMessagesTable = New<NTables::TInputMessages>(tableContext);
        }
    }
    return NFlow::CreateInputStore(context, GetDynamicSpec()->InputStore);
}

ITimerStorePtr TUniversalComputationBase::CreateTimerStore()
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);

    if (GetPartitionState() != EPartitionState::Executing) {
        return nullptr;
    }
    auto context = New<TTimerStoreContext>();
    context->Logger = Logger;
    context->Profiler = GetContext()->Profiler;

    context->Partition = GetContext()->Partition;
    context->KeySchema = GetSpec()->GroupBySchema;
    context->StreamsDependency = GetSpec()->StreamsDependency;
    context->TimerSpecs = GetSpec()->TimerStreams;
    context->WatermarkPercentileSpec = GetSpec()->WatermarkStrategy->WatermarkPercentile;
    {
        auto tableContext = New<NTables::TContext>();
        tableContext->Client = GetRetryableClient()->WithErrorComponent("timers");
        tableContext->PipelinePath = GetContext()->PipelinePath;
        tableContext->LoadThroughputThrottler = GetContext()->LoadThroughputThrottler;
        tableContext->Logger = Logger;
        tableContext->Profiler = GetContext()->Profiler.WithPrefix("/tables");
        tableContext->Tag = Format("%s:timers", GetContext()->Partition->ComputationId);
        context->TimersTable = New<NTables::TTimers>(tableContext, New<TDynamicTableRequestSpec>());
    }
    auto dynamicTimerStoreContext = New<TDynamicTimerStoreContext>();
    dynamicTimerStoreContext->DynamicTimerStoreSpec = GetDynamicSpec()->TimerStore;
    dynamicTimerStoreContext->Draining = GetDynamicSpec()->Draining;
    return NFlow::CreateTimerStore(context, std::move(dynamicTimerStoreContext));
}

IOutputStorePtr TUniversalComputationBase::CreateOutputStore()
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);

    auto createTableContext = [&] (TStringBuf tableName) {
        auto tableContext = New<NTables::TContext>();
        tableContext->Client = GetRetryableClient()->WithErrorComponent("output_messages");
        tableContext->PipelinePath = GetContext()->PipelinePath;
        tableContext->LoadThroughputThrottler = GetContext()->LoadThroughputThrottler;
        tableContext->Logger = Logger;
        tableContext->Profiler = GetContext()->Profiler.WithPrefix("/tables");
        tableContext->Tag = Format("%s:%s", GetContext()->Partition->ComputationId, tableName);
        return tableContext;
    };

    auto fillBase = [&] (TOutputStoreContext& context) {
        context.Logger = Logger;
        context.Profiler = GetContext()->Profiler;
        context.Partition = GetContext()->Partition;
        context.OutputStreamIds = GetSpec()->OutputStreamIds;
        context.WatermarkPercentileSpec = GetSpec()->WatermarkStrategy->WatermarkPercentile;
        context.StreamSpecStorage = GetContext()->StreamSpecStorage;
        context.StreamLimitUsageStates = GetContext()->OutputStreamLimitUsageStates;
    };

    auto context = New<TCompactOutputStoreContext>();
    fillBase(*context);
    context->CompactPartitionOutputMessagesTable = NTables::CreateCompactPartitionOutputMessages(
        createTableContext("compact_partition_output_messages"),
        New<TDynamicTableRequestSpec>());
    context->CompactOutputMessagesTable = NTables::CreateCompactOutputMessages(
        createTableContext("compact_output_messages"),
        New<TDynamicTableRequestSpec>());
    context->TimeProvider = GetTimeProvider();
    return NFlow::CreateCompactOutputStore(context, GetDynamicSpec()->OutputStore);
}

TWatermarkStatePtr TUniversalComputationBase::GetEpochWatermarkState() const
{
    return GetWatermarkState();
}

TSystemTimestamp TUniversalComputationBase::GetCurrentTimestamp() const
{
    return GetEpochWatermarkState()->GetCurrentTimestamp();
}

TSystemTimestamp TUniversalComputationBase::GetInputEventWatermark() const
{
    auto watermark = InfinitySystemTimestamp;
    for (const auto& streamId : GetSpec()->InputStreamIds) {
        watermark = std::min(watermark, GetEventWatermark(streamId));
    }
    return watermark;
}

TSystemTimestamp TUniversalComputationBase::GetEpochInputEventWatermark() const
{
    return GetInputEventWatermark();
}

TSystemTimestamp TUniversalComputationBase::GetEventWatermark(const TStreamId& streamId) const
{
    return GetWatermarkState()->GetEventWatermark(streamId);
}

TSystemTimestamp TUniversalComputationBase::GetEpochEventWatermark(const TStreamId& streamId) const
{
    return GetEventWatermark(streamId);
}

TSystemTimestamp TUniversalComputationBase::GetSystemWatermark(const TStreamId& streamId) const
{
    return GetWatermarkState()->GetSystemWatermark(streamId);
}

TSystemTimestamp TUniversalComputationBase::GetEpochSystemWatermark(const TStreamId& streamId) const
{
    return GetSystemWatermark(streamId);
}

TSystemTimestamp TUniversalComputationBase::GetWatermark(const TStreamId& streamId, ETimeType timeType) const
{
    return GetWatermarkState()->GetWatermark(streamId, timeType);
}

TSystemTimestamp TUniversalComputationBase::GetEpochWatermark(const TStreamId& streamId, ETimeType timeType) const
{
    return GetWatermark(streamId, timeType);
}

THashMap<TStreamId, TInflightStreamTraverseDataPtr> TUniversalComputationBase::BuildInflights() const
{
    const auto emptyStream = New<TInflightStreamTraverseData>();
    emptyStream->Empty = true;
    emptyStream->Suspended = true;
    emptyStream->InflightMetrics->Count = 0;
    emptyStream->InflightMetrics->ByteSize = 0;

    THashMap<TStreamId, TInflightStreamTraverseDataPtr> inflights;
    if (TimerStore_) {
        for (const auto& [streamId, inflight] : TimerStore_->BuildInflight()) {
            inflights[streamId] = inflight;
        }
    } else {
        for (const auto& streamId : GetKeys(GetSpec()->TimerStreams)) {
            inflights[streamId] = CloneYsonStruct(emptyStream);
        }
    }

    // Materialize source / source-substitute inflights first so we can hand the
    // KeyVisitors an "all upstream non-visit streams are completed" signal —
    // visit-streams use it to terminate finite-input pipelines.
    for (const auto& sourceStreamId : GetKeys(GetSpec()->SourceStreams)) {
        inflights[sourceStreamId] = CloneYsonStruct(emptyStream);
    }
    if (ActiveSource_) {
        YT_VERIFY(ActiveSourceStreamId_);
        inflights[*ActiveSourceStreamId_] = ActiveSource_->BuildInflight();
    }

    bool upstreamCompleted = true;
    if (ActiveSource_) {
        upstreamCompleted &= inflights[*ActiveSourceStreamId_]->Empty;
    }
    for (const auto& [_, streamData] : GetInputTraverse()) {
        upstreamCompleted &= (streamData->State >= EStreamState::Completed);
    }
    if (upstreamCompleted) {
        for (const auto& [_, visitor] : KeyVisitors_) {
            visitor->SetUpstreamCompleted();
        }
    }
    for (const auto& [streamId, visitor] : KeyVisitors_) {
        for (const auto& [innerStreamId, inflight] : visitor->BuildInflight()) {
            inflights[innerStreamId] = inflight;
        }
    }
    // Computations that aren't actively running visit-streams (e.g. partition not Executing)
    // still report empty inflight so the watermark tracker can see them.
    for (const auto& streamId : GetKeys(GetSpec()->KeyVisitorStreams)) {
        if (!inflights.contains(streamId)) {
            inflights[streamId] = CloneYsonStruct(emptyStream);
        }
    }

    if (OutputStore_) {
        for (const auto& [streamId, inflight] : OutputStore_->BuildInflight()) {
            inflights[streamId] = inflight;
        }
    } else {
        for (const auto& streamId : GetSpec()->OutputStreamIds) {
            inflights[streamId] = CloneYsonStruct(emptyStream);
        }
    }
    return inflights;
}

void TUniversalComputationBase::RegisterInputBeforeProcessing(
    const std::vector<TInputMessageConstPtr>& inputMessages,
    const std::vector<TInputTimerConstPtr>& inputTimers,
    const std::vector<TInputVisitConstPtr>& inputVisits,
    const THashMap<TStreamId, TSystemTimestamp>& watermarkOverrides)
{
    struct TStreamState
    {
        TSystemTimestamp Watermark = ZeroSystemTimestamp;
        TStreamMessageCounters* Counters = nullptr;
    };

    THashMap<TStreamId, TStreamState> streamStates;
    streamStates.reserve(GetSpec()->InputStreamIds.size() + GetSpec()->SourceStreams.size());

    auto checkWatermark = [&] (const TMessageMeta& messageMeta) {
        auto [iter, emplaced] = streamStates.try_emplace(messageMeta.StreamId, TStreamState());
        auto& state = iter->second;
        if (emplaced) {
            state.Watermark = GetEpochEventWatermark(messageMeta.StreamId);
            if (auto overrideIter = watermarkOverrides.find(messageMeta.StreamId); overrideIter != watermarkOverrides.end()) {
                state.Watermark = overrideIter->second;
            }
            state.Counters = &GetOrCrash(StreamMessageCounters_, messageMeta.StreamId);
        }
        state.Counters->TotalMessages.Increment();
        InputEventLagObserver_.Observe(messageMeta.StreamId, messageMeta.EventTimestamp);
        if (messageMeta.EventTimestamp < state.Watermark) {
            YT_LOG_WARNING("Input is late (MessageId: %v, StreamId: %v, EventTimestamp: %v, EventWatermark: %v)",
                messageMeta.MessageId,
                messageMeta.StreamId,
                messageMeta.EventTimestamp,
                state.Watermark);

            state.Counters->LateMessages.Increment();
        }
    };

    for (auto& message : inputMessages) {
        YT_LOG_DEBUG("MessageLifeCycle.Computation: message was taken to be processed in epoch "
            "(MessageId: %v, Key: %v, StreamId: %v, SystemTimestamp: %v, EventTimestamp: %v)",
            message->MessageId,
            message->Key,
            message->StreamId,
            message->SystemTimestamp,
            message->EventTimestamp);
        checkWatermark(*message);
    }
    for (auto& timer : inputTimers) {
        YT_LOG_DEBUG("MessageLifeCycle.Computation: timer was taken to be processed in epoch "
            "(MessageId: %v, Key: %v, StreamId: %v, SystemTimestamp: %v, EventTimestamp: %v, TriggerTimestamp: %v)",
            timer->MessageId,
            timer->Key,
            timer->StreamId,
            timer->SystemTimestamp,
            timer->EventTimestamp,
            timer->TriggerTimestamp);
        checkWatermark(*timer);
    }
    for (auto& visit : inputVisits) {
        YT_LOG_DEBUG("MessageLifeCycle.Computation: visit was taken to be processed in epoch "
            "(MessageId: %v, Key: %v, StreamId: %v, SystemTimestamp: %v, EventTimestamp: %v)",
            visit->MessageId,
            visit->Key,
            visit->StreamId,
            visit->SystemTimestamp,
            visit->EventTimestamp);
        checkWatermark(*visit);
    }
}

template <class TGetKey, class TMakeTrackerCallback>
void TUniversalComputationBase::DistributeOutputMessagesImpl(
    const IComputationRunContextPtr& context,
    std::span<const TOutputMessageConstPtr> messages,
    const TDynamicComputationSpecPtr& dynamicSpec,
    TGetKey&& getKey,
    TMakeTrackerCallback&& makeTrackerCallback)
{
    OutputEventLagObserver_.ObserveBatch(messages);

    std::vector<TDistributingTracker> trackers;
    trackers.reserve(messages.size());
    for (size_t i = 0; i < messages.size(); ++i) {
        const auto& outputMessage = messages[i];
        trackers.emplace_back(makeTrackerCallback(i));
        for (const auto& [sinkId, sinkSpec] : GetSpec()->Sinks) {
            if (!sinkSpec->InputStreamIds.contains(outputMessage->StreamId)) {
                continue;
            }
            auto sink = GetOrCreateSink(sinkId, getKey(i), dynamicSpec);
            sink->Distribute(outputMessage, trackers.back().AddDestination());
        }
    }

    // Register all messages with the run context in one lock acquisition.
    context->RegisterOutputMessages(messages, std::span<TDistributingTracker>(trackers));

    // Activate all trackers.
    for (auto& tracker : trackers) {
        tracker.Activate();
    }
}

void TUniversalComputationBase::RegisterOutputMessages(
    const IComputationRunContextPtr& context,
    std::span<const TOutputMessageConstPtr> messages,
    const std::optional<TKey>& parentKey,
    const TDynamicComputationSpecPtr& dynamicSpec)
{
    if (messages.empty()) {
        return;
    }

    DistributeOutputMessagesImpl(
        context,
        messages,
        dynamicSpec,
        /*getKey*/ [&] (size_t) -> const std::optional<TKey>& {
            return parentKey;
        },
        /*makeTrackerCallback*/ [&] (size_t i) {
            return [pendingOutputs = PendingProcessedOutputs_, msg = TOutputMessageConstPtr(messages[i])] () mutable {
                pendingOutputs->PushNormal(std::move(msg));
            };
        });
}

TUniversalComputationBase::TRunIterationGuard TUniversalComputationBase::StartRunIteration(const IComputationRunContextPtr& context)
{
    // Refresh the throttler priority: smaller timestamp == higher priority on the server queue.
    auto sourceTimestamp = InfinitySystemTimestamp;
    if (auto orderedSource = DynamicPointerCast<IOrderedSource>(ActiveSource_)) {
        sourceTimestamp = orderedSource->GetReadAlignmentTimestamp();
    }
    auto priority = std::min(context->GetInputStabilizedEventTimestamp(), sourceTimestamp);
    // InfinitySystemTimestamp means we don't have any information yet.
    // So it's better to have top priority to get at least some data to process.
    if (priority == InfinitySystemTimestamp) {
        priority = ZeroSystemTimestamp;
    }
    GetThrottlerFactory()->SetPriority(priority.Underlying());

    auto epochTraceContext = Tracer_->StartEpochTraceContext(++RunIteration_);
    TTraceContextGuard epochTraceGuard(epochTraceContext);
    TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Start"));
    TPromise<void> promise;
    {
        auto guard = Guard(Lock_);
        promise = std::exchange(RunIterationStartPromise_, NewPromise<void>());
        CurrentRunIterationPromise_ = std::exchange(RunIterationFinishPromise_, NewPromise<void>());
    }
    EpochCounter_.Increment();

    CurrentRunIterationPromise_.ToFuture().Subscribe(BIND([weakThis = MakeWeak(this), start = TInstant::Now()] (const TError& error) {
        if (auto strongThis = weakThis.Lock(); strongThis && error.IsOK()) {
            auto elapsed = TInstant::Now() - start;
            strongThis->EpochTimer_.Record(elapsed);
        }
    }));
    // Apply pending state before any computation in this iteration.
    ApplyPendingStates();

    if (TimerStore_) {
        TimerStore_->UpdateWatermarkState(GetWatermarkState());
    }
    if (InputStore_) {
        InputStore_->AdvanceSystemWatermark(MergeStreamTraverseData(GetValues(GetInputTraverse()), EInflightMerge::None)->SystemWatermark);
    }
    promise.Set();
    YT_LOG_INFO("Run iteration started");
    return {
        .TraceContext = std::move(epochTraceContext),
        .TraceContextGuard = std::move(epochTraceGuard),
    };
}

void TUniversalComputationBase::ThrottleInputBatch(
    const std::vector<TInputMessageConstPtr>& messages,
    const std::vector<TInputTimerConstPtr>& timers,
    const std::vector<TInputVisitConstPtr>& visits)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);

    if (messages.empty() && timers.empty() && visits.empty()) {
        return;
    }

    const auto& dynamicSpec = GetDynamicSpec();
    const auto& rowsThrottlerId = dynamicSpec->InputRowsThrottlerId;
    const auto& bytesThrottlerId = dynamicSpec->InputBytesThrottlerId;
    if (!rowsThrottlerId && !bytesThrottlerId) {
        return;
    }

    TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Input.Throttle"));

    std::vector<TFuture<void>> futures;
    if (rowsThrottlerId) {
        i64 size = std::ssize(messages) + std::ssize(timers) + std::ssize(visits);
        futures.push_back(GetThrottler(*rowsThrottlerId)->Throttle(size));
    }
    if (bytesThrottlerId) {
        i64 totalBytes = 0;
        for (const auto& input : messages) {
            totalBytes += input->ByteSize;
        }
        for (const auto& timer : timers) {
            totalBytes += timer->ByteSize;
        }
        for (const auto& visit : visits) {
            totalBytes += visit->ByteSize;
        }
        futures.push_back(GetThrottler(*bytesThrottlerId)->Throttle(totalBytes));
    }

    WaitFor(AllSucceeded(std::move(futures)))
        .ThrowOnError();
}

IRetryableTransactionPtr TUniversalComputationBase::PrepareTransaction(const IComputationRunContextPtr& context)
{
    TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("StartTransaction"));
    TPromise<void> beforeCommit;
    {
        auto guard = Guard(Lock_);
        beforeCommit = std::exchange(BeforeCommitInIterationPromise_, NewPromise<void>());
    }
    beforeCommit.Set();

    DrainDistributedOutputs(context);

    return GetTransactionManager()->CreateTransaction();
}

void TUniversalComputationBase::Commit(IComputationRunContextPtr context, IRetryableTransactionPtr transaction)
{
    YT_VERIFY(transaction);
    {
        TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("FinalizeTransaction"));
        if (ActiveSource_) {
            ActiveSource_->Sync();
        }
        if (InputStore_) {
            InputStore_->Sync(transaction);
        }
        OutputStore_->Sync(transaction);
        if (TimerStore_) {
            TimerStore_->Sync(transaction);
        }
        for (const auto& [_, visitor] : KeyVisitors_) {
            visitor->Sync(transaction);
        }
        for (const auto& [sinkId, key, sink] : GetAllSinks()) {
            sink->Sync(transaction);
        }
        StateManager_->Sync(transaction);
    }
    {
        TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Commit"));
        WaitFor(GetTransactionManager()->CommitTransaction(transaction)).ThrowOnError();
    }
    {
        TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("PostCommit"));
        ObserveEpochEventLags(TInstant::Now());
        context->Commit();
        if (ActiveSource_) {
            ActiveSource_->Commit();
        }
        for (const auto& [sinkId, key, sink] : GetAllSinks()) {
            sink->Commit();
        }
        YT_LOG_INFO("Transaction committed");
    }
}

void TUniversalComputationBase::FinishRunIteration()
{
    TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Finish"));
    CurrentRunIterationPromise_.Set();
    CurrentRunIterationPromise_ = {};
    YT_LOG_INFO("Run iteration finished");

    // Graceful shutdown signal from controller (e.g. graceful rebalance):
    // the current iteration's commit is already done, exit the run loop via
    // a typed exception so the controller can distinguish this from a real
    // partition completion in job feedback.
    if (GetDynamicPartitionSpec()->FinishAfterCurrentEpoch) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::GracefulShutdown,
            "Job finished by graceful shutdown signal");
    }
}

TUniversalComputationBase::TCheckOutputLimitsResult TUniversalComputationBase::CheckOutputLimits(
    const TDynamicComputationSpecPtr& dynamicSpec,
    const TUniversalComputationDynamicPartitionSpecPtr& dynamicPartitionSpec) const
{
    NTracing::TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Accounting"));

    TCheckOutputLimitsResult result;
    THashSet<TStreamId> allowedOutputStreams(GetSpec()->OutputStreamIds.size());
    const auto outputStoreCountAndByteSize = OutputStore_->GetCountAndByteSizes();
    const auto streamsAllowedByLimits = GetStreamsWithinLimits(GetContext()->OutputStreamLimitUsageStates);
    for (const auto& streamId : GetSpec()->OutputStreamIds) {
        if (dynamicPartitionSpec && dynamicPartitionSpec->BlockedOutputStreams.contains(streamId)) {
            result.BlockedByController = true;
            YT_LOG_INFO("Output stream is blocked by controller, "
                "probably waiting for finishing previous overlapping partition "
                "(StreamId: %v)",
                streamId);
            continue;
        }
        if (!streamsAllowedByLimits.contains(streamId)) {
            result.OutputBufferOverflow = true;
            YT_LOG_INFO("Output buffer is full for stream (StreamId: %v)",
                streamId);
            continue;
        }
        auto [count, byteSize] = outputStoreCountAndByteSize.at(streamId);
        if (count >= dynamicSpec->OutputStoreCountLimit ||
            byteSize >= dynamicSpec->OutputStoreByteSizeLimit)
        {
            result.OutputStoreOverflow = true;
            YT_LOG_INFO("Output store is full for stream "
                "(StreamId: %v, Count: %v, CountLimit: %v, ByteSize: %v, ByteSizeLimit: %v)",
                streamId,
                count,
                dynamicSpec->OutputStoreCountLimit,
                byteSize,
                dynamicSpec->OutputStoreByteSizeLimit);
            continue;
        }
        allowedOutputStreams.insert(streamId);
    }
    result.AllowedInputStreams = ComputeAllowedInputStreams(allowedOutputStreams, GetSpec());
    return result;
}

void TUniversalComputationBase::WaitForBackoff(
    const TDynamicComputationSpecPtr& dynamicSpec,
    const TCheckOutputLimitsResult& outputLimitsCheckResult,
    bool emptyInput) const
{
    if (outputLimitsCheckResult.OutputStoreOverflow) {
        TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Distribute.OutputStoreOverflow"));
        YT_LOG_INFO("Output store overflow epoch");
        TDelayedExecutor::WaitForDuration(dynamicSpec->EmptyBatchBackoff);
    } else if (outputLimitsCheckResult.OutputBufferOverflow) {
        TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Distribute.OutputBufferOverflow"));
        YT_LOG_INFO("Output buffer overflow epoch");
        TDelayedExecutor::WaitForDuration(dynamicSpec->EmptyBatchBackoff);
    } else if (outputLimitsCheckResult.BlockedByController) {
        TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Distribute.BlockedByController"));
        YT_LOG_INFO("Blocked by controller epoch");
        TDelayedExecutor::WaitForDuration(dynamicSpec->EmptyBatchBackoff);
    } else if (emptyInput) {
        TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Input.Empty"));
        YT_LOG_INFO("Empty epoch");
        TDelayedExecutor::WaitForDuration(dynamicSpec->EmptyBatchBackoff);
    }
}

void TUniversalComputationBase::ValidateTimerStoreLimits(const TDynamicComputationSpecPtr& dynamicSpec) const
{
    NTracing::TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Accounting"));

    if (TimerStore_->GetCount() > dynamicSpec->TimerStoreCountLimit) {
        THROW_ERROR_EXCEPTION("Too much timers in memory: count %v, limit %v",
            TimerStore_->GetCount(),
            dynamicSpec->TimerStoreCountLimit);
    }
    if (TimerStore_->GetByteSize() > dynamicSpec->TimerStoreByteSizeLimit) {
        THROW_ERROR_EXCEPTION("Too much timers in memory: byte size %v, limit %v",
            TimerStore_->GetByteSize(),
            dynamicSpec->TimerStoreByteSizeLimit);
    }
}

ITimeProvider::TGlobalUniqueSeqNo TUniversalComputationBase::GenerateGlobalUniqueSeqNo()
{
    NTracing::TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("GenerateGlobalUniqueSeqNo"));
    return WaitFor(GetTimeProvider()->GenerateGlobalUniqueSeqNo()).ValueOrThrow();
}

void TUniversalComputationBase::InitOutputStoreDistribution(const IComputationRunContextPtr& context, bool allowOutputDuplicates)
{
    YT_VERIFY(!AllowOutputDuplicates_.has_value(), "InitOutputStoreDistribution must not be called twice");
    AllowOutputDuplicates_ = allowOutputDuplicates;

    const bool loadKeyState = GetPartitionState() == EPartitionState::Executing || GetPartitionState() == EPartitionState::Completing;

    auto outputs = WaitFor(OutputStore_->Init(loadKeyState)).ValueOrThrow();
    if (outputs.empty()) {
        return;
    }

    auto iterGuard = StartRunIteration(context);

    const auto dynamicSpec = GetDynamicSpec();

    std::vector<TOutputMessageConstPtr> outputMessages;
    outputMessages.reserve(outputs.size());
    for (auto& [msg, key] : outputs) {
        outputMessages.push_back(std::move(msg));
    }

    // Each tracker enqueues to the init queue so DrainDistributedOutputs can unregister them.
    DistributeOutputMessagesImpl(
        context,
        std::span<const TOutputMessageConstPtr>(outputMessages),
        dynamicSpec,
        /*getKey*/ [&] (size_t i) -> const std::optional<TKey>& {
            return outputs[i].second;
        },
        /*makeTrackerCallback*/ [&] (size_t i) {
            return [pendingOutputs = PendingProcessedOutputs_, msg = outputMessages[i]] () mutable {
                pendingOutputs->PushInit(std::move(msg));
            };
        });

    FinishRunIteration();
}

void TUniversalComputationBase::Run(const IComputationRunContextPtr& context)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);

    YT_LOG_INFO("Starting execution");
    auto initTraceContextGuard = TTraceContextGuard(Tracer_->CreateInitTraceContext());

    DoPrepare(context);

    if (GetPartitionState() == EPartitionState::Executing) {
        DoExecute(context, std::move(initTraceContextGuard));
    } else if (GetPartitionState() == EPartitionState::Interrupting) {
        DoInterrupt(context, std::move(initTraceContextGuard));
    } else if (GetPartitionState() == EPartitionState::Completing) {
        DoComplete(context, std::move(initTraceContextGuard));
    } else {
        YT_LOG_FATAL("Unexpected partition state (PartitionState: %v)", GetPartitionState());
    }
    YT_LOG_INFO("Finished (FinishedPartitionState: %v)", ConvertToYsonString(GetPartitionState(), NYson::EYsonFormat::Text));
}

void TUniversalComputationBase::DoInterrupt(const IComputationRunContextPtr& context, NTracing::TTraceContextGuard&& initTraceContextGuard)
{
    YT_LOG_INFO("Started DoInterrupt");

    bool isFinished = true;
    {
        auto iterGuard = StartRunIteration(context);
        const auto [now, uniqueSeqNo] = GenerateGlobalUniqueSeqNo();
        isFinished = UpdateStatus(/*reportTime*/ now, /*systemWatermark*/ now, BuildInflights());
        FinishRunIteration();
    }

    initTraceContextGuard.Release();
    YT_LOG_INFO("Init completed");

    while (!isFinished) {
        auto iterGuard = StartRunIteration(context);
        auto dynamicSpec = GetDynamicSpec();

        const auto [now, uniqueSeqNo] = GenerateGlobalUniqueSeqNo();

        auto tx = PrepareTransaction(context);
        Commit(context, tx);

        isFinished = UpdateStatus(/*reportTime*/ now, /*systemWatermark*/ now, BuildInflights());
        FinishRunIteration();
        TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Distribute.InterruptedPartitionOutputMessages"));
        TDelayedExecutor::WaitForDuration(dynamicSpec->EmptyBatchBackoff);
    }

    DoCleanup(context, /*eraseOwnedState*/ false);

    YT_LOG_INFO("Completed DoInterrupt");
}

void TUniversalComputationBase::DoComplete(const IComputationRunContextPtr& context, NTracing::TTraceContextGuard&& initTraceContextGuard)
{
    YT_LOG_INFO("Started DoComplete");
    initTraceContextGuard.Release();
    DoCleanup(context, /*eraseOwnedState*/ true);
    YT_LOG_INFO("Completed DoComplete");
}

void TUniversalComputationBase::DoCleanup(const IComputationRunContextPtr& context, bool eraseOwnedState)
{
    YT_LOG_INFO("Started DoCleanup (EraseOwnedState: %v)", eraseOwnedState);

    auto iterGuard = StartRunIteration(context);
    TTraceContextGuard traceGuard(Tracer_->CreateEpochPartTraceContext("Cleanup"));
    auto tableContext = New<NTables::TContext>();
    tableContext->Client = GetRetryableClient()->WithErrorComponent("cleanup");
    tableContext->PipelinePath = GetContext()->PipelinePath;
    tableContext->LoadThroughputThrottler = GetContext()->LoadThroughputThrottler;
    tableContext->Logger = GetContext()->Logger;
    tableContext->Profiler = GetContext()->Profiler.WithPrefix("/tables");
    tableContext->Tag = Format("%v/cleanup", GetComputationId());

    const auto keyStates = New<NTables::TKeyStates>(
        tableContext,
        GetDynamicSpec()->StateManager->TableRequest);
    const auto partitionStates = New<NTables::TPartitionStates>(
        tableContext,
        GetDynamicSpec()->StateManager->TableRequest,
        Format("%v/cleanup", GetComputationId()));
    const auto keyVisitorStates = New<NTables::TKeyVisitorStates>(
        tableContext,
        GetDynamicSpec()->StateManager->TableRequest);
    const auto keysToErase = [&] () -> std::vector<NTables::TKeyStates::TTableKey> {
        if (eraseOwnedState && GetContext()->Partition->SourceKey.has_value()) {
            const auto& key = *GetContext()->Partition->SourceKey;
            return WaitFor(keyStates->ListAll({.ComputationId = GetComputationId(), .ExactKey = key}))
                .ValueOrThrow();
        }
        return {};
    }();
    auto partitionsToErase = WaitFor(partitionStates->ListAll({.PartitionId = GetContext()->Partition->PartitionId})).ValueOrThrow();
    // Wipe key_visitor_states only on completion (eraseOwnedState), when the
    // partition exclusively owns its range; on interrupt the range is handed to
    // overlapping successors that are still writing this coverage.
    std::vector<std::pair<NTables::IKeyVisitorStates::TTableKey, std::optional<NTables::IKeyVisitorStates::TValue>>> keyVisitorMutations;
    if (eraseOwnedState && !GetContext()->Partition->SourceKey.has_value()) {
        const auto keyVisitorRowsToErase = WaitFor(keyVisitorStates->ReadAll({
                .ComputationId = GetComputationId(),
                .LowerKey = GetContext()->Partition->LowerKey.value_or(MinKey()),
                .UpperKey = GetContext()->Partition->UpperKey.value_or(MaxKey()),
                                                   }))
            .ValueOrThrow();
        keyVisitorMutations.reserve(keyVisitorRowsToErase.size());
        for (auto& [tableKey, _] : keyVisitorRowsToErase) {
            keyVisitorMutations.emplace_back(tableKey, std::nullopt);
        }
    }

    {
        auto transaction = PrepareTransaction(context);
        keyStates->Erase(transaction, keysToErase);
        partitionStates->Erase(transaction, partitionsToErase);
        if (!keyVisitorMutations.empty()) {
            keyVisitorStates->Write(transaction, keyVisitorMutations);
        }
        StateManager_->Sync(transaction);
        WaitFor(GetTransactionManager()->CommitTransaction(transaction)).ThrowOnError();
    }

    WaitFor(GetTransactionManager()->Cleanup()).ThrowOnError();

    FinishRunIteration();
    YT_LOG_INFO("Completed DoCleanup");
}

ISinkPtr TUniversalComputationBase::GetOrCreateSink(const TSinkId& sinkId, const std::optional<TKey>& parentKey, const TDynamicComputationSpecPtr& dynamicSpec)
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);

    if (auto sink = GetOrDefault(GetOrDefault(Sinks_, sinkId), parentKey)) {
        return sink;
    }

    auto context = New<TSinkContext>();
    static_cast<TComputationContextBase&>(*context) = *GetContext();
    context->Profiler = context->Profiler.WithPrefix("/sink").WithTag("sink_id", sinkId.Underlying());
    context->StatusProfiler = context->StatusProfiler->WithPrefix(Format("/sinks/%v", sinkId));
    context->Logger = context->Logger.WithTag("SinkId: %v", sinkId.Underlying());
    context->SinkSpec = GetOrCrash(GetSpec()->Sinks, sinkId);
    auto dynamicSinkContext = New<TDynamicSinkContext>();
    dynamicSinkContext->DynamicSinkSpec = GetOrDefault(dynamicSpec->Sinks, sinkId, New<TDynamicSinkSpec>());
    auto sink = TRegistry::Get()->CreateSink(context, dynamicSinkContext);

    const auto stateName = Format("sinks/%v", sinkId);
    const auto initContext = StateManager_->CreateContext();
    if (parentKey) {
        sink->Init(initContext->AsKey(*parentKey)->WithPrefix(stateName));
    } else {
        sink->Init(initContext->AsPartition()->WithPrefix(stateName));
    }
    Sinks_[sinkId][parentKey] = sink;
    return sink;
}

THashMap<std::string, THashSet<TKey>> TUniversalComputationBase::CollectVisitorDrivenJoinerKeys(
    const IInputContextPtr& inputContext) const
{
    if (inputContext->GetVisits().empty()) {
        return {};
    }
    const auto& joiners = StateManager_->GetExternalStateJoiners();
    THashMap<TStreamId, std::vector<std::string>> boundJoinersByStream;
    for (const auto& [streamId, streamSpec] : GetSpec()->KeyVisitorStreams) {
        if (!streamSpec->ExternalNames) {
            continue;
        }
        for (const auto& name : *streamSpec->ExternalNames) {
            auto it = joiners.find(name);
            if (it != joiners.end() && it->second->IsVisitorDriven()) {
                boundJoinersByStream[streamId].push_back(name);
            }
        }
    }
    if (boundJoinersByStream.empty()) {
        return {};
    }
    THashMap<std::string, THashSet<TKey>> result;
    for (const auto& visit : inputContext->GetVisits()) {
        auto it = boundJoinersByStream.find(visit->StreamId);
        if (it == boundJoinersByStream.end()) {
            continue;
        }
        for (const auto& name : it->second) {
            result[name].insert(visit->Key);
        }
    }
    return result;
}

bool TUniversalComputationBase::ComputeHasVisitorDrivenJoiners() const
{
    const auto& joiners = StateManager_->GetExternalStateJoiners();
    for (const auto& [streamId, streamSpec] : GetSpec()->KeyVisitorStreams) {
        if (!streamSpec->ExternalNames) {
            continue;
        }
        for (const auto& name : *streamSpec->ExternalNames) {
            auto it = joiners.find(name);
            if (it != joiners.end() && it->second->IsVisitorDriven()) {
                return true;
            }
        }
    }
    return false;
}

void ValidateKeyVisitorJoinerBindings(const TComputationSpec& spec)
{
    THashMap<std::string, TStreamId> bindingStream;
    for (const auto& [streamId, streamSpec] : spec.KeyVisitorStreams) {
        if (!streamSpec->ExternalNames) {
            continue;
        }
        for (const auto& name : *streamSpec->ExternalNames) {
            auto it = spec.ExternalStateJoiners.find(name);
            if (it == spec.ExternalStateJoiners.end()) {
                continue;
            }
            if (!TRegistry::Get()->IsExternalStateJoinerVisitorDriven(it->second->ExternalStateJoinerClassName)) {
                continue;
            }
            auto [existing, inserted] = bindingStream.emplace(name, streamId);
            THROW_ERROR_EXCEPTION_UNLESS(inserted,
                "Visitor-driven joiner %Qv is referenced by more than one key_visitor stream "
                "(%v and %v); a visitor-driven joiner may be bound to at most one stream",
                name,
                existing->second,
                streamId);
        }
    }
}

void TUniversalComputationBase::ValidateSpec(const TComputationSpec& spec)
{
    ValidateKeyVisitorJoinerBindings(spec);
}

void TUniversalComputationBase::PreloadKeyStates(const IInputContextPtr& inputContext)
{
    if (!StateManager_->HasPreloadCallbacks() && !HasVisitorDrivenJoiners_) {
        return;
    }

    auto visitorDrivenJoinerKeys = CollectVisitorDrivenJoinerKeys(inputContext);

    if (!StateManager_->HasPreloadCallbacks() && visitorDrivenJoinerKeys.empty()) {
        return;
    }

    YT_LOG_DEBUG("Preload key states (MessageCount: %v, TimerCount: %v, VisitCount: %v)",
        inputContext->GetMessages().size(),
        inputContext->GetTimers().size(),
        inputContext->GetVisits().size());
    WaitFor(AllSucceeded(std::vector<TFuture<void>>{
            StateManager_->PreloadKeyStates(inputContext),
            StateManager_->PreloadVisitorDrivenJoiners(visitorDrivenJoinerKeys),
            }))
        .ThrowOnError();
}

std::vector<std::tuple<TSinkId, std::optional<TKey>, ISinkPtr>> TUniversalComputationBase::GetAllSinks() const
{
    YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(GetContext()->SerializedInvoker);
    std::vector<std::tuple<TSinkId, std::optional<TKey>, ISinkPtr>> result;
    for (const auto& [sinkId, sinks] : Sinks_) {
        for (const auto& [parentKey, sink] : sinks) {
            result.push_back(std::tuple(sinkId, parentKey, sink));
        }
    }
    return result;
}

auto TUniversalComputationBase::CreateStreamMessageCounters(const NProfiling::TProfiler& profiler, const TComputationSpecPtr& spec)
    -> THashMap<TStreamId, TStreamMessageCounters>
{
    THashMap<TStreamId, TStreamMessageCounters> counters;
    for (const auto& streamId : Concatenate(
        spec->InputStreamIds,
        GetKeys(spec->SourceStreams),
        GetKeys(spec->TimerStreams),
        GetKeys(spec->KeyVisitorStreams)))
    {
        auto streamProfiler = profiler.WithTag("stream_id", streamId.Underlying());
        counters[streamId] = TStreamMessageCounters{
            .TotalMessages = streamProfiler.Counter("/total"),
            .LateMessages = streamProfiler.Counter("/late"),
        };
    }
    return counters;
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TStreamId> TUniversalComputationBase::GetInputEventLagStreamIds(const TComputationSpecPtr& spec)
{
    std::vector<TStreamId> result;
    for (const auto& streamId : Concatenate(
        spec->InputStreamIds,
        GetKeys(spec->SourceStreams),
        GetKeys(spec->TimerStreams),
        GetKeys(spec->KeyVisitorStreams)))
    {
        result.push_back(streamId);
    }
    return result;
}

std::vector<TStreamId> TUniversalComputationBase::GetOutputEventLagStreamIds(const TComputationSpecPtr& spec)
{
    return {spec->OutputStreamIds.begin(), spec->OutputStreamIds.end()};
}

void TUniversalComputationBase::ObserveEpochEventLags(TInstant commitNow)
{
    InputEventLagObserver_.Flush(commitNow);
    OutputEventLagObserver_.Flush(commitNow);
}

////////////////////////////////////////////////////////////////////////////////

void TUniversalComputationBase::DrainDistributedOutputs(const IComputationRunContextPtr& context)
{
    // Atomically extract both pending deques under a single lock acquisition.
    auto [pending, initPending] = PendingProcessedOutputs_->Extract();

    // OutputStore in-flight set drains via TryUnregister/AsyncUnregister below.
    ProcessDistributedMessages(context, std::move(pending));

    // NOLINTNEXTLINE(bugprone-use-after-move): ProcessDistributedMessages takes an rvalue reference and does not move the deque.
    ClearAsynchronously(std::move(pending));

    if (!initPending.empty()) {
        YT_VERIFY(AllowOutputDuplicates_.has_value());
        const bool allowOutputDuplicates = *AllowOutputDuplicates_;
        std::vector<TOutputMessageConstPtr> initMessages(
            std::make_move_iterator(initPending.begin()),
            std::make_move_iterator(initPending.end()));
        if (allowOutputDuplicates) {
            OutputStore_->AsyncUnregisterBatch(initMessages);
        } else {
            OutputStore_->TryUnregisterBatch(initMessages);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
