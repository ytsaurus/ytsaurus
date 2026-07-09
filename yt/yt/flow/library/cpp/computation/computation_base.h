#pragma once

#include "public.h"

#include "job_state/job_init_context.h"
#include "job_state/state_manager.h"
#include "key_visitor.h"
#include "universal_controller.h"

#include <yt/yt/flow/library/cpp/common/computation.h>
#include <yt/yt/flow/library/cpp/common/external_state_manager.h>
#include <yt/yt/flow/library/cpp/common/input_context.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/lag.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/output_collector.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/schema.h>
#include <yt/yt/flow/library/cpp/common/seq_no_provider.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>
#include <yt/yt/flow/library/cpp/common/timer.h>
#include <yt/yt/flow/library/cpp/common/visit.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/flow/library/cpp/distributed_throttler/public.h>

#include <yt/yt/flow/library/cpp/tables/transaction_manager.h>

#include <yt/yt/flow/lib/serializer/serializer.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/actions/bind.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>
#include <library/cpp/yt/threading/atomic_object.h>
#include <library/cpp/yt/threading/spin_lock.h>

#include <library/cpp/iterator/concatenate.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! A default abstract base class for all computations.
/*!
 *  This base provides the following features out-of-the-box:
 *  1) Properly tagged logger.
 *  2) Version management.
 */

class TComputationBase
    : public IComputation
{
public:
    void Reconfigure(TDynamicComputationContextPtr dynamicContext) final;
    void UpdateWatermarkState(TWatermarkStatePtr watermarkState) final;
    void SetInputTraverse(THashMap<TStreamId, TStreamTraverseDataPtr> inputStreams) final;

protected:
    const NLogging::TLogger Logger;

    TComputationBase(
        TComputationContextPtr context,
        TDynamicComputationContextPtr dynamicContext);

    const TComputationContextPtr& GetContext() const;
    const IUniqueSeqNoProviderPtr& GetUniqueSeqNoProvider() const;
    const TComputationSpecPtr& GetSpec() const;
    TDynamicComputationSpecPtr GetDynamicSpec() const;
    TDynamicComputationContextPtr GetDynamicContext() const;

    IComputation::TParametersPtr GetParametersBase() const final;
    IComputation::TDynamicParametersPtr GetDynamicParametersBase() const final;
    IComputation::TDynamicPartitionSpecPtr GetDynamicPartitionSpecBase() const final;

    i64 GetSpecGeneration() const;

    //! Returns schema of key of all input messages. In fact it is equal to GetSpec()->GroupBySchema.
    const NTableClient::TTableSchemaPtr& GetKeySchema() const;
    TWatermarkStatePtr GetWatermarkState() const;
    const THashMap<TStreamId, TStreamTraverseDataPtr>& GetInputTraverse() const;

    EPartitionState GetPartitionState() const;
    TJobId GetJobId() const;
    const TComputationId& GetComputationId() const;
    TPartitionId GetPartitionId() const;

    IRetryableClientPtr GetRetryableClient() const;
    NTables::TTransactionManagerPtr GetTransactionManager() const;

    TMessageBuilder MakeOutputMessageBuilder(std::optional<TStreamId> streamId = {}, TMessageBuilder::TInitFunction init = {}) const;

    //! Converts a message to the output stream schema and sets its StreamId.
    //! If |streamId| is not provided, uses the single output stream (throws if there is not exactly one).
    TMessage ConvertToOutputMessage(const TMessage& message, std::optional<TStreamId> streamId = {}) const;

    TTimer MakeTimer(
        const TKey& key,
        const TStreamId& streamId,
        const TSystemTimestamp& triggerTimestamp,
        const TSystemTimestamp& eventTimestamp) const;

    TNodeTraverseDataPtr GetNodeTraverse();

    //! Applies pending states at the start of a run iteration.
    //! Must be called from JobSerializedInvoker_.
    void ApplyPendingStates();

    //! Registers an callback to be called when the dynamic computation spec changes.
    //! Callback called at the start of each RunIteration only if the dynamic spec changes.
    void SubscribeOnReconfigure(NYT::TCallback<void()> callback, EWatchReconfigure watchReconfigureMask = EWatchReconfigure::DynamicComputationSpec) final;

    /*!
     *  Registers an object to be reconfigured when the dynamic computation spec changes.
     *
     *  When the specification is reconfigured, the `Reconfigure` method of the provided object is called with the updated dynamic field value.
     *  The dynamic field value can be obtained in two ways:
     *  - via a pointer to the dynamic parameters field, or
     *  - by invoking a callable that extracts the value from the dynamic parameters.
     *
     *  Reconfiguration occurs at the start of each RunIteration only if the dynamic spec changes.
     *
     *  \param derived Pointer to an instance of the derived computation class.
     *  \param object Object that needs to be reconfigured when the dynamic field changes.
     *  \param dynamicField Pointer to the dynamic parameters field or a callable for extracting the parameters.
     *  \param watchReconfigureMask Mask of fields to watch for changes.
     */
    template <class TDerived, class TObject, class TDynamicField>
    static void RegisterForReconfiguration(TDerived* derived, TObject object, TDynamicField dynamicField, EWatchReconfigure watchReconfigureMask = EWatchReconfigure::DynamicComputationSpec)
    {
        derived->SubscribeOnReconfigure(BIND(
            [weakDerived = MakeWeak(derived), object, dynamicField] () {
                if (auto derived = weakDerived.Lock()) {
                    if (auto&& params = std::invoke(dynamicField, derived->GetDynamicParameters())) {
                        object->Reconfigure(params);
                    }
                }
            }),
            watchReconfigureMask);
    }

    bool UpdateTraverse(
        TSystemTimestamp reportTime,
        TSystemTimestamp systemWatermark,
        const THashMap<TStreamId, TInflightStreamTraverseDataPtr>& inflights);

    //! Returns the distributed throttler client for the given id.
    //! Throws if |throttlerId| is not in the dynamic pipeline spec's
    //! `throttlers` — this is a configuration error.
    NConcurrency::IThroughputThrottlerPtr GetThrottler(const TThrottlerId& throttlerId);

protected:
    const NDistributedThrottler::IDistributedThrottlerFactoryPtr& GetThrottlerFactory() const
    {
        return ThrottlerFactory_;
    }

private:
    const TComputationContextPtr Context_;
    const IComputation::TParametersPtr Parameters_;
    const std::vector<TStreamId> TopologicalStreamOrder_;
    const IRetryableClientPtr RetryableClient_;
    const IUniqueSeqNoProviderPtr UniqueSeqNoProvider_;
    const NTables::TTransactionManagerPtr TransactionManager_;


    // Written from ControlSerializedInvoker_, read from JobSerializedInvoker_ (in ApplyPendingStates).
    TAtomicIntrusivePtr<TWatermarkState> PendingWatermarkState_;
    NThreading::TAtomicObject<std::optional<THashMap<TStreamId, TStreamTraverseDataPtr>>> PendingInputStreams_;
    TAtomicIntrusivePtr<TDynamicComputationContext> PendingDynamicContext_;

    // Only from JobSerializedInvoker_.
    TWatermarkStatePtr WatermarkState_;
    std::optional<THashMap<TStreamId, TStreamTraverseDataPtr>> InputStreams_;
    TDynamicComputationContextPtr DynamicContext_;
    IComputation::TDynamicParametersPtr DynamicParameters_;
    IComputation::TDynamicPartitionSpecPtr DynamicPartitionSpec_;

    // Declared after DynamicContext_ so its ctor init can read
    // DynamicContext_->Throttlers.
    const NDistributedThrottler::IDistributedThrottlerFactoryPtr ThrottlerFactory_;

    // Callbacks called from ApplyPendingStates when spec changes.
    // Only from JobSerializedInvoker_.
    std::vector<std::pair<NYT::TCallback<void()>, EWatchReconfigure>> ReconfigureCallbacks_;

    // Read from ControlSerializedInvoker_, written from JobSerializedInvoker_.
    TAtomicIntrusivePtr<TNodeTraverseData> NodeTraverse_;

private:
    NTables::TTransactionManagerPtr CreateTransactionManager() const;

    static std::vector<TStreamId> BuildTopologicalStreamOrder(TComputationSpecPtr spec);
};

////////////////////////////////////////////////////////////////////////////////

class TRootOutputCollector
    : public TRefCounted
{
public:
    struct TTransformResult
    {
        std::vector<TMessage> OutputMessages;
        //! Per-output-message distribute flag, kept in lockstep with OutputMessages.
        std::vector<bool> OutputMessagesDistribute;
        std::vector<TMessageParentsConstPtr> OutputMessagesParentMessageIds;
        std::vector<TTimer> OutputTimers;
        std::vector<TMessageParentsConstPtr> OutputTimersParentMessageIds;
    };

    TRootOutputCollector(TComputationSpecPtr spec, IMetaSetterPtr metaSetter, bool supportsDistribute = false);

    [[nodiscard]] IOutputCollectorPtr SetParents(
        const std::vector<TInputMessageConstPtr>& messages,
        const std::vector<TInputTimerConstPtr>& timers,
        const std::vector<TInputVisitConstPtr>& visits);

    void AddMessage(TMessage&& message, const TMessageParentsConstPtr& parents, bool distribute);
    void AddTimer(TTimer&& timer, const TMessageParentsConstPtr& parents);

    TTransformResult CollectResult();

private:
    const TComputationSpecPtr Spec_;
    const IMetaSetterPtr MetaSetter_;
    //! Whether AddMessage() accepts |distribute| = false. Only source computations
    //! support it; other computations throw if asked to drop a message via this flag.
    const bool SupportsDistribute_;
    TTransformResult Result_;
};

DEFINE_REFCOUNTED_TYPE(TRootOutputCollector);

////////////////////////////////////////////////////////////////////////////////

class TOutputCollector
    : public IOutputCollector
{
public:
    TOutputCollector(TRootOutputCollectorPtr rootCollector, const TMessageParentsConstPtr& parents);

    [[nodiscard]] IOutputCollectorPtr SetParents(
        const std::vector<TInputMessageConstPtr>& messages,
        const std::vector<TInputTimerConstPtr>& timers,
        const std::vector<TInputVisitConstPtr>& visits) override;
    void AddMessage(TMessage&& message, bool distribute) override;
    void AddTimer(TSystemTimestamp triggerTimestamp, std::optional<TSystemTimestamp> eventTimestamp = {}) override;
    void AddTimer(const TStreamId& streamId, TSystemTimestamp triggerTimestamp, std::optional<TSystemTimestamp> eventTimestamp = {}) override;
    void AddTimer(TTimer&& timer) override;

private:
    const TRootOutputCollectorPtr RootCollector_;
    const TMessageParentsConstPtr Parents_;
};

////////////////////////////////////////////////////////////////////////////////

struct TOutputStoreStreamOrchidState
    : public NYTree::TYsonStruct
{
    i64 UsedCount{};
    i64 LimitCount{};

    i64 UsedBytes{};
    i64 LimitBytes{};

    REGISTER_YSON_STRUCT(TOutputStoreStreamOrchidState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOutputStoreStreamOrchidState);

////////////////////////////////////////////////////////////////////////////////

struct TUniversalComputationOrchidState
    : public TComputationOrchidState
{
    std::string PartitionDescription;
    THashMap<std::string, TDuration> EpochPartsWallTime;
    THashMap<TStreamId, TOutputStoreStreamOrchidStatePtr> OutputStore;

    REGISTER_YSON_STRUCT(TUniversalComputationOrchidState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUniversalComputationOrchidState);

////////////////////////////////////////////////////////////////////////////////

struct TUniversalComputationDynamicPartitionSpec
    : public TComputationBase::TDynamicPartitionSpec
{
public:
    NYTree::IMapNodePtr ActiveSource;
    THashSet<TStreamId> BlockedOutputStreams;
    //! If set, the computation should finish after completing the current epoch
    //! and report success. Used for graceful partition migration between workers.
    bool FinishAfterCurrentEpoch{};

    REGISTER_YSON_STRUCT(TUniversalComputationDynamicPartitionSpec);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUniversalComputationDynamicPartitionSpec);

////////////////////////////////////////////////////////////////////////////////

struct TUniversalComputationPartitionStatus
    : public NYTree::TYsonStruct
{
    std::optional<NYTree::IMapNodePtr> ActiveSourceStatus;

    REGISTER_YSON_STRUCT(TUniversalComputationPartitionStatus);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUniversalComputationPartitionStatus);

////////////////////////////////////////////////////////////////////////////////

// Shared state for tracker callbacks: holds two pending deques and a Finished flag.
// Callbacks capture a strong pointer and push under the spinlock; if Finished is set
// (set in the destructor of TUniversalComputationBase) they do nothing, avoiding
// any weak-pointer lock overhead.
class TPendingDistributedOutputs
    : public TRefCounted
{
public:
    using TDeque = std::deque<TOutputMessageConstPtr>;

    void PushNormal(TOutputMessageConstPtr&& message);
    void PushInit(TOutputMessageConstPtr&& message);

    // Atomically extracts both deques and returns them as a pair.
    // If finish=true, also sets Finished=true to prevent further pushes.
    std::pair<TDeque, TDeque> Extract(bool finish = false);

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    bool Finished_ = false;
    TDeque Normal_;
    TDeque Init_;
};

////////////////////////////////////////////////////////////////////////////////

//! Throws if any visitor-driven joiner is listed in the ``external_names`` of more than one
//! key-visitor stream. Such a joiner holds a single forward-sweep cursor and must be driven
//! by at most one stream. Visitor-driven-ness comes from the registry trait, so unregistered
//! classes are skipped (reported by #TRegistry::ValidateComputationSpec separately).
void ValidateKeyVisitorJoinerBindings(const TComputationSpec& spec);

////////////////////////////////////////////////////////////////////////////////

class TUniversalComputationBase
    : public TComputationBase
{
public:
    using TComputationController = TUniversalComputationController;

    YT_FLOW_EXTEND_DYNAMIC_PARTITION_SPEC(TUniversalComputationDynamicPartitionSpec);

    YT_FLOW_EXTEND_SPEC_VALIDATION(ValidateSpec);

    struct TCheckOutputLimitsResult
    {
        THashSet<TStreamId> AllowedInputStreams;

        // If AllowedInputStreams.empty() at least one of flags is true.
        bool OutputBufferOverflow = false;
        bool OutputStoreOverflow = false;
        bool BlockedByController = false;

        // TODO: Highlight critical output stream (the one that is latest in stream DAG).
        // TStreamId CriticalOutputStreamId;
    };

public:
    TUniversalComputationBase(
        TComputationContextPtr context,
        TDynamicComputationContextPtr dynamicContext);
    ~TUniversalComputationBase() override;

    TComputationOrchidStatePtr GetOrchidState() override;
    TComputationStatusPtr GetStatus() override;

    bool UpdateStatus(
        TSystemTimestamp reportTime,
        TSystemTimestamp systemWatermark,
        const THashMap<TStreamId, TInflightStreamTraverseDataPtr>& inflights);

protected:
    //! Override to add extra entries to InputLimits reported via GetStatus().
    //! Called from UpdateStatus() on the serialized invoker.
    //! The returned map is merged into the base input limits (input_buffer_bytes, etc.).
    virtual THashMap<std::string, THashMap<TStreamId, TJobEntityLimitStatus>> GetExtraInputLimits();

    TWatermarkStatePtr GetEpochWatermarkState() const;
    TSystemTimestamp GetCurrentTimestamp() const;
    TSystemTimestamp GetInputEventWatermark() const;
    TSystemTimestamp GetEpochInputEventWatermark() const;
    TSystemTimestamp GetEventWatermark(const TStreamId& streamId) const;
    TSystemTimestamp GetEpochEventWatermark(const TStreamId& streamId) const;
    TSystemTimestamp GetSystemWatermark(const TStreamId& streamId) const;
    TSystemTimestamp GetEpochSystemWatermark(const TStreamId& streamId) const;
    TSystemTimestamp GetWatermark(const TStreamId& streamId, ETimeType timeType) const;
    TSystemTimestamp GetEpochWatermark(const TStreamId& streamId, ETimeType timeType) const;

    THashMap<TStreamId, TInflightStreamTraverseDataPtr> BuildInflights() const;

    void RegisterInputBeforeProcessing(
        const std::vector<TInputMessageConstPtr>& inputMessages,
        const std::vector<TInputTimerConstPtr>& inputTimers,
        const std::vector<TInputVisitConstPtr>& inputVisits,
        const THashMap<TStreamId, TSystemTimestamp>& watermarkOverrides = {});

    //! Blocks until the throttlers picked by `InputRowsThrottlerId` and
    //! `InputBytesThrottlerId` grant quota for the batch. No-op if neither
    //! id is set or the batch is empty. Must run on the job serialized
    //! invoker; wrapped in the "Input.Throttle" trace part.
    void ThrottleInputBatch(
        const std::vector<TInputMessageConstPtr>& messages,
        const std::vector<TInputTimerConstPtr>& timers,
        const std::vector<TInputVisitConstPtr>& visits);

    // Batch variant: registers all messages at once, taking locks once for the whole batch.
    void RegisterOutputMessages(
        const IComputationRunContextPtr& context,
        std::span<const TOutputMessageConstPtr> messages,
        const std::optional<TKey>& parentKey,
        const TDynamicComputationSpecPtr& dynamicSpec);

    template <class TCallback>
    void SubscribeRunIterationStart(TCallback callback)
    {
        auto guard = Guard(Lock_);
        RunIterationStartPromise_.ToFuture().Subscribe(callback);
    }

    template <class TCallback>
    void SubscribeBeforeNextCommitInIteration(TCallback callback)
    {
        auto guard = Guard(Lock_);
        BeforeCommitInIterationPromise_.ToFuture().Subscribe(callback);
    }

    template <class TCallback>
    void SubscribeRunIterationFinish(TCallback callback)
    {
        auto guard = Guard(Lock_);
        RunIterationFinishPromise_.ToFuture().Subscribe(callback);
    }

    struct TRunIterationGuard
    {
        NTracing::TTraceContextPtr TraceContext;
        NTracing::TTraceContextGuard TraceContextGuard;
    };

    TRunIterationGuard StartRunIteration(const IComputationRunContextPtr& context);
    IRetryableTransactionPtr PrepareTransaction(const IComputationRunContextPtr& context);
    void Commit(IComputationRunContextPtr context, IRetryableTransactionPtr transaction);
    void FinishRunIteration();

    TCheckOutputLimitsResult CheckOutputLimits(
        const TDynamicComputationSpecPtr& dynamicSpec,
        const TUniversalComputationDynamicPartitionSpecPtr& dynamicPartitionSpec) const;
    void WaitForBackoff(
        const TDynamicComputationSpecPtr& dynamicSpec,
        const TCheckOutputLimitsResult& outputLimitsCheckResult,
        bool emptyInput) const;
    void ValidateTimerStoreLimits(const TDynamicComputationSpecPtr& dynamicSpec) const;

    IUniqueSeqNoProvider::TResult GenerateSeqNo();

    template <class... T>
    void ClearAsynchronously(T&&... args) const
    {
        static_assert((std::is_rvalue_reference_v<T&&> && ...), "All arguments must be rvalue");
        GetContext()->PoolInvoker->Invoke(BIND([object = std::tuple<T...>{std::move(args)...}] () mutable {
            object = {};
        }));
    }

    void InitOutputStoreDistribution(const IComputationRunContextPtr& context, bool allowOutputDuplicates);

    void Run(const IComputationRunContextPtr& context) final;

    virtual void DoPrepare(const IComputationRunContextPtr& context) = 0;
    virtual void DoExecute(const IComputationRunContextPtr& context, NTracing::TTraceContextGuard&& initTraceContextGuard) = 0;
    void DoInterrupt(const IComputationRunContextPtr& context, NTracing::TTraceContextGuard&& initTraceContextGuard);
    void DoComplete(const IComputationRunContextPtr& context, NTracing::TTraceContextGuard&& initTraceContextGuard);
    void DoCleanup(const IComputationRunContextPtr& context, bool eraseOwnedState);

    ISinkPtr GetOrCreateSink(const TSinkId& sinkId, const std::optional<TKey>& parentKey, const TDynamicComputationSpecPtr& dynamicSpec);
    std::vector<std::tuple<TSinkId, std::optional<TKey>, ISinkPtr>> GetAllSinks() const;

    void PreloadKeyStates(const IInputContextPtr& inputContext);

    //! Maps each visitor-driven joiner (bound to a key-visitor stream via ``external_names``)
    //! to the keys of the visits that stream delivered in |inputContext|.
    THashMap<std::string, THashSet<TKey>> CollectVisitorDrivenJoinerKeys(
        const IInputContextPtr& inputContext) const;

    TYsonMessagePtr ConvertToYsonMessage(const TInputMessageConstPtr& message) const
    {
        return ConvertToYsonMessage(*message);
    }

    TYsonMessagePtr ConvertToYsonMessage(const TMessage& message) const
    {
        const auto spec = GetContext()->StreamSpecStorage->GetSpec(message.StreamId);
        if (!spec->ClassName) {
            THROW_ERROR_EXCEPTION("Impossible to convert message to yson message due to undefined \"class_name\"")
                << TErrorAttribute("stream_id", message.StreamId);
        }
        auto ysonMessage = TRegistry::Get()->CreateYsonMessage(*spec->ClassName);
        ::NYT::NFlow::ConvertToYsonMessage(message, ysonMessage);
        return ysonMessage;
    }

    TMessage ConvertToMessage(const TYsonMessagePtr& ysonMessage) const
    {
        auto streamId = ysonMessage->Meta->StreamId;
        if (streamId.Underlying().empty()) {
            if (GetSpec()->OutputStreamIds.size() == 1) {
                streamId = *GetSpec()->OutputStreamIds.begin();
            } else {
                THROW_ERROR_EXCEPTION("Impossible to guess output stream");
            }
        }
        auto spec = GetContext()->StreamSpecStorage->GetSpec(streamId);
        if (!spec->ClassName) {
            THROW_ERROR_EXCEPTION("Impossible to convert yson message to message due to undefined \"class_name\"")
                << TErrorAttribute("stream_id", streamId);
        }
        TRegistry::Get()->ValidateYsonMessageType(*spec->ClassName, ysonMessage);
        auto message = ::NYT::NFlow::ConvertToMessage(ysonMessage, spec->Schema);
        message.StreamId = streamId;
        return message;
    }

    template <class T>
    TIntrusivePtr<T> ConvertToYsonKey(const TKey& key) const
    {
        return NYsonSerializer::Deserialize<T>(key.Underlying(), GetKeySchema());
    }

    virtual void ProcessDistributedMessages(const IComputationRunContextPtr& context, std::deque<TOutputMessageConstPtr>&& messages) = 0;

protected:
    const TJobStateManagerPtr StateManager_;
    const bool HasVisitorDrivenJoiners_;
    const std::optional<TStreamId> ActiveSourceStreamId_;
    const ISourcePtr ActiveSource_;

    THashMap<TSinkId, THashMap<std::optional<TKey>, ISinkPtr>> Sinks_;

    const IInputStorePtr InputStore_;
    const ITimerStorePtr TimerStore_;
    const IOutputStorePtr OutputStore_;
    //! KeyVisitor objects per visit-stream id; empty when the partition is not Executing
    //! or when no key_visitor_streams entries are configured.
    const THashMap<TStreamId, TKeyVisitorPtr> KeyVisitors_;
    const IComputationTracerPtr Tracer_;
    const IEventTimestampAssignerPtr EventTimestampAssigner_;

private:
    struct TStreamMessageCounters
    {
        NProfiling::TCounter TotalMessages;
        NProfiling::TCounter LateMessages;
    };

    const TInstant StartTime_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, LimitsLock_);
    THashMap<std::string, THashMap<TStreamId, TJobEntityLimitStatus>> InputLimits_;
    THashMap<std::string, THashMap<TStreamId, TJobEntityLimitStatus>> OutputLimits_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    i64 RunIteration_ = -1;
    TPromise<void> RunIterationStartPromise_;
    TPromise<void> BeforeCommitInIterationPromise_;
    TPromise<void> RunIterationFinishPromise_;
    TPromise<void> CurrentRunIterationPromise_;

    NProfiling::TEventTimer EpochTimer_;
    NProfiling::TCounter EpochCounter_;
    THashMap<TStreamId, TStreamMessageCounters> StreamMessageCounters_;

    TStreamEventLagObserver InputEventLagObserver_;
    TStreamEventLagObserver OutputEventLagObserver_;

    TIntrusivePtr<TPendingDistributedOutputs> PendingProcessedOutputs_;
    std::optional<bool> AllowOutputDuplicates_;

private:
    std::optional<TStreamId> CreateActiveSourceStreamId();
    ISourcePtr CreateActiveSource();
    THashMap<TSinkId, ISinkPtr> CreateSinks();
    IInputStorePtr CreateInputStore();
    ITimerStorePtr CreateTimerStore();
    IOutputStorePtr CreateOutputStore();
    THashMap<TStreamId, TKeyVisitorPtr> CreateKeyVisitors() const;
    TJobStateManagerPtr CreateStateManager() const;
    bool ComputeHasVisitorDrivenJoiners() const;
    static void ValidateSpec(const TComputationSpec& spec);

    static THashMap<TStreamId, TStreamMessageCounters> CreateStreamMessageCounters(const NProfiling::TProfiler& profiler, const TComputationSpecPtr& spec);

    static std::vector<TStreamId> GetInputEventLagStreamIds(const TComputationSpecPtr& spec);
    static std::vector<TStreamId> GetOutputEventLagStreamIds(const TComputationSpecPtr& spec);

    void ObserveEpochEventLags(TInstant commitNow);

    // Common implementation for RegisterOutputMessages and InitOutputStoreDistribution.
    // Iterates over |messages|, distributes each to the appropriate sink, registers
    // with context, and activates all trackers.
    //   getKey(i)                  -> const std::optional<TKey>&  (used for GetOrCreateSink)
    //   makeTrackerCallback(i, cookie) -> callable()              (stored in TDistributingTracker)
    template <class TGetKey, class TMakeTrackerCallback>
    void DistributeOutputMessagesImpl(
        const IComputationRunContextPtr& context,
        std::span<const TOutputMessageConstPtr> messages,
        const TDynamicComputationSpecPtr& dynamicSpec,
        TGetKey&& getKey,
        TMakeTrackerCallback&& makeTrackerCallback);

    void DrainDistributedOutputs(const IComputationRunContextPtr& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
