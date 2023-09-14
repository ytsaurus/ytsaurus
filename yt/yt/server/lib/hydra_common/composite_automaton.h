#pragma once

#include "automaton.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/test_framework/testing_tag.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/library/ytprof/api/api.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESyncSerializationPriority,
    (Keys)
    (Values)
);

DEFINE_ENUM(EAsyncSerializationPriority,
    (Default)
);

class TCompositeAutomatonPart
    : public virtual TRefCounted
{
public:
    TCompositeAutomatonPart(TTestingTag);

    TCompositeAutomatonPart(
        ISimpleHydraManagerPtr hydraManager,
        TCompositeAutomatonPtr automaton,
        IInvokerPtr automatonInvoker);

protected:
    ISimpleHydraManager* const HydraManager_;
    TCompositeAutomaton* const Automaton_;
    const IInvokerPtr AutomatonInvoker_;

    IInvokerPtr EpochAutomatonInvoker_;


    void RegisterSaver(
        ESyncSerializationPriority priority,
        const TString& name,
        TCallback<void(TSaveContext&)> callback);

    template <class TContext>
    void RegisterSaver(
        ESyncSerializationPriority priority,
        const TString& name,
        TCallback<void(TContext&)> callback);

    void RegisterSaver(
        EAsyncSerializationPriority priority,
        const TString& name,
        TCallback<TCallback<void(TSaveContext&)>()> callback);

    template <class TContext>
    void RegisterSaver(
        EAsyncSerializationPriority priority,
        const TString& name,
        TCallback<TCallback<void(TContext&)>()> callback);

    void RegisterLoader(
        const TString& name,
        TCallback<void(TLoadContext&)> callback);

    template <class TContext>
    void RegisterLoader(
        const TString& name,
        TCallback<void(TContext&)> callback);

    template <class TRequest>
    void RegisterMethod(
        TCallback<void(TRequest*)> callback,
        const std::vector<TString>& aliases = {});
    template <class TRpcRequest, class TRpcResponse, class THandlerRequest, class THandlerResponse>
    void RegisterMethod(
        TCallback<void(const TIntrusivePtr<NRpc::TTypedServiceContext<TRpcRequest, TRpcResponse>>&, THandlerRequest*, THandlerResponse*)> callback,
        const std::vector<TString>& aliases = {});

    bool IsLeader() const;
    bool IsFollower() const;
    bool IsRecovery() const;

    virtual bool ValidateSnapshotVersion(int version);
    virtual int GetCurrentSnapshotVersion();

    virtual void Clear();
    virtual void SetZeroState();

    virtual void OnBeforeSnapshotLoaded();
    virtual void OnAfterSnapshotLoaded();

    virtual void OnStartLeading();
    virtual void OnLeaderRecoveryComplete();
    virtual void OnLeaderActive();
    virtual void OnStopLeading();

    virtual void OnStartFollowing();
    virtual void OnFollowerRecoveryComplete();
    virtual void OnStopFollowing();

    virtual void OnRecoveryStarted();
    virtual void OnRecoveryComplete();

    virtual void CheckInvariants();

private:
    using TThis = TCompositeAutomatonPart;
    friend class TCompositeAutomaton;

    void StartEpoch();
    void StopEpoch();

    void LogHandlerError(const TError& error);
};

DEFINE_REFCOUNTED_TYPE(TCompositeAutomatonPart)

////////////////////////////////////////////////////////////////////////////////

class TCompositeAutomaton
    : public IAutomaton
{
public:
    void SetSerializationDumpEnabled(bool value);
    void SetEnableTotalWriteCountReport(bool enableTotalWriteCountReport);
    void SetLowerWriteCountDumpLimit(i64 lowerLimit);
    void SetUpperWriteCountDumpLimit(i64 upperLimit);

    void SetSnapshotValidationOptions(const TSnapshotValidationOptions& options) override;

    TFuture<void> SaveSnapshot(NConcurrency::IAsyncOutputStreamPtr writer) override;
    void LoadSnapshot(NConcurrency::IAsyncZeroCopyInputStreamPtr reader) override;
    void PrepareState() override;

    void ApplyMutation(TMutationContext* context) override;

    void Clear() override;
    void SetZeroState() override;

    void RememberReign(TReign reign);
    EFinalRecoveryAction GetFinalRecoveryAction() override;

    void CheckInvariants() override;

    using TWaitTimeObserver = std::function<void(TDuration)>;
    void RegisterWaitTimeObserver(TWaitTimeObserver waitTimeObserver);

protected:
    bool SerializationDumpEnabled_ = false;
    bool EnableTotalWriteCountReport_ = false;
    i64 LowerWriteCountDumpLimit_ = 0;
    i64 UpperWriteCountDumpLimit_ = std::numeric_limits<i64>::max();

    const NLogging::TLogger Logger;
    NProfiling::TProfiler Profiler_;

    TWaitTimeObserver WaitTimeObserver_;

protected:
    TCompositeAutomaton(
        IInvokerPtr asyncSnapshotInvoker,
        TCellId cellId);

    void RegisterPart(TCompositeAutomatonPartPtr part);

    virtual std::unique_ptr<TSaveContext> CreateSaveContext(
        ICheckpointableOutputStream* output) = 0;
    virtual std::unique_ptr<TLoadContext> CreateLoadContext(
        ICheckpointableInputStream* input) = 0;

    void SetupLoadContext(TLoadContext* context);

    template <class TRequest>
    void RegisterMethod(
        TCallback<void(TRequest*)> callback,
        const std::vector<TString>& aliases = {});
    template <class TRpcRequest, class TRpcResponse, class THandlerRequest, class THandlerResponse>
    void RegisterMethod(
        TCallback<void(const TIntrusivePtr<NRpc::TTypedServiceContext<TRpcRequest, TRpcResponse>>&, THandlerRequest*, THandlerResponse*)> callback,
        const std::vector<TString>& aliases = {});

    void RegisterMethod(
        const TString& name,
        TCallback<void(TMutationContext*)> callback);

private:
    using TThis = TCompositeAutomaton;
    friend class TCompositeAutomatonPart;

    const IInvokerPtr AsyncSnapshotInvoker_;

    struct TMethodDescriptor
    {
        TCallback<void(TMutationContext* context)> Callback;
        NProfiling::TTimeCounter CumulativeTimeCounter;
        NProfiling::TTimeCounter CumulativeExecuteTimeCounter;
        NProfiling::TTimeCounter CumulativeDeserializeTimeCounter;
        NProfiling::TCounter MutationCounter;
        NProfiling::TGauge RequestSizeCounter;

        NYTProf::TProfilerTagPtr CpuProfilerTag;
    };

    struct TSaverDescriptorBase
    {
        TString Name;
        int SnapshotVersion;
    };

    struct TSyncSaverDescriptor
        : public TSaverDescriptorBase
    {
        ESyncSerializationPriority Priority;
        TCallback<void(TSaveContext&)> Callback;
    };

    struct TAsyncSaverDescriptor
        : public TSaverDescriptorBase
    {
        EAsyncSerializationPriority Priority;
        TCallback<TCallback<void(TSaveContext&)>()> Callback;
    };

    struct TLoaderDescriptor
    {
        TString Name;
        TCallback<void(TLoadContext&)> Callback;
    };

    ISimpleHydraManager* HydraManager_ = nullptr;

    std::vector<TWeakPtr<TCompositeAutomatonPart>> Parts_;

    THashMap<TString, TMethodDescriptor> MethodNameToDescriptor_;

    THashMap<TString, TLoaderDescriptor> PartNameToLoaderDescriptor_;

    THashSet<TString> SaverPartNames_;
    std::vector<TSyncSaverDescriptor> SyncSavers_;
    std::vector<TAsyncSaverDescriptor> AsyncSavers_;

    EFinalRecoveryAction FinalRecoveryAction_ = EFinalRecoveryAction::None;

    NProfiling::TEventTimer MutationWaitTimer_;

private:
    void DoSaveSnapshot(
        NConcurrency::IAsyncOutputStreamPtr writer,
        NConcurrency::EWaitForStrategy strategy,
        const std::function<void(TSaveContext&)>& callback);
    void DoLoadSnapshot(
        NConcurrency::IAsyncZeroCopyInputStreamPtr reader,
        const std::function<void(TLoadContext&)>& callback);

    void WritePartHeader(
        TSaveContext& context,
        const TSaverDescriptorBase& descriptor);

    void OnRecoveryStarted();
    void OnRecoveryComplete();

    TMethodDescriptor* GetMethodDescriptor(const TString& mutationType);
    std::vector<TCompositeAutomatonPartPtr> GetParts();
    void LogHandlerError(const TError& error);
    void DeserializeRequestAndProfile(
        google::protobuf::MessageLite* requestMessage,
        TRef requestData,
        TMethodDescriptor* methodDescriptor);

    bool IsRecovery() const;

    void HydraResetStateHash(NProto::TReqResetStateHash* request);
};

DEFINE_REFCOUNTED_TYPE(TCompositeAutomaton)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra

#define COMPOSITE_AUTOMATON_INL_H_
#include "composite_automaton-inl.h"
#undef COMPOSITE_AUTOMATON_INL_H_
