#pragma once

#include "public.h"
#include "automaton.h"

#include <yt/core/logging/log.h>

#include <yt/core/misc/checkpointable_stream.h>
#include <yt/core/misc/serialize.h>

#include <yt/core/profiling/profiler.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

constexpr int NullObjectSerializationIndex = -1;
constexpr int DestroyedObjectSerializationIndex = -2;

struct TEntitySerializationKey
{
    TEntitySerializationKey()
        : Index(NullObjectSerializationIndex)
    { }

    explicit TEntitySerializationKey(int index)
        : Index(index)
    { }

    static TEntitySerializationKey NullObjectKey()
    {
        return TEntitySerializationKey();
    }

    static TEntitySerializationKey DestroyedObjectKey()
    {
        return TEntitySerializationKey(DestroyedObjectSerializationIndex);
    }

#define DEFINE_OPERATOR(op) \
    bool operator op (TEntitySerializationKey other) const \
    { \
        return Index op other.Index; \
    }

    DEFINE_OPERATOR(==)
    DEFINE_OPERATOR(!=)
    DEFINE_OPERATOR(<)
    DEFINE_OPERATOR(<=)
    DEFINE_OPERATOR(>)
    DEFINE_OPERATOR(>=)
#undef DEFINE_OPERATOR

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

    int Index;
};

////////////////////////////////////////////////////////////////////////////////

class TSaveContext
    : public NYT::TStreamSaveContext
{
public:
    DEFINE_BYVAL_RW_PROPERTY(ICheckpointableOutputStream*, CheckpointableOutput);

public:
    virtual ~TSaveContext() = default;

    TEntitySerializationKey GenerateSerializationKey();

private:
    int SerializationKeyIndex_ = 0;

};

////////////////////////////////////////////////////////////////////////////////

class TLoadContext
    : public NYT::TStreamLoadContext
{
public:
    DEFINE_BYVAL_RW_PROPERTY(ICheckpointableInputStream*, CheckpointableInput);

public:
    virtual ~TLoadContext() = default;

    TEntitySerializationKey RegisterEntity(TEntityBase* entity);

    template <class T>
    T* GetEntity(TEntitySerializationKey key) const;

private:
    std::vector<TEntityBase*> Entities_;

};

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
    TCompositeAutomatonPart(
        IHydraManagerPtr hydraManager,
        TCompositeAutomatonPtr automaton,
        IInvokerPtr automatonInvoker);

protected:
    IHydraManager* const HydraManager_;
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
        TCallback<void(TRequest*)> callback);
    template <class TRpcRequest, class TRpcResponse, class THandlerRequest, class THandlerResponse>
    void RegisterMethod(
        TCallback<void(const TIntrusivePtr<NRpc::TTypedServiceContext<TRpcRequest, TRpcResponse>>&, THandlerRequest*, THandlerResponse*)> callback);

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

private:
    typedef TCompositeAutomatonPart TThis;
    friend class TCompositeAutomaton;

    void RegisterMethod(
        const TString& name,
        TCallback<void(TMutationContext*)> callback);

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

    virtual TFuture<void> SaveSnapshot(NConcurrency::IAsyncOutputStreamPtr writer) override;
    virtual void LoadSnapshot(NConcurrency::IAsyncZeroCopyInputStreamPtr reader) override;

    virtual void ApplyMutation(TMutationContext* context) override;

    virtual void Clear() override;
    virtual void SetZeroState() override;

protected:
    bool SerializationDumpEnabled_ = false;

    const NLogging::TLogger Logger;
    NProfiling::TProfiler Profiler;

protected:
    explicit TCompositeAutomaton(
        IInvokerPtr asyncSnapshotInvoker,
        TCellId cellId,
        const NProfiling::TTagIdList& profilingTagIds = {});

    void RegisterPart(TCompositeAutomatonPartPtr part);

    virtual std::unique_ptr<TSaveContext> CreateSaveContext(
        ICheckpointableOutputStream* output) = 0;
    virtual std::unique_ptr<TLoadContext> CreateLoadContext(
        ICheckpointableInputStream* input) = 0;

    void InitSaveContext(
        TSaveContext& context,
        ICheckpointableOutputStream* output);
    void InitLoadContext(
        TLoadContext& context,
        ICheckpointableInputStream* input);

private:
    typedef TCompositeAutomaton TThis;
    friend class TCompositeAutomatonPart;

    const IInvokerPtr AsyncSnapshotInvoker_;

    struct TMethodDescriptor
    {
        TCallback<void(TMutationContext* context)> Callback;
        NProfiling::TMonotonicCounter CumulativeTimeCounter;
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

    IHydraManager* HydraManager_ = nullptr;

    std::vector<TWeakPtr<TCompositeAutomatonPart>> Parts_;

    THashMap<TString, TMethodDescriptor> MethodNameToDescriptor_;

    THashMap<TString, TLoaderDescriptor> PartNameToLoaderDescriptor_;

    THashSet<TString> SaverPartNames_;
    std::vector<TSyncSaverDescriptor> SyncSavers_;
    std::vector<TAsyncSaverDescriptor> AsyncSavers_;

    NProfiling::TMonotonicCounter MutationCounter_ = {"/mutation_count"};
    NProfiling::TAggregateGauge MutationWaitTimeCounter_ = {"/mutation_wait_time"};

private:
    void DoSaveSnapshot(
        NConcurrency::IAsyncOutputStreamPtr writer,
        NConcurrency::ESyncStreamAdapterStrategy strategy,
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

    bool IsRecovery() const;
};

DEFINE_REFCOUNTED_TYPE(TCompositeAutomaton)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra

#define COMPOSITE_AUTOMATON_INL_H_
#include "composite_automaton-inl.h"
#undef COMPOSITE_AUTOMATON_INL_H_
