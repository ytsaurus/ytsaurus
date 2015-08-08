#pragma once

#include "public.h"
#include "automaton.h"

#include <core/misc/serialize.h>
#include <core/misc/checkpointable_stream.h>

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

struct TEntitySerializationKey
{
    TEntitySerializationKey()
        : Index(-1)
    { }

    explicit TEntitySerializationKey(int index)
        : Index(index)
    { }

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
    TSaveContext();

    TEntitySerializationKey GenerateSerializationKey();

    void Reset();

private:
    int SerializationKeyIndex_;

};

////////////////////////////////////////////////////////////////////////////////

class TLoadContext
    : public NYT::TStreamLoadContext
{
public:
    DEFINE_BYVAL_RW_PROPERTY(ICheckpointableInputStream*, CheckpointableInput);
    DEFINE_BYVAL_RW_PROPERTY(int, Version);

public:
    TLoadContext();

    void Reset();

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
    const IHydraManagerPtr HydraManager_;
    TCompositeAutomaton* const Automaton_;
    const IInvokerPtr AutomatonInvoker_;

    IInvokerPtr EpochAutomatonInvoker_;


    void RegisterSaver(
        ESyncSerializationPriority priority,
        const Stroka& name,
        TCallback<void()> callback);

    void RegisterSaver(
        ESyncSerializationPriority priority,
        const Stroka& name,
        TCallback<void(TSaveContext&)> callback);

    void RegisterSaver(
        EAsyncSerializationPriority priority,
        const Stroka& name,
        TCallback<TCallback<void()>()> callback);

    void RegisterSaver(
        EAsyncSerializationPriority priority,
        const Stroka& name,
        TCallback<TCallback<void(TSaveContext&)>()> callback);

    void RegisterLoader(
        const Stroka& name,
        TCallback<void()> callback);

    void RegisterLoader(
        const Stroka& name,
        TCallback<void(TLoadContext&)> callback);

    template <class TRequest, class TResponse>
    void RegisterMethod(TCallback<TResponse(const TRequest&)> callback);

    bool IsLeader() const;
    bool IsFollower() const;
    bool IsRecovery() const;

    virtual bool ValidateSnapshotVersion(int version);
    virtual int GetCurrentSnapshotVersion();

    virtual void Clear();

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

    template <class TRequest, class TResponse>
    struct TThunkTraits;

    void RegisterMethod(
        const Stroka& name,
        TCallback<void(TMutationContext*)> callback);


    void StartEpoch();
    void StopEpoch();

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

protected:
    bool SerializationDumpEnabled_ = false;

    NLogging::TLogger Logger;
    NProfiling::TProfiler Profiler;

    explicit TCompositeAutomaton(IInvokerPtr asyncSnapshotInvoker);

    void RegisterPart(TCompositeAutomatonPart* part);

    virtual TSaveContext& SaveContext() = 0;
    virtual TLoadContext& LoadContext() = 0;

private:
    typedef TCompositeAutomaton TThis;
    friend class TCompositeAutomatonPart;

    const IInvokerPtr AsyncSnapshotInvoker_;

    struct TMethodDescriptor
    {
        TCallback<void(TMutationContext* context)> Callback;
        NProfiling::TTagId TagId;
    };

    struct TSaverDescriptorBase
    {
        Stroka Name;
        TCompositeAutomatonPart* Part;
    };

    struct TSyncSaverDescriptor
        : public TSaverDescriptorBase
    {
        ESyncSerializationPriority Priority;
        TCallback<void()> Callback;
    };

    struct TAsyncSaverDescriptor
        : public TSaverDescriptorBase
    {
        EAsyncSerializationPriority Priority;
        TCallback<TCallback<void()>()> Callback;
    };

    struct TLoaderDescriptor
    {
        Stroka Name;
        TCallback<void()> Callback;
        TCompositeAutomatonPart* Part = nullptr;
    };

    std::vector<TCompositeAutomatonPart*> Parts_;

    yhash_map<Stroka, TMethodDescriptor> MethodNameToDescriptor_;

    yhash_map<Stroka, TLoaderDescriptor> PartNameToLoaderDescriptor_;

    yhash_set<Stroka> SaverPartNames_;
    std::vector<TSyncSaverDescriptor> SyncSavers_;
    std::vector<TAsyncSaverDescriptor> AsyncSavers_;


    void DoSaveSnapshot(
        NConcurrency::IAsyncOutputStreamPtr writer,
        NConcurrency::ESyncStreamAdapterStrategy strategy,
        const std::function<void()>& callback);

    void DoLoadSnapshot(
        NConcurrency::IAsyncZeroCopyInputStreamPtr reader,
        const std::function<void()>& callback);

    void WritePartHeader(const TSaverDescriptorBase& descriptor);

    void OnRecoveryStarted();
    void OnRecoveryComplete();

};

DEFINE_REFCOUNTED_TYPE(TCompositeAutomaton)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT

#define COMPOSITE_AUTOMATON_INL_H_
#include "composite_automaton-inl.h"
#undef COMPOSITE_AUTOMATON_INL_H_
