#pragma once

#include "public.h"
#include "automaton.h"

#include <core/logging/log.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

class TSaveContext
    : public NYT::TStreamSaveContext
{ };

////////////////////////////////////////////////////////////////////////////////

class TLoadContext
    : public NYT::TStreamLoadContext
{
public:
    DEFINE_BYVAL_RW_PROPERTY(int, Version);

public:
    TLoadContext();

};

////////////////////////////////////////////////////////////////////////////////

class TCompositeAutomatonPart
    : public virtual TRefCounted
{
public:
    TCompositeAutomatonPart(
        IHydraManagerPtr hydraManager,
        TCompositeAutomatonPtr automaton);

protected:
    IHydraManagerPtr HydraManager;
    TCompositeAutomaton* Automaton;

    void RegisterSaver(
        int priority,
        const Stroka& name,
        TClosure saver);

    void RegisterLoader(
        const Stroka& name,
        TClosure loader);

    void RegisterSaver(
        int priority,
        const Stroka& name,
        TCallback<void(TSaveContext&)> saver);

    void RegisterLoader(
        const Stroka& name,
        TCallback<void(TLoadContext&)> loader);

    template <class TRequest, class TResponse>
    void RegisterMethod(TCallback<TResponse(const TRequest&)> handler);

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

};

DEFINE_REFCOUNTED_TYPE(TCompositeAutomatonPart)

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ESerializationPriority,
    (Keys)
    (Values)
);

class TCompositeAutomaton
    : public IAutomaton
{
public:
    void RegisterPart(TCompositeAutomatonPart* part);

protected:
    NLog::TLogger Logger;


    TCompositeAutomaton();

    virtual TSaveContext& SaveContext() = 0;
    virtual TLoadContext& LoadContext() = 0;

private:
    friend class TCompositeAutomatonPart;

    struct TSaverInfo
    {
        int Priority;
        Stroka Name;
        TClosure Saver;
        TCompositeAutomatonPart* Part;

        TSaverInfo(
            int priority,
            const Stroka& name,
            TClosure saver,
            TCompositeAutomatonPart* part);
    };

    struct TLoaderInfo
    {
        Stroka Name;
        TClosure Loader;
        TCompositeAutomatonPart* Part;

        TLoaderInfo(
            const Stroka& name,
            TClosure loader,
            TCompositeAutomatonPart* part);
    };

    yhash_map<Stroka, TCallback<void(TMutationContext* context)>> Methods;

    std::vector<TCompositeAutomatonPart*> Parts;

    yhash_map<Stroka, TLoaderInfo> Loaders;
    yhash_map<Stroka, TSaverInfo>  Savers;



    virtual void SaveSnapshot(TOutputStream* output) override;
    virtual void LoadSnapshot(TInputStream* input) override;

    virtual void ApplyMutation(TMutationContext* context) override;

    virtual void Clear() override;

};

DEFINE_REFCOUNTED_TYPE(TCompositeAutomaton)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT

#define COMPOSITE_AUTOMATON_INL_H_
#include "composite_automaton-inl.h"
#undef COMPOSITE_AUTOMATON_INL_H_
