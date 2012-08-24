#pragma once

#include "public.h"
#include "meta_state.h"

#include <ytlib/rpc/service.h>
#include <ytlib/meta_state/meta_state_manager.pb.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////
    
class TMetaStatePart
    : public virtual TRefCounted
{
public:
    TMetaStatePart(
        IMetaStateManagerPtr metaStateManager,
        TCompositeMetaStatePtr metaState);

protected:
    IMetaStateManagerPtr MetaStateManager;
    TCompositeMetaStatePtr MetaState;

    template <class TRequest, class TResponse>
    void RegisterMethod(TCallback<TResponse(const TRequest&)> handler);
    
    template <class TRequest, class TResponse>
    bool HasMethod(TCallback<TResponse(const TRequest&)> handler);

    bool IsLeader() const;
    bool IsFolllower() const;
    bool IsRecovery() const;

    virtual void Clear();

    virtual void OnStartLeading();
    virtual void OnLeaderRecoveryComplete();
    virtual void OnStopLeading();

    virtual void OnStartFollowing();
    virtual void OnFollowerRecoveryComplete();
    virtual void OnStopFollowing();

    virtual void OnStartRecovery();
    virtual void OnStopRecovery();

    void NoOperation(const NProto::TReqNoOperation& request);

private:
    typedef TMetaStatePart TThis;
    friend class TCompositeMetaState;

    template <class TRequest, class TResponse>
    struct TThunkTraits;
};

////////////////////////////////////////////////////////////////////////////////
    
DECLARE_ENUM(ESavePhase,
    (Keys)
    (Values)
);

////////////////////////////////////////////////////////////////////////////////

class TCompositeMetaState
    : public IMetaState 
{
public:
    typedef TCallback<void(TOutputStream*)> TSaver;
    typedef TCallback<void(TInputStream*)> TLoader;

    void RegisterPart(TMetaStatePartPtr part);
    void RegisterLoader(const Stroka& name, TLoader loader);
    void RegisterSaver(const Stroka& name, TSaver saver, ESavePhase phase);

private:
    friend class TMetaStatePart;

    struct TSaverInfo
    {
        Stroka Name;
        TSaver Saver;
        ESavePhase Phase;

        TSaverInfo(const Stroka& name, TSaver saver, ESavePhase phase);
    };

    typedef yhash_map< Stroka, TCallback<void(TMutationContext* context)> > TMethodMap;
    TMethodMap Methods;

    std::vector<TMetaStatePartPtr> Parts;

    typedef yhash_map< Stroka, TLoader > TLoaderMap;
    typedef yhash_map< Stroka, TSaverInfo> TSaverMap;

    TLoaderMap Loaders;
    TSaverMap Savers;

    virtual void Save(TOutputStream* output);
    virtual void Load(TInputStream* input);

    virtual void ApplyMutation(TMutationContext* context) throw();

    virtual void Clear();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT

#define COMPOSITE_META_STATE_INL_H_
#include "composite_meta_state-inl.h"
#undef COMPOSITE_META_STATE_INL_H_
