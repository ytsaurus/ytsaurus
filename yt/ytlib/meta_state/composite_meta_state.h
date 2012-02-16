#pragma once

#include "meta_state_manager.h"

#include <ytlib/actions/action.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////
    
class TCompositeMetaState;

class TMetaStatePart
    : public virtual TRefCounted
{
public:
    typedef TIntrusivePtr<TMetaStatePart> TPtr;

    TMetaStatePart(
        IMetaStateManager* metaStateManager,
        TCompositeMetaState* metaState);

protected:
    IMetaStateManager::TPtr MetaStateManager;
    TIntrusivePtr<TCompositeMetaState> MetaState;

    template<class TMessage, class TResult>
    void RegisterMethod(TIntrusivePtr< IParamFunc<const TMessage&, TResult> > changeMethod);

    // TODO: move to inl
    template<class TThis, class TMessage, class TResult>
    void RegisterMethod(
        TThis* this_,
        TResult (TThis::* changeMethod)(const TMessage&))
    {
        RegisterMethod(FromMethod(changeMethod, this_));
    }

    bool IsLeader() const;
    bool IsFolllower() const;
    bool IsRecovery() const;

    virtual void Clear();

    virtual void OnStartLeading();
    virtual void OnLeaderRecoveryComplete();
    virtual void OnStopLeading();

private:
    friend class TCompositeMetaState;
    typedef TMetaStatePart TThis;

    template<class TMessage, class TResult>
    void MethodThunk(
        const TRef& changeData,
        typename IParamFunc<const TMessage&, TResult>::TPtr changeMethod);

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
    typedef TIntrusivePtr<TCompositeMetaState> TPtr;

    typedef IParamAction<TOutputStream*> TSaver;
    typedef IParamAction<TInputStream*> TLoader;

    void RegisterPart(TMetaStatePart::TPtr part);
    void RegisterLoader(const Stroka& name, TLoader::TPtr loader);
    void RegisterSaver(const Stroka& name, TSaver::TPtr saver, ESavePhase phase);

private:
    friend class TMetaStatePart;

    typedef yhash_map<Stroka, IParamAction<const TRef&>::TPtr> TMethodMap;
    TMethodMap Methods;

    yvector<TMetaStatePart::TPtr> Parts;

    typedef yhash_map<Stroka, TLoader::TPtr> TLoaderMap;
    typedef yhash_map<Stroka, TPair<TSaver::TPtr, ESavePhase> > TSaverMap;

    TLoaderMap Loaders;
    TSaverMap Savers;

    virtual void Save(TOutputStream* output);
    virtual void Load(TInputStream* input);

    virtual void ApplyChange(const TRef& changeData);

    virtual void Clear();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT

#define COMPOSITE_META_STATE_INL_H_
#include "composite_meta_state-inl.h"
#undef COMPOSITE_META_STATE_INL_H_
