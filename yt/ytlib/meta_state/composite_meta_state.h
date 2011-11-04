#pragma once

#include "meta_state_manager.h"

#include "../actions/action.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////
    
class TCompositeMetaState;

class TMetaStatePart
    : public virtual TRefCountedBase
{
public:
    typedef TIntrusivePtr<TMetaStatePart> TPtr;

    TMetaStatePart(
        TMetaStateManager::TPtr metaStateManager,
        TIntrusivePtr<TCompositeMetaState> metaState);

protected:
    TMetaStateManager::TPtr MetaStateManager;
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

    virtual Stroka GetPartName() const = 0;
    virtual TFuture<TVoid>::TPtr Save(TOutputStream* output, IInvoker::TPtr invoker);
    virtual void Load(TInputStream* input);
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

class TCompositeMetaState
    : public IMetaState 
{
public:
    typedef TIntrusivePtr<TCompositeMetaState> TPtr;

    void RegisterPart(TMetaStatePart::TPtr part);

private:
    friend class TMetaStatePart;

    typedef yhash_map<Stroka, IParamAction<const TRef&>::TPtr> TMethodMap;
    TMethodMap Methods;

    typedef yhash_map<Stroka, TMetaStatePart::TPtr> TPartMap;
    TPartMap Parts;

    virtual TFuture<TVoid>::TPtr Save(TOutputStream* output, IInvoker::TPtr invoker);
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
