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
    
class TCompositeMetaState
    : public IMetaState 
{
public:
    typedef TIntrusivePtr<TCompositeMetaState> TPtr;
   

    struct TSaveContext
    {
        TSaveContext(TOutputStream* output, IInvoker::TPtr invoker);

        TOutputStream* Output;
        IInvoker::TPtr Invoker;
    };

    typedef IParamFunc<const TSaveContext&, TFuture<TVoid>::TPtr> TSaver;
    typedef IParamAction<TInputStream*> TLoader;

    void RegisterPart(TMetaStatePart::TPtr part);
    void RegisterLoader(const Stroka& name, TLoader::TPtr loader);
    void RegisterSaver(const Stroka& name, TSaver::TPtr saver);

private:
    friend class TMetaStatePart;

    typedef yhash_map<Stroka, IParamAction<const TRef&>::TPtr> TMethodMap;
    TMethodMap Methods;

    yvector<TMetaStatePart::TPtr> Parts;

    typedef yhash_map<Stroka, TLoader::TPtr> TLoaderMap;
    typedef yhash_map<Stroka, TSaver::TPtr> TSaverMap;

    TLoaderMap Loaders;
    TSaverMap Savers;

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
