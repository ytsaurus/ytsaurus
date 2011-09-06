#pragma once

#include "invoker.h"

#include "../misc/common.h"
#include "../misc/ptr.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TVoid
{ };

template<class T>
class TAsyncResult;

struct IAction;

template<class TResult>
struct IFunc;

template<class TParam>
struct IParamAction;

template<class TParam, class TResult>
struct IParamFunc;

////////////////////////////////////////////////////////////////////////////////

struct IAction
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IAction> TPtr;

    virtual void Do() = 0;

    TPtr Via(TIntrusivePtr<IInvoker> invoker);

    template<class TParam>
    typename IParamAction<TParam>::TPtr ToParamAction();
};

////////////////////////////////////////////////////////////////////////////////

template <class TResult>
struct TAsyncTraits
{
    typedef TIntrusivePtr< TAsyncResult<TResult> > TAsync;
};

template <class TResult>
struct TAsyncTraits< TIntrusivePtr< TAsyncResult<TResult> > >
{
    typedef TIntrusivePtr< TAsyncResult<TResult> > TAsync;
};

////////////////////////////////////////////////////////////////////////////////

template<class TResult>
struct IFunc
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr< IFunc<TResult> > TPtr;

    virtual TResult Do() = 0;

    TIntrusivePtr< IFunc<typename TAsyncTraits<TResult>::TAsync> >
        AsyncVia(TIntrusivePtr<IInvoker> invoker);
};

////////////////////////////////////////////////////////////////////////////////

template<class TParam>
struct IParamAction
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr< IParamAction<TParam> > TPtr;

    virtual void Do(TParam param) = 0;

    IAction::TPtr Bind(TParam param);

    TPtr Via(TIntrusivePtr<IInvoker> invoker);
};

////////////////////////////////////////////////////////////////////////////////

template<class TParam, class TResult>
struct IParamFunc
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr< IParamFunc<TParam, TResult> > TPtr;

    virtual TResult Do(TParam param) = 0;

    typename IFunc<TResult>::TPtr Bind(TParam param);

    TIntrusivePtr< IParamFunc<TParam, typename TAsyncTraits<TResult>::TAsync> >
        AsyncVia(TIntrusivePtr<IInvoker> invoker);
};

////////////////////////////////////////////////////////////////////////////////

// TODO: move to signal.h
template<class T>
class TSignalBase
{
public:
    void Subscribe(typename T::TPtr action)
    {
        Actions.push_back(action);
    }

    bool Unsubscribe(typename T::TPtr action)
    {
        auto it = Find(Actions, action);
        if (it == Actions.end())
            return false;
        Actions.erase(it);
        return true;
    }

protected:
    yvector<typename T::TPtr> Actions;

};

////////////////////////////////////////////////////////////////////////////////

class TSignal
    : public TSignalBase<IAction>
{
public:
    void Fire()
    {
        yvector<IAction::TPtr> actions(this->Actions);
        for (auto it = actions.begin();
             it != actions.end();
             ++it)
        {
            (*it)->Do();
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

template<class TParam>
class TParamSignal
    : public TSignalBase< IParamAction<TParam> >
{
public:
    void Fire(const TParam& arg)
    {
        typename TParamSignal::TActions actions(TParamSignal::Actions);
        for (auto it = actions.begin();
            it != actions.end();
            ++it)
        {
            (*it)->Do(arg);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ACTION_INL_H_
#include "action-inl.h"
#undef ACTION_INL_H_
