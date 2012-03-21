#pragma once

#include "common.h"
#include "invoker.h"
#include "bind.h"
#include "callback.h"

#include <ytlib/misc/new.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TVoid
{ };

template <class T>
class TFuture;

struct IAction;

template <class TResult>
struct IFunc;

template <class TParam>
struct IParamAction;

template <class TParam, class TResult>
struct IParamFunc;

////////////////////////////////////////////////////////////////////////////////

class TCancelableContext;

struct IAction
    : public virtual TIntrinsicRefCounted
{
    typedef TIntrusivePtr<IAction> TPtr;

    virtual void Do() = 0;

    TPtr Via(TIntrusivePtr<IInvoker> invoker);
    TPtr Via(TIntrusivePtr<IInvoker> invoker, TIntrusivePtr<TCancelableContext> context);

    template <class TParam>
    typename IParamAction<TParam>::TPtr ToParamAction();

    TCallback<void()> ToCallback()
    {
        return Bind(&IAction::Do, MakeStrong(this));
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TResult>
struct TAsyncTraits
{
    typedef TIntrusivePtr< TFuture<TResult> > TAsync;
};

template <class TResult>
struct TAsyncTraits< TIntrusivePtr< TFuture<TResult> > >
{
    typedef TIntrusivePtr< TFuture<TResult> > TAsync;
};

////////////////////////////////////////////////////////////////////////////////

template <class TResult>
struct IFunc
    : public virtual TIntrinsicRefCounted
{
    typedef TIntrusivePtr< IFunc<TResult> > TPtr;

    virtual TResult Do() = 0;

    TIntrusivePtr< IFunc<typename TAsyncTraits<TResult>::TAsync> >
        AsyncVia(TIntrusivePtr<IInvoker> invoker);
};

////////////////////////////////////////////////////////////////////////////////

template <class TParam>
struct IParamAction
    : public virtual TIntrinsicRefCounted
{
    typedef TIntrusivePtr< IParamAction<TParam> > TPtr;

    virtual void Do(TParam param) = 0;

    IAction::TPtr Bind(TParam param);

    TPtr Via(TIntrusivePtr<IInvoker> invoker);

    TCallback<void(TParam)> ToCallback()
    {
        return Bind(&IParamAction::Do, MakeStrong(this));
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TParam, class TResult>
struct IParamFunc
    : public virtual TIntrinsicRefCounted
{
    typedef TIntrusivePtr< IParamFunc<TParam, TResult> > TPtr;

    virtual TResult Do(TParam param) = 0;

    typename IFunc<TResult>::TPtr Bind(TParam param);

    TIntrusivePtr< IParamFunc<TParam, typename TAsyncTraits<TResult>::TAsync> >
        AsyncVia(TIntrusivePtr<IInvoker> invoker);
};

////////////////////////////////////////////////////////////////////////////////

// A bunch of helpers for constructing delegates from functors.

template <class TFunctor, class TResult>
struct TFunctorActionTraits
{
    typedef IFunc<TResult> TDelegate;

    static TResult Thunk(const TFunctor& functor)
    {
        return functor();
    }
};

template <class TFunctor>
struct TFunctorActionTraits<TFunctor, void>
{
    typedef IAction TDelegate;

    static void Thunk(const TFunctor& functor)
    {
        functor();
    }
};

template <class TFunctor, class TParam, class TResult>
struct TFunctorFuncTraits
{
    typedef IParamFunc<TParam, TResult> TDelegate;

    static TResult Thunk(TParam param, const TFunctor& functor)
    {
        return functor(param);
    }
};

template <class TFunctor, class TParam>
struct TFunctorFuncTraits<TFunctor, TParam, void>
{
    typedef IParamAction<TParam> TDelegate;

    static void Thunk(TParam param, const TFunctor& functor)
    {
        functor(param);
    }
};

template <class TFunctor, class TOp>
struct TFunctorTraits
{ };

template <class TFunctor, class TResult>
struct TFunctorTraits<TFunctor, TResult (TFunctor::*)() const>
{
    static typename TFunctorActionTraits<TFunctor, TResult>::TDelegate::TPtr Construct(const TFunctor& functor)
    {
        return FromMethod(
            &TFunctorActionTraits<TFunctor, TResult>::Thunk,
            functor);
    }
};

template <class TFunctor, class TParam, class TResult>
struct TFunctorTraits<TFunctor, TResult (TFunctor::*)(TParam) const>
{
    static typename TFunctorFuncTraits<TFunctor, TParam, TResult>::TDelegate::TPtr Construct(const TFunctor& functor)
    {
        return FromMethod(
            &TFunctorFuncTraits<TFunctor, TParam, TResult>::Thunk,
            functor);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ACTION_INL_H_
#include "action-inl.h"
#undef ACTION_INL_H_
