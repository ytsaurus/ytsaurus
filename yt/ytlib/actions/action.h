#pragma once

#include "invoker.h"

#include "../misc/common.h"
#include "../misc/new.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TVoid
{ };

template<class T>
class TFuture;

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
    typedef TIntrusivePtr< TFuture<TResult> > TAsync;
};

template <class TResult>
struct TAsyncTraits< TIntrusivePtr< TFuture<TResult> > >
{
    typedef TIntrusivePtr< TFuture<TResult> > TAsync;
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

} // namespace NYT

#define ACTION_INL_H_
#include "action-inl.h"
#undef ACTION_INL_H_
