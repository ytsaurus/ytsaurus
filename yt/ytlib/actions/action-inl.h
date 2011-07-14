#ifndef ACTION_INL_H_
#error "Direct inclusion of this file is not allowed, include action.h"
#endif

#include "async_result.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TResult>
struct TAsyncFuncTraits
{
    typedef TIntrusivePtr< TAsyncResult<TResult> > TAsync;

    static void InnerThunk(
        TIntrusivePtr< IFunc<TResult> > func,
        TAsync result)
    {
        result->Set(func->Do());
    }

    static TAsync OuterThunk(
        TIntrusivePtr< IFunc<TResult> > func,
        TIntrusivePtr<IInvoker> invoker)
    {
        TAsync result = new TAsyncResult<TResult>();
        invoker->Invoke(FromMethod(&InnerThunk, func, result));
        return result;
    }
};

template <class TResult>
struct TAsyncFuncTraits< TIntrusivePtr< TAsyncResult<TResult> > >
{
    typedef TIntrusivePtr< TAsyncResult<TResult> > TAsync;

    static void InnerThunk(
        TIntrusivePtr< IFunc<TAsync> > func,
        TAsync result)
    {
        func->Do()->Subscribe(FromMethod(
            &TAsyncResult<TResult>::Set, result));
    }

    static TAsync OuterThunk(
        TIntrusivePtr< IFunc<TAsync> > func,
        TIntrusivePtr<IInvoker> invoker)
    {
        TAsync result = new TAsyncResult<TResult>();
        invoker->Invoke(FromMethod(&InnerThunk, func, result));
        return result;
    }
};

template<class TResult>
TIntrusivePtr< IFunc <typename TAsyncTraits<TResult>::TAsync> >
IFunc<TResult>::AsyncVia(TIntrusivePtr<IInvoker> invoker)
{
    return FromMethod(
        &TAsyncFuncTraits<TResult>::OuterThunk,
        TPtr(this),
        invoker);
}

////////////////////////////////////////////////////////////////////////////////

template<class TParam>
IAction::TPtr IParamAction<TParam>::Bind(TParam param)
{
    return FromMethod(
        &IParamAction<TParam>::Do,
        TPtr(this),
        param);
}

template<class TParam>
void ParamActionViaThunk(
    TParam param,
    typename IParamAction<TParam>::TPtr paramAction,
    TIntrusivePtr< IInvoker > invoker)
{
    invoker->Invoke(paramAction->Bind(param));
}

template<class TParam>
typename IParamAction<TParam>::TPtr IParamAction<TParam>::Via(
    TIntrusivePtr< IInvoker > invoker)
{
    return FromMethod(
        &ParamActionViaThunk<TParam>,
        TPtr(this),
        invoker);
}

template<class TParam>
void ParamActionViaThunk(TParam param, IAction::TPtr action)
{
    UNUSED(param);
    action->Do();
}

////////////////////////////////////////////////////////////////////////////////

template <class TParam, class TResult>
struct TAsyncParamFuncTraits
{
    typedef TIntrusivePtr< TAsyncResult<TResult> > TAsync;

    static void InnerThunk(
        TParam param,
        TIntrusivePtr< IParamFunc<TParam, TResult> > func,
        TAsync result)
    {
        result->Set(func->Do(param));
    }

    static TAsync OuterThunk(
        TParam param,
        TIntrusivePtr< IParamFunc<TParam, TResult> > func,
        TIntrusivePtr<IInvoker> invoker)
    {
        TAsync result = new TAsyncResult<TResult>();
        invoker->Invoke(FromMethod(&InnerThunk, param, func, result));
        return result;
    }
};

template <class TParam, class TResult>
struct TAsyncParamFuncTraits< TParam, TIntrusivePtr< TAsyncResult<TResult> > >
{
    typedef TIntrusivePtr< TAsyncResult<TResult> > TAsync;

    static void InnerThunk(
        TParam param,
        TIntrusivePtr< IParamFunc<TParam, TAsync> > func,
        TAsync result)
    {
        func->Do(param)->Subscribe(FromMethod(
            &TAsyncResult<TResult>::Set,
            result));
    }

    static TAsync OuterThunk(
        TParam param,
        TIntrusivePtr< IParamFunc<TParam, TAsync> > func,
        TIntrusivePtr<IInvoker> invoker)
    {
        TAsync result = new TAsyncResult<TResult>();
        invoker->Invoke(FromMethod(&InnerThunk, param, func, result));
        return result;
    }
};

template<class TParam, class TResult>
TIntrusivePtr< IParamFunc<TParam, typename TAsyncTraits<TResult>::TAsync> >
IParamFunc<TParam, TResult>::AsyncVia(TIntrusivePtr<IInvoker> invoker)
{
    return FromMethod(
        &TAsyncParamFuncTraits<TParam, TResult>::OuterThunk,
        TPtr(this),
        invoker);
}

////////////////////////////////////////////////////////////////////////////////

template<class TParam>
typename IParamAction<TParam>::TPtr IAction::ToParamAction()
{
    return FromMethod(&ParamActionViaThunk<TParam>, TPtr(this));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
