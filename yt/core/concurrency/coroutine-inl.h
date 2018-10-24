#pragma once
#ifndef COROUTINE_INL_H_
#error "Direct inclusion of this file is not allowed, include coroutine.h"
// For the sake of sane code completion.
#include "coroutine.h"
#endif
#undef COROUTINE_INL_H_

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

namespace  NDetail {

template <class TCallee, class TCaller, class TArguments, unsigned... Indexes>
void Invoke(
    TCallee& callee,
    TCaller& caller,
    TArguments&& arguments,
    NMpl::TSequence<Indexes...>)
{
    callee.Run(
        caller,
        std::get<Indexes>(std::forward<TArguments>(arguments))...);
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class R, class... TArgs>
TCoroutine<R(TArgs...)>::TCoroutine(TCoroutine<R(TArgs...)>::TCallee&& callee, const EExecutionStackKind stackKind)
    : NDetail::TCoroutineBase(stackKind)
    , Callee_(std::move(callee))
{ }

template <class R, class... TArgs>
template <class... TParams>
const TNullable<R>& TCoroutine<R(TArgs...)>::Run(TParams&& ... params)
{
    static_assert(sizeof...(TParams) == sizeof...(TArgs),
        "TParams<> and TArgs<> have different length");
    Arguments_ = std::make_tuple(std::forward<TParams>(params)...);
    JumpToCoroutine();
    return Result_;
}

template <class R, class... TArgs>
template <class Q>
typename TCoroutine<R(TArgs...)>::TArguments&& TCoroutine<R(TArgs...)>::Yield(Q&& result)
{
    Result_ = std::forward<Q>(result);
    JumpToCaller();
    return std::move(Arguments_);
}

template <class R, class... TArgs>
void TCoroutine<R(TArgs...)>::Invoke()
{
    try {
        NDetail::Invoke(
            Callee_,
            *this,
            std::move(Arguments_),
            typename NMpl::TGenerateSequence<sizeof...(TArgs)>::TType());
        Result_.Reset();
    } catch (...) {
        Result_.Reset();
        throw;
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class... TArgs>
TCoroutine<void(TArgs...)>::TCoroutine(TCoroutine<void(TArgs...)>::TCallee&& callee, const EExecutionStackKind stackKind)
    : NDetail::TCoroutineBase(stackKind)
    , Callee_(std::move(callee))
{ }

template <class... TArgs>
template <class... TParams>
bool TCoroutine<void(TArgs...)>::Run(TParams&& ... params)
{
    static_assert(sizeof...(TParams) == sizeof...(TArgs),
        "TParams<> and TArgs<> have different length");
    Arguments_ = std::make_tuple(std::forward<TParams>(params)...);
    JumpToCoroutine();
    return Result_;
}

template <class... TArgs>
void TCoroutine<void(TArgs...)>::Invoke()
{
    try {
        NDetail::Invoke(
            Callee_,
            *this,
            std::move(Arguments_),
            typename NMpl::TGenerateSequence<sizeof...(TArgs)>::TType());
        Result_ = false;
    } catch (...) {
        Result_ = false;
        throw;
    }
}


template <class... TArgs>
typename TCoroutine<void(TArgs...)>::TArguments&& TCoroutine<void(TArgs...)>::Yield()
{
    Result_ = true;
    JumpToCaller();
    return std::move(Arguments_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
