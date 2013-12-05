#pragma once

#include "public.h"
#include "fiber.h"

#include <core/misc/nullable.h>

namespace NYT {
namespace NConcurrency {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

class TCoroutineBase
{
protected:
    TCoroutineBase();

    TCoroutineBase(const TCoroutineBase&) = delete;
    TCoroutineBase(TCoroutineBase&&) = default;

    virtual ~TCoroutineBase();

    virtual void Trampoline() = 0;

    TFiberPtr Fiber;

public:
    EFiberState GetState() const
    {
        return Fiber->GetState();
    }

};

template <unsigned...>
struct TSequence { };

template <unsigned N, unsigned... Indexes>
struct TGenerateSequence : TGenerateSequence<N - 1, N - 1, Indexes...> { };

template <unsigned... Indexes>
struct TGenerateSequence<0, Indexes...> {
    typedef TSequence<Indexes...> TType;
};

template <class TCallee, class TCaller, class TArguments, unsigned... Indexes>
void Invoke(
    TCallee& Callee,
    TCaller& Caller,
    TArguments&& Arguments,
    TSequence<Indexes...>)
{
    Callee.Run(
        Caller,
        std::get<Indexes>(std::forward<TArguments>(Arguments))...);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

template <class R, class... TArgs>
class TCoroutine<R(TArgs...)>
    : public NDetail::TCoroutineBase
{
public:
    typedef R (FunctionalSignature)(TArgs...);
    typedef void (CoroutineSignature)(TCoroutine&, TArgs...);

    typedef TCallback<CoroutineSignature> TCallee;
    typedef std::tuple<TArgs...> TArguments;

    TCoroutine() = default;
    TCoroutine(TCoroutine&& other) = default;

    TCoroutine(TCallee&& callee)
        : NDetail::TCoroutineBase()
        , Callee(std::move(callee))
    { }

    void Reset(TCallee callee)
    {
        Fiber->Reset();
        Callee = std::move(callee);
    }

    template <class... TParams>
    const TNullable<R>& Run(TParams&&... params)
    {
        static_assert(sizeof...(TParams) == sizeof...(TArgs),
            "TParams<> and TArgs<> have different length");
        Arguments = std::make_tuple(std::forward<TParams>(params)...);
        Fiber->Run();
        return Result;
    }

    template <class Q>
    TArguments&& Yield(Q&& result)
    {
        Result = std::forward<Q>(result);
        Fiber->Yield();
        return std::move(Arguments);
    }

private:
    virtual void Trampoline() override
    {
        try {
            NDetail::Invoke(
                Callee,
                *this,
                std::move(Arguments),
                typename NDetail::TGenerateSequence<sizeof...(TArgs)>::TType());
            Result.Reset();
        } catch (...) {
            Result.Reset();
            throw;
        }
    }

private:
    TCallee Callee;
    TArguments Arguments;
    TNullable<R> Result;

};

template <class... TArgs>
class TCoroutine<void(TArgs...)>
    : public NDetail::TCoroutineBase
{
public:
    typedef void (FunctionalSignature)(TArgs...);
    typedef void (CoroutineSignature)(TCoroutine&, TArgs...);

    typedef TCallback<CoroutineSignature> TCallee;
    typedef std::tuple<TArgs...> TArguments;

    TCoroutine() = default;
    TCoroutine(TCoroutine&& other) = default;

    TCoroutine(TCallee&& callee)
        : NDetail::TCoroutineBase()
        , Callee(std::move(callee))
    { }

    void Reset(TCallee callee)
    {
        Fiber->Reset();
        Callee = std::move(callee);
    }

    template <class... TParams>
    bool Run(TParams&&... params)
    {
        static_assert(sizeof...(TParams) == sizeof...(TArgs),
            "TParams<> and TArgs<> have different length");
        Arguments = std::make_tuple(std::forward<TParams>(params)...);
        Fiber->Run();
        return Result;
    }

    TArguments&& Yield()
    {
        Result = true;
        Fiber->Yield();
        return std::move(Arguments);
    }

private:
    virtual void Trampoline() override
    {
        try {
            NDetail::Invoke(
                Callee,
                *this,
                std::move(Arguments),
                typename NDetail::TGenerateSequence<sizeof...(TArgs)>::TType());
            Result = false;
        } catch (...) {
            Result = false;
            throw;
        }
    }

private:
    TCallee Callee;
    TArguments Arguments;
    bool Result;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
