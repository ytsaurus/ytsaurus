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
    // TODO(babenko): cannot mark move ctor as default due to VS2013 bug
    TCoroutineBase(TCoroutineBase&& other);

    TCoroutineBase(const TCoroutineBase&) = delete;

    virtual ~TCoroutineBase();

    virtual void Trampoline() = 0;

    TFiberPtr Fiber_;

public:
    EFiberState GetState() const
    {
        return Fiber_->GetState();
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
    // TODO(babenko): cannot mark move ctor as default due to VS2013 bug
    TCoroutine(TCoroutine&& other)
        : Callee_(std::move(other.Callee))
        , Arguments_(std::move(other.Arguments))
        , Result_(std::move(other.Result))
    { }

    TCoroutine(TCallee&& callee)
        : NDetail::TCoroutineBase()
        , Callee_(std::move(callee))
    { }

    void Reset(TCallee callee)
    {
        Fiber_->Reset();
        Callee_ = std::move(callee);
    }

    template <class... TParams>
    const TNullable<R>& Run(TParams&&... params)
    {
        static_assert(sizeof...(TParams) == sizeof...(TArgs),
            "TParams<> and TArgs<> have different length");
        Arguments_ = std::make_tuple(std::forward<TParams>(params)...);
        Fiber_->Run();
        return Result_;
    }

    template <class Q>
    TArguments&& Yield(Q&& result)
    {
        Result_ = std::forward<Q>(result);
        Fiber_->Yield();
        return std::move(Arguments_);
    }

private:
    virtual void Trampoline() override
    {
        try {
            NDetail::Invoke(
                Callee_,
                *this,
                std::move(Arguments_),
                typename NDetail::TGenerateSequence<sizeof...(TArgs)>::TType());
            Result_.Reset();
        } catch (...) {
            Result_.Reset();
            throw;
        }
    }

private:
    TCallee Callee_;
    TArguments Arguments_;
    TNullable<R> Result_;

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
    // TODO(babenko): cannot mark move ctor as default due to VS2013 bug
    TCoroutine(TCoroutine&& other)
        : Callee_(std::move(other.Callee))
        , Arguments_(std::move(other.Arguments))
        , Result_(other.Result)
    { }

    TCoroutine(TCallee&& callee)
        : NDetail::TCoroutineBase()
        , Callee_(std::move(callee))
    { }

    void Reset(TCallee callee)
    {
        Fiber_->Reset();
        Callee_ = std::move(callee);
    }

    template <class... TParams>
    bool Run(TParams&&... params)
    {
        static_assert(sizeof...(TParams) == sizeof...(TArgs),
            "TParams<> and TArgs<> have different length");
        Arguments_ = std::make_tuple(std::forward<TParams>(params)...);
        Fiber_->Run();
        return Result_;
    }

    TArguments&& Yield()
    {
        Result_ = true;
        Fiber_->Yield();
        return std::move(Arguments_);
    }

private:
    virtual void Trampoline() override
    {
        try {
            NDetail::Invoke(
                Callee_,
                *this,
                std::move(Arguments_),
                typename NDetail::TGenerateSequence<sizeof...(TArgs)>::TType());
            Result_ = false;
        } catch (...) {
            Result_ = false;
            throw;
        }
    }

private:
    TCallee Callee_;
    TArguments Arguments_;
    bool Result_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
