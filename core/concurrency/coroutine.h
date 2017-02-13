#pragma once

#include "public.h"
#include "execution_context.h"
#include "execution_stack.h"
#include "scheduler.h"

#include <yt/core/misc/nullable.h>

namespace NYT {
namespace NConcurrency {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

class TCoroutineBase
{
protected:
    TCoroutineBase();
    TCoroutineBase(const TCoroutineBase&) = delete;
    TCoroutineBase(TCoroutineBase&& other);

    virtual ~TCoroutineBase();

    virtual void Invoke() = 0;

    static void Trampoline(void*);

    void JumpToCaller();
    void JumpToCoroutine();

    bool Completed_;

    TExecutionContext CallerContext_;

    std::shared_ptr<TExecutionStack> CoroutineStack_;
    TExecutionContext CoroutineContext_;
    std::exception_ptr CoroutineException_;

public:
    bool IsCompleted() const;

};

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
    TCoroutine(TCoroutine&& other)
        : NDetail::TCoroutineBase(std::move(other))
        , Callee_(std::move(other.Callee_))
        , Arguments_(std::move(other.Arguments_))
        , Result_(std::move(other.Result_))
    { }

    TCoroutine(TCallee&& callee)
        : NDetail::TCoroutineBase()
        , Callee_(std::move(callee))
    { }

    template <class... TParams>
    const TNullable<R>& Run(TParams&&... params)
    {
        static_assert(sizeof...(TParams) == sizeof...(TArgs),
            "TParams<> and TArgs<> have different length");
        Arguments_ = std::make_tuple(std::forward<TParams>(params)...);
        JumpToCoroutine();
        return Result_;
    }

    template <class Q>
    TArguments&& Yield(Q&& result)
    {
        Result_ = std::forward<Q>(result);
        JumpToCaller();
        return std::move(Arguments_);
    }

private:
    virtual void Invoke() override
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
    TCoroutine(TCoroutine&& other)
        : NDetail::TCoroutineBase(std::move(other))
        , Callee_(std::move(other.Callee_))
        , Arguments_(std::move(other.Arguments_))
        , Result_(other.Result_)
    { }

    TCoroutine(TCallee&& callee)
        : NDetail::TCoroutineBase()
        , Callee_(std::move(callee))
    { }

    template <class... TParams>
    bool Run(TParams&&... params)
    {
        static_assert(sizeof...(TParams) == sizeof...(TArgs),
            "TParams<> and TArgs<> have different length");
        Arguments_ = std::make_tuple(std::forward<TParams>(params)...);
        JumpToCoroutine();
        return Result_;
    }

    TArguments&& Yield()
    {
        Result_ = true;
        JumpToCaller();
        return std::move(Arguments_);
    }

private:
    virtual void Invoke() override
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

private:
    TCallee Callee_;
    TArguments Arguments_;
    bool Result_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
