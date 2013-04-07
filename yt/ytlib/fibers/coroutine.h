#include "fiber.h"

#include <ytlib/misc/nullable.h>

namespace NYT {

template <class Signature>
class TCoroutine;

class TCoroutineBase
{
protected:
    TCoroutineBase()
        : Fiber(New<TFiber>(BIND(&TCoroutineBase::Trampoline, this)))
    { }

    virtual void Trampoline() = 0;

public:
    EFiberState GetState() const
    {
        return Fiber->GetState();
    }

protected:
    TFiberPtr Fiber;
};

template <class R, class A>
class TCoroutine<R(A)>
    : public TCoroutineBase
    , private TNonCopyable
{
public:
    typedef TCallback<void(TCoroutine&, A)> TCoroutineCallback;

    TCoroutine()
        : TCoroutineBase()
    { }

    TCoroutine(TCoroutineCallback callee)
        : TCoroutineBase()
        , Callee(std::move(callee))
    { }

    ~TCoroutine()
    { }

    template <class P>
    const TNullable<R>& Run(P&& argument)
    {
        Argument = std::forward<P>(argument);
        Fiber->Run();
        return Result;
    }

    template <class Q>
    A Yield(Q&& result)
    {
        Result = std::forward<Q>(result);
        TFiber::Yield();
        return Argument;
    }

private:
    virtual void Trampoline() override
    {
        try {
            Callee.Run(*this, std::move(Argument));
            Result.Reset();
        } catch(const std::exception& ex) {
            fprintf(
                stderr,
                "*** Uncaught exception in TCoroutine: %s\n",
                ex.what());
            abort();
        }
    }

private:
    TCoroutineCallback Callee;
    TNullable<R> Result;
    A Argument;
};

template <class R>
class TCoroutine<R()>
    : public TCoroutineBase
    , private TNonCopyable
{
public:
    typedef TCallback<void(TCoroutine&)> TCoroutineCallback;

    TCoroutine()
        : TCoroutineBase()
    { }

    TCoroutine(TCoroutineCallback callee)
        : TCoroutineBase()
        , Callee(std::move(callee))
    { }

    ~TCoroutine()
    { }

    const TNullable<R>& Run()
    {
        Fiber->Run();
        return Result;
    }

    template <class Q>
    void Yield(Q&& result)
    {
        Result = std::forward<Q>(result);
        TFiber::Yield();
    }

private:
    virtual void Trampoline() override
    {
        try {
            Callee.Run(*this);
            Result.Reset();
        } catch(const std::exception& ex) {
            fprintf(
                stderr,
                "*** Uncaught exception in TCoroutine: %s\n",
                ex.what());
            abort();
        }
    }

private:
    TCoroutineCallback Callee;
    TNullable<R> Result;
};

} // namespace NYT

