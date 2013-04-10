// WARNING: This file was auto-generated.
// Please, consider incorporating any changes into the generator.

// Generated on Wed Apr 10 12:50:15 2013.


#pragma once

#include "fiber.h"

#include <ytlib/misc/nullable.h>

#if defined(_MSC_VER) && (_MSC_VER < 1700)
// Use tuple from tr1.
#include <tr1/tuple>
namespace std {
    using ::std::tr1::tuple;
    using ::std::tr1::tie;
    using ::std::tr1::get;
} // namespace std
#else
#include <tuple>
#endif

namespace NYT {

template <class Signature>
class TCoroutine;

class TCoroutineBase
{
protected:
    TCoroutineBase();

    virtual ~TCoroutineBase();
    virtual void Trampoline();

public:
    EFiberState GetState() const;

protected:
    TFiberPtr Fiber;
};

template <class R>
class TCoroutine<R()>
    : public TCoroutineBase
{
public:
    typedef R (FunctionalSignature)();
    typedef void (CoroutineSignature)(TCoroutine&);

    typedef TCallback<CoroutineSignature> TCallee;

    TCoroutine()
        : TCoroutineBase()
    { }

    TCoroutine(const TCallee& callee)
        : TCoroutineBase()
        , Callee(callee)
    { }

    TCoroutine(TCallee&& callee)
        : TCoroutineBase()
        , Callee(std::move(callee))
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
        return;
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
    TCallee Callee;
    TNullable<R> Result;
};

template <class R, class A1>
class TCoroutine<R(A1)>
    : public TCoroutineBase
{
public:
    typedef R (FunctionalSignature)(A1);
    typedef void (CoroutineSignature)(TCoroutine&, A1);

    typedef TCallback<CoroutineSignature> TCallee;
    typedef std::tuple<A1> TArguments;

    TCoroutine()
        : TCoroutineBase()
    { }

    TCoroutine(const TCallee& callee)
        : TCoroutineBase()
        , Callee(callee)
    { }

    TCoroutine(TCallee&& callee)
        : TCoroutineBase()
        , Callee(std::move(callee))
    { }

    template <class P1>
    const TNullable<R>& Run(P1&& p1)
    {
        Arguments = std::make_tuple(std::forward<P1>(p1));
        Fiber->Run();
        return Result;
    }

    template <class Q>
    TArguments&& Yield(Q&& result)
    {
        Result = std::forward<Q>(result);
        TFiber::Yield();
        return std::move(Arguments);
    }

private:
    virtual void Trampoline() override
    {
        try {
            Callee.Run(*this, std::get<0>(std::move(Arguments)));
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
    TCallee Callee;
    TArguments Arguments;
    TNullable<R> Result;
};

template <class R, class A1, class A2>
class TCoroutine<R(A1, A2)>
    : public TCoroutineBase
{
public:
    typedef R (FunctionalSignature)(A1, A2);
    typedef void (CoroutineSignature)(TCoroutine&, A1, A2);

    typedef TCallback<CoroutineSignature> TCallee;
    typedef std::tuple<A1, A2> TArguments;

    TCoroutine()
        : TCoroutineBase()
    { }

    TCoroutine(const TCallee& callee)
        : TCoroutineBase()
        , Callee(callee)
    { }

    TCoroutine(TCallee&& callee)
        : TCoroutineBase()
        , Callee(std::move(callee))
    { }

    template <class P1, class P2>
    const TNullable<R>& Run(P1&& p1, P2&& p2)
    {
        Arguments = std::make_tuple(std::forward<P1>(p1), std::forward<P2>(p2));
        Fiber->Run();
        return Result;
    }

    template <class Q>
    TArguments&& Yield(Q&& result)
    {
        Result = std::forward<Q>(result);
        TFiber::Yield();
        return std::move(Arguments);
    }

private:
    virtual void Trampoline() override
    {
        try {
            Callee.Run(*this, std::get<0>(std::move(Arguments)),
                std::get<1>(std::move(Arguments)));
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
    TCallee Callee;
    TArguments Arguments;
    TNullable<R> Result;
};

template <class R, class A1, class A2, class A3>
class TCoroutine<R(A1, A2, A3)>
    : public TCoroutineBase
{
public:
    typedef R (FunctionalSignature)(A1, A2, A3);
    typedef void (CoroutineSignature)(TCoroutine&, A1, A2, A3);

    typedef TCallback<CoroutineSignature> TCallee;
    typedef std::tuple<A1, A2, A3> TArguments;

    TCoroutine()
        : TCoroutineBase()
    { }

    TCoroutine(const TCallee& callee)
        : TCoroutineBase()
        , Callee(callee)
    { }

    TCoroutine(TCallee&& callee)
        : TCoroutineBase()
        , Callee(std::move(callee))
    { }

    template <class P1, class P2, class P3>
    const TNullable<R>& Run(P1&& p1, P2&& p2, P3&& p3)
    {
        Arguments = std::make_tuple(std::forward<P1>(p1), std::forward<P2>(p2),
            std::forward<P3>(p3));
        Fiber->Run();
        return Result;
    }

    template <class Q>
    TArguments&& Yield(Q&& result)
    {
        Result = std::forward<Q>(result);
        TFiber::Yield();
        return std::move(Arguments);
    }

private:
    virtual void Trampoline() override
    {
        try {
            Callee.Run(*this, std::get<0>(std::move(Arguments)),
                std::get<1>(std::move(Arguments)),
                std::get<2>(std::move(Arguments)));
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
    TCallee Callee;
    TArguments Arguments;
    TNullable<R> Result;
};

template <class R, class A1, class A2, class A3, class A4>
class TCoroutine<R(A1, A2, A3, A4)>
    : public TCoroutineBase
{
public:
    typedef R (FunctionalSignature)(A1, A2, A3, A4);
    typedef void (CoroutineSignature)(TCoroutine&, A1, A2, A3, A4);

    typedef TCallback<CoroutineSignature> TCallee;
    typedef std::tuple<A1, A2, A3, A4> TArguments;

    TCoroutine()
        : TCoroutineBase()
    { }

    TCoroutine(const TCallee& callee)
        : TCoroutineBase()
        , Callee(callee)
    { }

    TCoroutine(TCallee&& callee)
        : TCoroutineBase()
        , Callee(std::move(callee))
    { }

    template <class P1, class P2, class P3, class P4>
    const TNullable<R>& Run(P1&& p1, P2&& p2, P3&& p3, P4&& p4)
    {
        Arguments = std::make_tuple(std::forward<P1>(p1), std::forward<P2>(p2),
            std::forward<P3>(p3), std::forward<P4>(p4));
        Fiber->Run();
        return Result;
    }

    template <class Q>
    TArguments&& Yield(Q&& result)
    {
        Result = std::forward<Q>(result);
        TFiber::Yield();
        return std::move(Arguments);
    }

private:
    virtual void Trampoline() override
    {
        try {
            Callee.Run(*this, std::get<0>(std::move(Arguments)),
                std::get<1>(std::move(Arguments)),
                std::get<2>(std::move(Arguments)),
                std::get<3>(std::move(Arguments)));
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
    TCallee Callee;
    TArguments Arguments;
    TNullable<R> Result;
};

template <class R, class A1, class A2, class A3, class A4, class A5>
class TCoroutine<R(A1, A2, A3, A4, A5)>
    : public TCoroutineBase
{
public:
    typedef R (FunctionalSignature)(A1, A2, A3, A4, A5);
    typedef void (CoroutineSignature)(TCoroutine&, A1, A2, A3, A4, A5);

    typedef TCallback<CoroutineSignature> TCallee;
    typedef std::tuple<A1, A2, A3, A4, A5> TArguments;

    TCoroutine()
        : TCoroutineBase()
    { }

    TCoroutine(const TCallee& callee)
        : TCoroutineBase()
        , Callee(callee)
    { }

    TCoroutine(TCallee&& callee)
        : TCoroutineBase()
        , Callee(std::move(callee))
    { }

    template <class P1, class P2, class P3, class P4, class P5>
    const TNullable<R>& Run(P1&& p1, P2&& p2, P3&& p3, P4&& p4, P5&& p5)
    {
        Arguments = std::make_tuple(std::forward<P1>(p1), std::forward<P2>(p2),
            std::forward<P3>(p3), std::forward<P4>(p4), std::forward<P5>(p5));
        Fiber->Run();
        return Result;
    }

    template <class Q>
    TArguments&& Yield(Q&& result)
    {
        Result = std::forward<Q>(result);
        TFiber::Yield();
        return std::move(Arguments);
    }

private:
    virtual void Trampoline() override
    {
        try {
            Callee.Run(*this, std::get<0>(std::move(Arguments)),
                std::get<1>(std::move(Arguments)),
                std::get<2>(std::move(Arguments)),
                std::get<3>(std::move(Arguments)),
                std::get<4>(std::move(Arguments)));
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
    TCallee Callee;
    TArguments Arguments;
    TNullable<R> Result;
};

template <class R, class A1, class A2, class A3, class A4, class A5, class A6>
class TCoroutine<R(A1, A2, A3, A4, A5, A6)>
    : public TCoroutineBase
{
public:
    typedef R (FunctionalSignature)(A1, A2, A3, A4, A5, A6);
    typedef void (CoroutineSignature)(TCoroutine&, A1, A2, A3, A4, A5, A6);

    typedef TCallback<CoroutineSignature> TCallee;
    typedef std::tuple<A1, A2, A3, A4, A5, A6> TArguments;

    TCoroutine()
        : TCoroutineBase()
    { }

    TCoroutine(const TCallee& callee)
        : TCoroutineBase()
        , Callee(callee)
    { }

    TCoroutine(TCallee&& callee)
        : TCoroutineBase()
        , Callee(std::move(callee))
    { }

    template <class P1, class P2, class P3, class P4, class P5, class P6>
    const TNullable<R>& Run(P1&& p1, P2&& p2, P3&& p3, P4&& p4, P5&& p5,
        P6&& p6)
    {
        Arguments = std::make_tuple(std::forward<P1>(p1), std::forward<P2>(p2),
            std::forward<P3>(p3), std::forward<P4>(p4), std::forward<P5>(p5),
            std::forward<P6>(p6));
        Fiber->Run();
        return Result;
    }

    template <class Q>
    TArguments&& Yield(Q&& result)
    {
        Result = std::forward<Q>(result);
        TFiber::Yield();
        return std::move(Arguments);
    }

private:
    virtual void Trampoline() override
    {
        try {
            Callee.Run(*this, std::get<0>(std::move(Arguments)),
                std::get<1>(std::move(Arguments)),
                std::get<2>(std::move(Arguments)),
                std::get<3>(std::move(Arguments)),
                std::get<4>(std::move(Arguments)),
                std::get<5>(std::move(Arguments)));
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
    TCallee Callee;
    TArguments Arguments;
    TNullable<R> Result;
};

template <class R, class A1, class A2, class A3, class A4, class A5, class A6,
    class A7>
class TCoroutine<R(A1, A2, A3, A4, A5, A6, A7)>
    : public TCoroutineBase
{
public:
    typedef R (FunctionalSignature)(A1, A2, A3, A4, A5, A6, A7);
    typedef void (CoroutineSignature)(TCoroutine&, A1, A2, A3, A4, A5, A6, A7);

    typedef TCallback<CoroutineSignature> TCallee;
    typedef std::tuple<A1, A2, A3, A4, A5, A6, A7> TArguments;

    TCoroutine()
        : TCoroutineBase()
    { }

    TCoroutine(const TCallee& callee)
        : TCoroutineBase()
        , Callee(callee)
    { }

    TCoroutine(TCallee&& callee)
        : TCoroutineBase()
        , Callee(std::move(callee))
    { }

    template <class P1, class P2, class P3, class P4, class P5, class P6,
        class P7>
    const TNullable<R>& Run(P1&& p1, P2&& p2, P3&& p3, P4&& p4, P5&& p5,
        P6&& p6, P7&& p7)
    {
        Arguments = std::make_tuple(std::forward<P1>(p1), std::forward<P2>(p2),
            std::forward<P3>(p3), std::forward<P4>(p4), std::forward<P5>(p5),
            std::forward<P6>(p6), std::forward<P7>(p7));
        Fiber->Run();
        return Result;
    }

    template <class Q>
    TArguments&& Yield(Q&& result)
    {
        Result = std::forward<Q>(result);
        TFiber::Yield();
        return std::move(Arguments);
    }

private:
    virtual void Trampoline() override
    {
        try {
            Callee.Run(*this, std::get<0>(std::move(Arguments)),
                std::get<1>(std::move(Arguments)),
                std::get<2>(std::move(Arguments)),
                std::get<3>(std::move(Arguments)),
                std::get<4>(std::move(Arguments)),
                std::get<5>(std::move(Arguments)),
                std::get<6>(std::move(Arguments)));
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
    TCallee Callee;
    TArguments Arguments;
    TNullable<R> Result;
};

} // namespace NYT
