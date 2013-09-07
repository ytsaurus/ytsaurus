// WARNING: This file was auto-generated.
// Please, consider incorporating any changes into the generator.

// Generated on Sat Sep 07 17:44:53 2013.


#pragma once

#include "public.h"
#include "fiber.h"

#include <ytlib/misc/nullable.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class Signature>
class TCoroutine;

class TCoroutineBase
{
protected:
    TCoroutineBase();
    virtual ~TCoroutineBase();

    virtual void Trampoline() = 0;

public:
    EFiberState GetState() const;

protected:
    TFiberPtr Fiber;

private:
    TCoroutineBase(const TCoroutineBase&);
    TCoroutineBase& operator=(const TCoroutineBase&);

};

////////////////////////////////////////////////////////////////////////////////
// === Arity 0, non-void result type.

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

    TCoroutine(TCallee&& callee)
        : TCoroutineBase()
        , Callee(std::move(callee))
    { }

    void Reset(TCallee callee)
    {
        Fiber->Reset();
        Callee = std::move(callee);
    }

    const TNullable<R>& Run()
    {
        Fiber->Run();
        return Result;
    }

    template <class Q>
    void Yield(Q&& result)
    {
        Result = std::forward<Q>(result);
        Fiber->Yield();
        return;
    }

private:
    virtual void Trampoline() override
    {
        try {
            Callee.Run(*this);
            Result.Reset();
        } catch(...) {
            Result.Reset();
            throw;
        }
    }

private:
    TCallee Callee;
    TNullable<R> Result;
};

////////////////////////////////////////////////////////////////////////////////
// === Arity 0, void result type.

template <>
class TCoroutine<void()>
    : public TCoroutineBase
{
public:
    typedef void (FunctionalSignature)();
    typedef void (CoroutineSignature)(TCoroutine&);

    typedef TCallback<CoroutineSignature> TCallee;

    TCoroutine()
        : TCoroutineBase()
    { }

    TCoroutine(TCallee&& callee)
        : TCoroutineBase()
        , Callee(std::move(callee))
    { }

    void Reset(TCallee callee)
    {
        Fiber->Reset();
        Callee = std::move(callee);
    }

    bool Run()
    {
        Fiber->Run();
        return Result;
    }

    void Yield()
    {
        Result = true;
        Fiber->Yield();
        return;
    }

private:
    virtual void Trampoline() override
    {
        try {
            Callee.Run(*this);
            Result = false;
        } catch(const std::exception& ex) {
            Result = false;
            throw;
        }
    }

private:
    TCallee Callee;
    bool Result;
};


////////////////////////////////////////////////////////////////////////////////
// === Arity 1, non-void result type.

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

    TCoroutine(TCallee&& callee)
        : TCoroutineBase()
        , Callee(std::move(callee))
    { }

    void Reset(TCallee callee)
    {
        Fiber->Reset();
        Callee = std::move(callee);
    }

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
        Fiber->Yield();
        return std::move(Arguments);
    }

private:
    virtual void Trampoline() override
    {
        try {
            Callee.Run(*this, std::get<0>(std::move(Arguments)));
            Result.Reset();
        } catch(...) {
            Result.Reset();
            throw;
        }
    }

private:
    TCallee Callee;
    TArguments Arguments;
    TNullable<R> Result;
};

////////////////////////////////////////////////////////////////////////////////
// === Arity 1, void result type.

template <class A1>
class TCoroutine<void(A1)>
    : public TCoroutineBase
{
public:
    typedef void (FunctionalSignature)(A1);
    typedef void (CoroutineSignature)(TCoroutine&, A1);

    typedef TCallback<CoroutineSignature> TCallee;
    typedef std::tuple<A1> TArguments;

    TCoroutine()
        : TCoroutineBase()
    { }

    TCoroutine(TCallee&& callee)
        : TCoroutineBase()
        , Callee(std::move(callee))
    { }

    void Reset(TCallee callee)
    {
        Fiber->Reset();
        Callee = std::move(callee);
    }

    template <class P1>
    bool Run(P1&& p1)
    {
        Arguments = std::make_tuple(std::forward<P1>(p1));
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
            Callee.Run(*this, std::get<0>(std::move(Arguments)));
            Result = false;
        } catch(const std::exception& ex) {
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
// === Arity 2, non-void result type.

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

    TCoroutine(TCallee&& callee)
        : TCoroutineBase()
        , Callee(std::move(callee))
    { }

    void Reset(TCallee callee)
    {
        Fiber->Reset();
        Callee = std::move(callee);
    }

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
        Fiber->Yield();
        return std::move(Arguments);
    }

private:
    virtual void Trampoline() override
    {
        try {
            Callee.Run(*this, std::get<0>(std::move(Arguments)),
                std::get<1>(std::move(Arguments)));
            Result.Reset();
        } catch(...) {
            Result.Reset();
            throw;
        }
    }

private:
    TCallee Callee;
    TArguments Arguments;
    TNullable<R> Result;
};

////////////////////////////////////////////////////////////////////////////////
// === Arity 2, void result type.

template <class A1, class A2>
class TCoroutine<void(A1, A2)>
    : public TCoroutineBase
{
public:
    typedef void (FunctionalSignature)(A1, A2);
    typedef void (CoroutineSignature)(TCoroutine&, A1, A2);

    typedef TCallback<CoroutineSignature> TCallee;
    typedef std::tuple<A1, A2> TArguments;

    TCoroutine()
        : TCoroutineBase()
    { }

    TCoroutine(TCallee&& callee)
        : TCoroutineBase()
        , Callee(std::move(callee))
    { }

    void Reset(TCallee callee)
    {
        Fiber->Reset();
        Callee = std::move(callee);
    }

    template <class P1, class P2>
    bool Run(P1&& p1, P2&& p2)
    {
        Arguments = std::make_tuple(std::forward<P1>(p1), std::forward<P2>(p2));
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
            Callee.Run(*this, std::get<0>(std::move(Arguments)),
                std::get<1>(std::move(Arguments)));
            Result = false;
        } catch(const std::exception& ex) {
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
// === Arity 3, non-void result type.

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

    TCoroutine(TCallee&& callee)
        : TCoroutineBase()
        , Callee(std::move(callee))
    { }

    void Reset(TCallee callee)
    {
        Fiber->Reset();
        Callee = std::move(callee);
    }

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
        Fiber->Yield();
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
        } catch(...) {
            Result.Reset();
            throw;
        }
    }

private:
    TCallee Callee;
    TArguments Arguments;
    TNullable<R> Result;
};

////////////////////////////////////////////////////////////////////////////////
// === Arity 3, void result type.

template <class A1, class A2, class A3>
class TCoroutine<void(A1, A2, A3)>
    : public TCoroutineBase
{
public:
    typedef void (FunctionalSignature)(A1, A2, A3);
    typedef void (CoroutineSignature)(TCoroutine&, A1, A2, A3);

    typedef TCallback<CoroutineSignature> TCallee;
    typedef std::tuple<A1, A2, A3> TArguments;

    TCoroutine()
        : TCoroutineBase()
    { }

    TCoroutine(TCallee&& callee)
        : TCoroutineBase()
        , Callee(std::move(callee))
    { }

    void Reset(TCallee callee)
    {
        Fiber->Reset();
        Callee = std::move(callee);
    }

    template <class P1, class P2, class P3>
    bool Run(P1&& p1, P2&& p2, P3&& p3)
    {
        Arguments = std::make_tuple(std::forward<P1>(p1), std::forward<P2>(p2),
            std::forward<P3>(p3));
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
            Callee.Run(*this, std::get<0>(std::move(Arguments)),
                std::get<1>(std::move(Arguments)),
                std::get<2>(std::move(Arguments)));
            Result = false;
        } catch(const std::exception& ex) {
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
// === Arity 4, non-void result type.

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

    TCoroutine(TCallee&& callee)
        : TCoroutineBase()
        , Callee(std::move(callee))
    { }

    void Reset(TCallee callee)
    {
        Fiber->Reset();
        Callee = std::move(callee);
    }

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
        Fiber->Yield();
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
        } catch(...) {
            Result.Reset();
            throw;
        }
    }

private:
    TCallee Callee;
    TArguments Arguments;
    TNullable<R> Result;
};

////////////////////////////////////////////////////////////////////////////////
// === Arity 4, void result type.

template <class A1, class A2, class A3, class A4>
class TCoroutine<void(A1, A2, A3, A4)>
    : public TCoroutineBase
{
public:
    typedef void (FunctionalSignature)(A1, A2, A3, A4);
    typedef void (CoroutineSignature)(TCoroutine&, A1, A2, A3, A4);

    typedef TCallback<CoroutineSignature> TCallee;
    typedef std::tuple<A1, A2, A3, A4> TArguments;

    TCoroutine()
        : TCoroutineBase()
    { }

    TCoroutine(TCallee&& callee)
        : TCoroutineBase()
        , Callee(std::move(callee))
    { }

    void Reset(TCallee callee)
    {
        Fiber->Reset();
        Callee = std::move(callee);
    }

    template <class P1, class P2, class P3, class P4>
    bool Run(P1&& p1, P2&& p2, P3&& p3, P4&& p4)
    {
        Arguments = std::make_tuple(std::forward<P1>(p1), std::forward<P2>(p2),
            std::forward<P3>(p3), std::forward<P4>(p4));
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
            Callee.Run(*this, std::get<0>(std::move(Arguments)),
                std::get<1>(std::move(Arguments)),
                std::get<2>(std::move(Arguments)),
                std::get<3>(std::move(Arguments)));
            Result = false;
        } catch(const std::exception& ex) {
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
// === Arity 5, non-void result type.

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

    TCoroutine(TCallee&& callee)
        : TCoroutineBase()
        , Callee(std::move(callee))
    { }

    void Reset(TCallee callee)
    {
        Fiber->Reset();
        Callee = std::move(callee);
    }

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
        Fiber->Yield();
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
        } catch(...) {
            Result.Reset();
            throw;
        }
    }

private:
    TCallee Callee;
    TArguments Arguments;
    TNullable<R> Result;
};

////////////////////////////////////////////////////////////////////////////////
// === Arity 5, void result type.

template <class A1, class A2, class A3, class A4, class A5>
class TCoroutine<void(A1, A2, A3, A4, A5)>
    : public TCoroutineBase
{
public:
    typedef void (FunctionalSignature)(A1, A2, A3, A4, A5);
    typedef void (CoroutineSignature)(TCoroutine&, A1, A2, A3, A4, A5);

    typedef TCallback<CoroutineSignature> TCallee;
    typedef std::tuple<A1, A2, A3, A4, A5> TArguments;

    TCoroutine()
        : TCoroutineBase()
    { }

    TCoroutine(TCallee&& callee)
        : TCoroutineBase()
        , Callee(std::move(callee))
    { }

    void Reset(TCallee callee)
    {
        Fiber->Reset();
        Callee = std::move(callee);
    }

    template <class P1, class P2, class P3, class P4, class P5>
    bool Run(P1&& p1, P2&& p2, P3&& p3, P4&& p4, P5&& p5)
    {
        Arguments = std::make_tuple(std::forward<P1>(p1), std::forward<P2>(p2),
            std::forward<P3>(p3), std::forward<P4>(p4), std::forward<P5>(p5));
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
            Callee.Run(*this, std::get<0>(std::move(Arguments)),
                std::get<1>(std::move(Arguments)),
                std::get<2>(std::move(Arguments)),
                std::get<3>(std::move(Arguments)),
                std::get<4>(std::move(Arguments)));
            Result = false;
        } catch(const std::exception& ex) {
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
// === Arity 6, non-void result type.

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

    TCoroutine(TCallee&& callee)
        : TCoroutineBase()
        , Callee(std::move(callee))
    { }

    void Reset(TCallee callee)
    {
        Fiber->Reset();
        Callee = std::move(callee);
    }

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
        Fiber->Yield();
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
        } catch(...) {
            Result.Reset();
            throw;
        }
    }

private:
    TCallee Callee;
    TArguments Arguments;
    TNullable<R> Result;
};

////////////////////////////////////////////////////////////////////////////////
// === Arity 6, void result type.

template <class A1, class A2, class A3, class A4, class A5, class A6>
class TCoroutine<void(A1, A2, A3, A4, A5, A6)>
    : public TCoroutineBase
{
public:
    typedef void (FunctionalSignature)(A1, A2, A3, A4, A5, A6);
    typedef void (CoroutineSignature)(TCoroutine&, A1, A2, A3, A4, A5, A6);

    typedef TCallback<CoroutineSignature> TCallee;
    typedef std::tuple<A1, A2, A3, A4, A5, A6> TArguments;

    TCoroutine()
        : TCoroutineBase()
    { }

    TCoroutine(TCallee&& callee)
        : TCoroutineBase()
        , Callee(std::move(callee))
    { }

    void Reset(TCallee callee)
    {
        Fiber->Reset();
        Callee = std::move(callee);
    }

    template <class P1, class P2, class P3, class P4, class P5, class P6>
    bool Run(P1&& p1, P2&& p2, P3&& p3, P4&& p4, P5&& p5, P6&& p6)
    {
        Arguments = std::make_tuple(std::forward<P1>(p1), std::forward<P2>(p2),
            std::forward<P3>(p3), std::forward<P4>(p4), std::forward<P5>(p5),
            std::forward<P6>(p6));
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
            Callee.Run(*this, std::get<0>(std::move(Arguments)),
                std::get<1>(std::move(Arguments)),
                std::get<2>(std::move(Arguments)),
                std::get<3>(std::move(Arguments)),
                std::get<4>(std::move(Arguments)),
                std::get<5>(std::move(Arguments)));
            Result = false;
        } catch(const std::exception& ex) {
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
// === Arity 7, non-void result type.

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

    TCoroutine(TCallee&& callee)
        : TCoroutineBase()
        , Callee(std::move(callee))
    { }

    void Reset(TCallee callee)
    {
        Fiber->Reset();
        Callee = std::move(callee);
    }

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
        Fiber->Yield();
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
        } catch(...) {
            Result.Reset();
            throw;
        }
    }

private:
    TCallee Callee;
    TArguments Arguments;
    TNullable<R> Result;
};

////////////////////////////////////////////////////////////////////////////////
// === Arity 7, void result type.

template <class A1, class A2, class A3, class A4, class A5, class A6, class A7>
class TCoroutine<void(A1, A2, A3, A4, A5, A6, A7)>
    : public TCoroutineBase
{
public:
    typedef void (FunctionalSignature)(A1, A2, A3, A4, A5, A6, A7);
    typedef void (CoroutineSignature)(TCoroutine&, A1, A2, A3, A4, A5, A6, A7);

    typedef TCallback<CoroutineSignature> TCallee;
    typedef std::tuple<A1, A2, A3, A4, A5, A6, A7> TArguments;

    TCoroutine()
        : TCoroutineBase()
    { }

    TCoroutine(TCallee&& callee)
        : TCoroutineBase()
        , Callee(std::move(callee))
    { }

    void Reset(TCallee callee)
    {
        Fiber->Reset();
        Callee = std::move(callee);
    }

    template <class P1, class P2, class P3, class P4, class P5, class P6,
        class P7>
    bool Run(P1&& p1, P2&& p2, P3&& p3, P4&& p4, P5&& p5, P6&& p6, P7&& p7)
    {
        Arguments = std::make_tuple(std::forward<P1>(p1), std::forward<P2>(p2),
            std::forward<P3>(p3), std::forward<P4>(p4), std::forward<P5>(p5),
            std::forward<P6>(p6), std::forward<P7>(p7));
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
            Callee.Run(*this, std::get<0>(std::move(Arguments)),
                std::get<1>(std::move(Arguments)),
                std::get<2>(std::move(Arguments)),
                std::get<3>(std::move(Arguments)),
                std::get<4>(std::move(Arguments)),
                std::get<5>(std::move(Arguments)),
                std::get<6>(std::move(Arguments)));
            Result = false;
        } catch(const std::exception& ex) {
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

} // namespace NYT
