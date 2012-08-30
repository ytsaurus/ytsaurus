#pragma once

#include "common.h"
#include "invoker.h"
#include "callback.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): move to public.h
class TActionQueue;
typedef TIntrusivePtr<TActionQueue> TActionQueuePtr;

class TFairShareActionQueue;
typedef TIntrusivePtr<TFairShareActionQueue> TFairShareActionQueuePtr;

class TPrioritizedActionQueue;
typedef TIntrusivePtr<TPrioritizedActionQueue> TPrioritizedActionQueuePtr;

class TThreadPool;
typedef TIntrusivePtr<TThreadPool> TThreadPoolPtr;

////////////////////////////////////////////////////////////////////////////////

class TActionQueue
    : public TRefCounted
{
public:
    // TActionQueue is used internally by the logging infrastructure,
    // which passes enableLogging = false to prevent infinite recursion.
    TActionQueue(const Stroka& threadName = "<ActionQueue>", bool enableLogging = true);
    virtual ~TActionQueue();

    void Shutdown();

    IInvokerPtr GetInvoker();
    int GetSize() const;

    static TCallback<TActionQueuePtr()> CreateFactory(const Stroka& threadName);
    
private:
    friend class TThreadPool;

    class TImpl;
    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

class TFairShareActionQueue
    : public TRefCounted
{
public:
    explicit TFairShareActionQueue(int queueCount = 1, const Stroka& threadName = "<FSActionQueue>");
    virtual ~TFairShareActionQueue();

    void Shutdown();

    IInvokerPtr GetInvoker(int queueIndex);

    static TCallback<TFairShareActionQueuePtr()> CreateFactory(int queueCount, const Stroka& threadName);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

class TThreadPool
    : public TRefCounted
{
public:
    TThreadPool(int threadCount = 1, const Stroka& threadName = "<ThreadPool>");
    virtual ~TThreadPool();

    void Shutdown();

    IInvokerPtr GetInvoker();

    static TCallback<TThreadPoolPtr()> CreateFactory(int queueCount, const Stroka& threadName);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

class TPrioritizedActionQueue
    : public TRefCounted
{
public:
    explicit TPrioritizedActionQueue(const Stroka& threadName);
    virtual ~TPrioritizedActionQueue();

    void Shutdown();

    IInvokerPtr CreateInvoker(i64 priority);

private:
    class TInvoker;
    class TImpl;
    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

//! Returns an invoker that executes all queues actions in the
//! context of #underlyingInvoker (possibly in different threads)
//! but in a serialized fashion (i.e. all queued actions are executed
//! in the proper order and no two actions are executed in parallel).
IInvokerPtr CreateSerializedInvoker(IInvokerPtr underlyingInvoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

