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
    explicit TThreadPool(int threadCount, const Stroka& threadName);
    virtual ~TThreadPool();

    void Shutdown();

    IInvokerPtr GetInvoker();

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

