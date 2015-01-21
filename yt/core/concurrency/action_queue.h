#pragma once

#include "public.h"

#include <core/actions/callback.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TActionQueue
    : public TRefCounted
{
public:
    explicit TActionQueue(
        const Stroka& threadName = "<ActionQueue>",
        bool enableLogging = true,
        bool enableProfiling = true);
    virtual ~TActionQueue();

    void Shutdown();

    IInvokerPtr GetInvoker();

    static TCallback<TActionQueuePtr()> CreateFactory(
        const Stroka& threadName,
        bool enableLogging = true,
        bool enableProfiling = true);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;
};

DEFINE_REFCOUNTED_TYPE(TActionQueue)

////////////////////////////////////////////////////////////////////////////////

class TFairShareActionQueue
    : public TRefCounted
{
public:
    explicit TFairShareActionQueue(
        const Stroka& threadName,
        const std::vector<Stroka>& bucketNames);
    virtual ~TFairShareActionQueue();

    void Shutdown();

    IInvokerPtr GetInvoker(int index);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TFairShareActionQueue)

////////////////////////////////////////////////////////////////////////////////

class TThreadPool
    : public TRefCounted
{
public:
    TThreadPool(
        int threadCount,
        const Stroka& threadNamePrefix);
    virtual ~TThreadPool();

    void Shutdown();

    IInvokerPtr GetInvoker();

    static TCallback<TThreadPoolPtr()> CreateFactory(
        int threadCount,
        const Stroka& threadName);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;
};

DEFINE_REFCOUNTED_TYPE(TThreadPool)

////////////////////////////////////////////////////////////////////////////////

//! Creates an invoker that executes all callbacks in the
//! context of #underlyingInvoker (possibly in different threads)
//! but in a serialized fashion (i.e. all queued actions are executed
//! in the proper order and no two actions are executed in parallel).
IInvokerPtr CreateSerializedInvoker(IInvokerPtr underlyingInvoker);

////////////////////////////////////////////////////////////////////////////////

//! Creates a wrapper around IInvoker that supports action reordering.
//! Actions with the highest priority are executed first.
IPrioritizedInvokerPtr CreatePrioritizedInvoker(IInvokerPtr underlyingInvoker);

//! Creates a wrapper around IInvoker that implements IPrioritizedInvoker but
//! does not perform any actual reordering. Priorities passed to #IPrioritizedInvoker::Invoke
//! are ignored.
IPrioritizedInvokerPtr CreateFakePrioritizedInvoker(IInvokerPtr underlyingInvoker);

////////////////////////////////////////////////////////////////////////////////

//! Creates an invoker that executes all callbacks in the
//! context of #underlyingInvoker allowing up to #maxConcurrentInvocations
//! outstanding requests to the latter.
IInvokerPtr CreateBoundedConcurrencyInvoker(
    IInvokerPtr underlyingInvoker,
    int maxConcurrentInvocations);

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
