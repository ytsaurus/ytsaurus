#pragma once

#include "common.h"
#include "invoker.h"
#include "callback.h"

#include <ytlib/profiling/profiler.h>

#include <ytlib/misc/nullable.h>

#include <ytlib/ytree/public.h>

#include <util/system/thread.h>
#include <util/system/event.h>
#include <util/thread/lfqueue.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TActionQueueBase;

class TQueueInvoker
    : public IInvoker
{
public:
    TQueueInvoker(
        const NYTree::TYPath& profilingPath,
        TActionQueueBase* owner,
        bool enableLogging);

    bool Invoke(const TClosure& action);
    void Shutdown();
    bool DequeueAndExecute();

    int GetSize() const;
    bool IsEmpty() const;

private:
    struct TItem
    {
        NProfiling::TCpuInstant StartInstant;
        TClosure Action;
    };

    TActionQueueBase* Owner;
    bool EnableLogging;
    NProfiling::TProfiler Profiler;

    NProfiling::TRateCounter EnqueueCounter;
    NProfiling::TRateCounter DequeueCounter;
    TAtomic QueueSize;
    NProfiling::TAggregateCounter QueueSizeCounter;
    NProfiling::TAggregateCounter WaitTimeCounter;
    NProfiling::TAggregateCounter ExecTimeCounter;
    NProfiling::TAggregateCounter TotalTimeCounter;

    TLockFreeQueue<TItem> Queue;
};

typedef TIntrusivePtr<TQueueInvoker> TQueueInvokerPtr;

///////////////////////////////////////////////////////////////////////////////

class TActionQueueBase
    : public TRefCounted
{
public:
    virtual ~TActionQueueBase();

protected:
    TActionQueueBase(const Stroka& threadName, bool enableLogging);

    void Start();
    void Shutdown();
    void Signal();

    virtual bool DequeueAndExecute() = 0;
    virtual void OnIdle();

    virtual void OnThreadStart();
    virtual void OnThreadShutdown();

    bool IsRunning() const;

private:
    friend class TQueueInvoker;

    static void* ThreadFunc(void* param);
    void ThreadMain();

    bool EnableLogging;
    volatile bool Running;
    Event WakeupEvent;
    TThread Thread;
    Stroka ThreadName;

};


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

