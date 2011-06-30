#pragma once

#include "action.h"

#include "../misc/common.h"

#include <util/generic/ptr.h>
#include <util/system/thread.h>
#include <util/system/event.h>
#include <util/thread/lfqueue.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TActionQueue
    : public IInvoker
{
public:
    typedef TIntrusivePtr<TActionQueue> TPtr;

    // TActionQueue is used internally by the logging infrastructure,
    // which passes enableLogging = false to prevent infinite recursion.
    TActionQueue(bool enableLogging = true);

    virtual ~TActionQueue();
    
    virtual void Invoke(IAction::TPtr action);

    void Shutdown();

protected:
    virtual void OnIdle();
    bool IsEmpty();

private:
    bool EnableLogging;
    TLockFreeQueue<IAction::TPtr> Queue;
    TThread Thread;
    volatile bool Finished;
    Event WakeupEvent;

    static void* ThreadFunc(void* param);
    void ThreadMain();
};

////////////////////////////////////////////////////////////////////////////////

}

