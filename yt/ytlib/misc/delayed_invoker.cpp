
#include "../logging/log.h"
#include "../misc/ptr.h"
#include "../misc/delayed_invoker.h"

#include <util/generic/set.h>
#include <util/system/spinlock.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("DelayedInvoker");
static const TDuration SleepQuantum = TDuration::MilliSeconds(1);

////////////////////////////////////////////////////////////////////////////////

void* TDelayedInvoker::ThreadFunc(void* param)
{
    TDelayedInvoker* invoker = reinterpret_cast<TDelayedInvoker*>(param);
    invoker->ThreadMain();
    return NULL;
}

void TDelayedInvoker::ThreadMain()
{
    while (!Finished) {
        while (true) {
            TEntry::TPtr entry;
            {
                TInstant now = TInstant::Now();
                TGuard<TSpinLock> guard(SpinLock);
                if (Entries.empty()) {
                    break;
                }
                entry = *Entries.begin();
                if (entry->Deadline > now) {
                    break;
                }
                Entries.erase(Entries.begin());
            }

            LOG_DEBUG("Running task %p", ~entry);
            entry->Action->Do();
        }
        Sleep(SleepQuantum);
    }
}

TDelayedInvoker::TDelayedInvoker()
    : Thread(ThreadFunc, (void*)this)
    , Finished(false)
{
    Thread.Start();
}

void TDelayedInvoker::Shutdown()
{
    Finished = true;
    Thread.Join();
}

TDelayedInvoker::~TDelayedInvoker()
{
    //Thread.Detach();
    // TODO: the following code causes a crash during termination. Investigate this.
    Shutdown();
}

TDelayedInvoker* TDelayedInvoker::Get()
{
    return Singleton<TDelayedInvoker>();
}

TDelayedInvoker::TCookie TDelayedInvoker::Submit(IAction::TPtr action, TDuration delay)
{
    TEntry::TPtr entry = New<TEntry>(action, delay.ToDeadLine());

    LOG_DEBUG("Submitted task %p with action %p for %s",
        ~entry,
        ~action,
        ~entry->Deadline.ToString());

    TGuard<TSpinLock> guard(SpinLock);
    Entries.insert(entry);
    return entry;
}

bool TDelayedInvoker::Cancel(TCookie cookie)
{
    bool result;
    {
        TGuard<TSpinLock> guard(SpinLock);
        result = Entries.erase(cookie) != 0;
    }
    LOG_DEBUG_IF(result, "Task %p is canceled", ~cookie);
    return result;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
