#include "stdafx.h"
#include "periodic_executor.h"

#include <core/actions/invoker_util.h>
#include <core/actions/bind.h>

#include <util/random/random.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TPeriodicExecutor::TPeriodicExecutor(
    IInvokerPtr invoker,
    TClosure callback,
    TDuration period,
    EPeriodicInvokerMode mode,
    TDuration splay)
    : Invoker(invoker)
    , Callback(callback)
    , Period(period)
    , Mode(mode)
    , Splay(splay)
    , Started(false)
    , Busy(false)
    , OutOfBandRequested(false)
{ }

void TPeriodicExecutor::Start()
{
    if (!AtomicCas(&Started, true, false))
        return;

    PostDelayedCallback(RandomDuration(Splay));
}

void TPeriodicExecutor::Stop()
{
    if (!AtomicCas(&Started, false, true))
        return;

    TDelayedExecutor::CancelAndClear(Cookie);
}

void TPeriodicExecutor::ScheduleOutOfBand()
{
    if (!AtomicGet(Started))
        return;

    if (AtomicGet(Busy)) {
        AtomicSet(OutOfBandRequested, true);
    } else {
        PostCallback();
    }
}

void TPeriodicExecutor::ScheduleNext()
{
    if (!AtomicGet(Started))
        return;

    // There several reasons why this may fail:
    // 1) Calling ScheduleNext outside of the periodic action
    // 2) Calling ScheduleNext more than once
    // 3) Calling ScheduleNext for an invoker in automatic mode
    YCHECK(AtomicCas(&Busy, false, true));

    if (AtomicCas(&OutOfBandRequested, false, true)) {
        PostCallback();
    } else {
        PostDelayedCallback(Period);
    }
}

void TPeriodicExecutor::PostDelayedCallback(TDuration delay)
{
    TDelayedExecutor::CancelAndClear(Cookie);
    Cookie = TDelayedExecutor::Submit(
        BIND(&TPeriodicExecutor::PostCallback, MakeStrong(this)),
        delay);
}

void TPeriodicExecutor::PostCallback()
{
    auto this_ = MakeStrong(this);
    bool result = Invoker->Invoke(BIND([this, this_] () {
        if (AtomicGet(Started) && !AtomicGet(Busy)) {
            AtomicSet(Busy, true);
            TDelayedExecutor::CancelAndClear(Cookie);
            Callback.Run();
            if (Mode == EPeriodicInvokerMode::Automatic) {
                ScheduleNext();
            }
        }
    }));
    if (!result) {
        PostDelayedCallback(Period);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
