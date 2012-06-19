#include "stdafx.h"
#include "periodic_invoker.h"

#include <ytlib/actions/invoker_util.h>
#include <ytlib/actions/bind.h>

#include <util/random/random.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TPeriodicInvoker::TPeriodicInvoker(
    IInvokerPtr invoker,
    TClosure callback,
    TDuration period,
    TDuration splay)
    : Invoker(invoker)
    , Callback(callback)
    , Period(period)
    , Splay(splay)
    , Started(false)
    , Busy(false)
    , OutOfBandRequested(false)
{ }

void TPeriodicInvoker::Start()
{
    YASSERT(!Started);
    Started = true;
    PostDelayedCallback(RandomDuration(Splay));
}

void TPeriodicInvoker::Stop()
{
    Started = false;
    Busy = false;
    TDelayedInvoker::CancelAndClear(Cookie);
}

void TPeriodicInvoker::ScheduleOutOfBand()
{
    YASSERT(Started);
    if (Busy) {
        OutOfBandRequested = true;
    } else {
        PostCallback();
    }
}

void TPeriodicInvoker::ScheduleNext()
{
    YASSERT(Started);
    YASSERT(Busy);
    Busy = false;
    if (OutOfBandRequested) {
        OutOfBandRequested = false;
        PostCallback();
    } else {
        PostDelayedCallback(Period);
    }
}

void TPeriodicInvoker::PostDelayedCallback(TDuration delay)
{
    TDelayedInvoker::CancelAndClear(Cookie);
    Cookie = TDelayedInvoker::Submit(
        BIND(&TPeriodicInvoker::PostCallback, MakeStrong(this)),
        delay);
}

void TPeriodicInvoker::PostCallback()
{
    auto this_ = MakeStrong(this);
    Invoker->Invoke(BIND([this, this_] () {
        if (Started && !Busy) {
            Busy = true;
            TDelayedInvoker::CancelAndClear(Cookie);
            Callback.Run();
        }
    }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
