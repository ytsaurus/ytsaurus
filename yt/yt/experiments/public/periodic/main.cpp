#include <yt/yt/core/misc/common.h>

#ifdef _darwin_
#include <sys/types.h>
#include <sys/event.h>
#include <sys/time.h>
#endif

void RunViaSleep()
{
    const TDuration SleepQuantum = TDuration::MilliSeconds(10);

    TInstant previousInstant = TInstant::Now();

    while (true) {
        Sleep(SleepQuantum);
        auto currentInstant = TInstant::Now();
        Cerr << (currentInstant - previousInstant).MillisecondsFloat() << " msec" << Endl;
        previousInstant = currentInstant;
    }
}

#ifdef _darwin_

void RunViaKQueue()
{
    TInstant previousInstant = TInstant::Now();

    const TDuration SleepQuantum = TDuration::MilliSeconds(5);
    int kq = kqueue();
    YT_VERIFY(kq != -1);
    struct kevent ev {
        .ident = 0,
        .filter = EVFILT_TIMER,
        .flags = EV_ADD | EV_ENABLE,
        .fflags = 0,
        .data = static_cast<intptr_t>(SleepQuantum.MilliSeconds()),
        .udata = nullptr,
    };
    YT_VERIFY(kevent(kq, /*changelist*/ &ev, /*nchanges*/ 1, /*eventlist*/ nullptr, /*nevents*/ 0, /*timeout*/ nullptr) >= 0);

    while (true) {
        auto result = kevent(kq, /*changelist*/ nullptr, /*nchanges*/ 0, /*eventlist*/ &ev, /*nevents*/ 1, /*timeout*/ nullptr);
        YT_VERIFY(result >= 0);
        if (result == 0) {
            continue;
        }
        YT_VERIFY(result == 1);

        auto currentInstant = TInstant::Now();
        Cerr << (currentInstant - previousInstant).MillisecondsFloat() << " msec" << Endl;
        previousInstant = currentInstant;
    }
}

#endif

int main(int argc, char* argv[])
{
    if (argc != 2) {
        Cerr << "Specify a single command-line argument which is one of (sleep, kqueue)" << Endl;
        return 1;
    }

    TString mode(argv[1]);
    if (mode == "sleep") {
        RunViaSleep();
    }
    #ifdef _darwin_
    else if (mode == "kqueue") {
        RunViaKQueue();
    }
    #endif
    else {
        YT_ABORT();
    }
}
