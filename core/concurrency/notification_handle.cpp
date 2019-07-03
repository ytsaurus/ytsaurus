#include "notification_handle.h"

#include <yt/core/misc/proc.h>

#ifdef _linux_
    #include <sys/eventfd.h>
#endif

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TNotificationHandle::TNotificationHandle()
{
#ifdef _linux_
    EventFD_ = HandleEintr(eventfd, 0, EFD_CLOEXEC | EFD_NONBLOCK);
    YT_VERIFY(EventFD_ >= 0);
#else
#ifdef _darwin_
    YT_VERIFY(HandleEintr(pipe, PipeFDs_) == 0);
#else
    YT_VERIFY(HandleEintr(pipe2, PipeFDs_, O_CLOEXEC) == 0);
#endif
    YT_VERIFY(fcntl(PipeFDs_[0], F_SETFL, O_NONBLOCK) == 0);
#endif
}

TNotificationHandle::~TNotificationHandle()
{
#ifdef _linux_
    YT_VERIFY(HandleEintr(close, EventFD_) == 0);
#else
    YT_VERIFY(HandleEintr(close, PipeFDs_[0]) == 0);
    YT_VERIFY(HandleEintr(close, PipeFDs_[1]) == 0);
#endif
}

void TNotificationHandle::Raise()
{
#ifdef _linux_
    size_t one = 1;
    YT_VERIFY(HandleEintr(write, EventFD_, &one, sizeof(one)) == sizeof(one));
#else
    if (PipeCount_.load(std::memory_order_relaxed) > 0) {
        // Avoid trashing pipe with redundant notifications.
        return;
    }
    char c = 'x';
    YT_VERIFY(HandleEintr(write, PipeFDs_[1], &c, sizeof(char)) == sizeof(char));
    PipeCount_.fetch_add(1, std::memory_order_relaxed);
#endif
}

void TNotificationHandle::Clear()
{
#ifdef _linux_
    size_t count = 0;
    YT_VERIFY(HandleEintr(read, EventFD_, &count, sizeof(count)) == sizeof(count));
#else
    for (int count = PipeCount_.exchange(0, std::memory_order_relaxed); count > 0; --count) {
        char c;
        YT_VERIFY(HandleEintr(read, PipeFDs_[0], &c, sizeof(char)) == sizeof(char));
    }
#endif
}

int TNotificationHandle::GetFD() const
{
#ifdef _linux_
    return EventFD_;
#else
    return PipeFDs_[0];
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
