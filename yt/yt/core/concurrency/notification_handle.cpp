#include "notification_handle.h"

#include <yt/yt/core/misc/proc.h>

#ifdef _linux_
    #include <sys/eventfd.h>
#endif

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TNotificationHandle::TNotificationHandle(bool blocking)
{
#ifdef _linux_
    EventFD_ = HandleEintr(
        eventfd,
        0,
        EFD_CLOEXEC | (blocking ? 0 : EFD_NONBLOCK));
    YT_VERIFY(EventFD_ >= 0);
#else
#ifdef _darwin_
    YT_VERIFY(HandleEintr(pipe, PipeFDs_) == 0);
#else
    YT_VERIFY(HandleEintr(pipe2, PipeFDs_, O_CLOEXEC) == 0);
#endif
    if (!blocking) {
        YT_VERIFY(fcntl(PipeFDs_[0], F_SETFL, O_NONBLOCK) == 0);
    }
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
    uint64_t one = 1;
    YT_VERIFY(HandleEintr(write, EventFD_, &one, sizeof(one)) == sizeof(one));
#else
    char c = 'x';
    YT_VERIFY(HandleEintr(write, PipeFDs_[1], &c, sizeof(char)) == sizeof(char));
#endif
}

void TNotificationHandle::Clear()
{
#ifdef _linux_
    uint64_t count = 0;
    auto ret = HandleEintr(read, EventFD_, &count, sizeof(count));
    // For edge-triggered one could clear multiple events, others get nothing.
    YT_VERIFY(ret == sizeof(count) || (ret < 0 && errno == EAGAIN));
#else
    while (true) {
        char c;
        auto ret = HandleEintr(read, PipeFDs_[0], &c, sizeof(c));
        YT_VERIFY(ret == sizeof(c) || (ret < 0 && errno == EAGAIN));
        if (ret < 0) {
            break;
        }
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
