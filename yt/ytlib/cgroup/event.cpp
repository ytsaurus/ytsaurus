#include "stdafx.h"
#include "private.h"
#include "event.h"

namespace NYT {
namespace NCGroup {

////////////////////////////////////////////////////////////////////////////////

static const int InvalidFd = -1;

////////////////////////////////////////////////////////////////////////////////

TEvent::TEvent(int eventFd, int fd)
    : EventFd_(eventFd)
    , Fd_(fd)
{ }

TEvent::TEvent()
    : TEvent(InvalidFd, InvalidFd)
{ }

TEvent::TEvent(TEvent&& other)
    : TEvent()
{
    Swap(other);
}

TEvent::~TEvent()
{
    Destroy();
}

TEvent& TEvent::operator=(TEvent&& other)
{
    if (this == &other) {
        return *this;
    }
    Destroy();
    Swap(other);
    return *this;
}

bool TEvent::Fired()
{
    YCHECK(EventFd_ != InvalidFd);

    if (Fired_) {
        return true;
    }

    auto bytesRead = ::read(EventFd_, &LastValue_, sizeof(LastValue_));

    if (bytesRead < 0) {
        if (errno == EWOULDBLOCK || errno == EAGAIN) {
            return false;
        }
        THROW_ERROR TError::FromSystem();
    }
    YCHECK(bytesRead == sizeof(LastValue_));
    Fired_ = true;
    return true;
}

void TEvent::Clear()
{
    Fired_ = false;
}

void TEvent::Destroy()
{
    Clear();
    if (EventFd_ != InvalidFd) {
        ::close(EventFd_);
    }
    EventFd_ = InvalidFd;

    if (Fd_ != InvalidFd) {
        ::close(Fd_);
    }
    Fd_ = InvalidFd;
}

i64 TEvent::GetLastValue() const
{
    return LastValue_;
}

void TEvent::Swap(TEvent& other)
{
    std::swap(EventFd_, other.EventFd_);
    std::swap(Fd_, other.Fd_);
    std::swap(Fired_, other.Fired_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCGroup
} // namespace NYT
