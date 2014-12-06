#pragma once

#include "public.h"

namespace NYT {
namespace NCGroup {

////////////////////////////////////////////////////////////////////////////////

class TEvent
    : private TNonCopyable
{
public:
    TEvent();
    ~TEvent();

    TEvent(TEvent&& other);

    bool Fired();

    void Clear();
    void Destroy();

    TEvent& operator=(TEvent&& other);

    i64 GetLastValue() const;

protected:
    TEvent(int eventFd, int fd = -1);

private:
    void Swap(TEvent& other);

    int EventFd_;
    int Fd_;
    bool Fired_ = false;
    i64 LastValue_;

    friend TMemory;
};

////////////////////////////////////////////////////////////////////////////////

} // NCGroup
} // NYT
