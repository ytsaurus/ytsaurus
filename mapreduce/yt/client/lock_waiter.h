#pragma once

#include <mapreduce/yt/interface/fwd.h>

#include <library/threading/future/future.h>

#include <util/generic/list.h>
#include <util/generic/ptr.h>

#include <util/system/condvar.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

class TLockWaiter
    : public TThrRefBase
{
public:
    explicit TLockWaiter(IClientPtr Client_);
    ~TLockWaiter();

    void Watch(const TLockId& lockId, NThreading::TPromise<void> acquired);

private:
    void WatchLoop();
    static void* WatchLoopProc(void*);
    void Stop();

private:
    struct TItem;

    IClientPtr const Client_;

    ylist<TItem> InProgress_;
    ylist<TItem> Pending_;

    TThread WaiterThread_;
    TMutex Lock_;
    TCondVar HasData_;

    bool IsRunning_ = true;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
