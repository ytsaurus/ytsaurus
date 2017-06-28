#include "lock_waiter.h"

#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/http/error.h>
#include <mapreduce/yt/interface/client.h>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

struct TLockWaiter::TItem {
public:
    const TLockId LockId;
    NThreading::TPromise<void> Acquired;

    NThreading::TFuture<TNode> LockState;

public:
    TItem(TLockId lockId, NThreading::TPromise<void> acquired)
        : LockId(std::move(lockId))
        , Acquired(std::move(acquired))
    { }
};

////////////////////////////////////////////////////////////////////////////////

TLockWaiter::TLockWaiter(IClient* client)
    : Client_(client)
    , WaiterThread_(&TLockWaiter::WatchLoopProc, this)
{
    WaiterThread_.Start();
}

TLockWaiter::~TLockWaiter()
{
    Stop();
}

void TLockWaiter::Watch(const TLockId& lockId, NThreading::TPromise<void> acquired)
{
    auto g = Guard(Lock_);
    Pending_.emplace_back(lockId, std::move(acquired));
    HasData_.Signal();
}

void TLockWaiter::WatchLoop()
{
    TInstant nextRequest = TInstant::Zero();
    while (true) {
        {
            auto g = Guard(Lock_);
            if (IsRunning_ && Pending_.empty() && InProgress_.empty()) {
                HasData_.Wait(Lock_);
            }

            if (!IsRunning_) {
                return;
            }

            SleepUntil(nextRequest);
            nextRequest = TInstant::Now() + TConfig::Get()->WaitLockPollInterval;
            if (!Pending_.empty()) {
                InProgress_.splice(InProgress_.end(), Pending_);
            }
            Y_VERIFY(!InProgress_.empty());
        }

        TBatchRequest batchRequest;

        for (auto& item : InProgress_) {
            TString path = "//sys/locks/" + GetGuidAsString(item.LockId) + "/@state";
            item.LockState = batchRequest.Get(path);
        }

        Client_->ExecuteBatch(batchRequest);

        for (auto it = InProgress_.begin(); it != InProgress_.end();) {
            auto& item = *it;
            bool shouldRemoveCurrentItem = false;

            try {
                const auto& state = item.LockState.GetValue().AsString();
                if (state == "acquired") {
                    item.Acquired.SetValue();
                    shouldRemoveCurrentItem = true;
                }
            } catch (const TErrorResponse& e) {
                if (!e.IsRetriable()) {
                    item.Acquired.SetException(std::current_exception());
                    shouldRemoveCurrentItem = true;
                }
            }

            if (shouldRemoveCurrentItem) {
                it = InProgress_.erase(it);
            } else {
                ++it;
            }
        }
    }
}

void* TLockWaiter::WatchLoopProc(void* data)
{
    static_cast<TLockWaiter*>(data)->WatchLoop();
    return nullptr;
}

void TLockWaiter::Stop()
{
    auto g = Guard(Lock_);
    IsRunning_ = false;
    HasData_.Signal();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
