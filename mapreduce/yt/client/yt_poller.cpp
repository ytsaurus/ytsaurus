#include "yt_poller.h"

#include <mapreduce/yt/raw_client/raw_batch_request.h>
#include <mapreduce/yt/raw_client/raw_requests.h>

#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/debug_metrics.h>
#include <mapreduce/yt/common/wait_proxy.h>

#include <mapreduce/yt/http/retry_request.h>

#include <mapreduce/yt/interface/logging/log.h>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

TYtPoller::TYtPoller(TAuth auth)
    : Auth_(std::move(auth))
    , WaiterThread_(&TYtPoller::WatchLoopProc, this)
{
    WaiterThread_.Start();
}

TYtPoller::~TYtPoller()
{
    Stop();
}

void TYtPoller::Watch(IYtPollerItemPtr item)
{
    auto g = Guard(Lock_);
    Pending_.emplace_back(std::move(item));
    HasData_.Signal();
}

void TYtPoller::WatchLoop()
{
    TInstant nextRequest = TInstant::Zero();
    while (true) {
        {
            auto g = Guard(Lock_);
            if (IsRunning_ && Pending_.empty() && InProgress_.empty()) {
                TWaitProxy::WaitCondVar(HasData_, Lock_);
            }

            if (!IsRunning_) {
                return;
            }

            TWaitProxy::SleepUntil(nextRequest);
            nextRequest = TInstant::Now() + TConfig::Get()->WaitLockPollInterval;
            if (!Pending_.empty()) {
                InProgress_.splice(InProgress_.end(), Pending_);
            }
            Y_VERIFY(!InProgress_.empty());
        }

        TRawBatchRequest rawBatchRequest;

        for (auto& item : InProgress_) {
            item->PrepareRequest(&rawBatchRequest);
        }

        TAttemptLimitedRetryPolicy retryPolicy(TConfig::Get()->RetryCount);
        try {
            ExecuteBatch(
                Auth_,
                rawBatchRequest,
                TExecuteBatchOptions(),
                retryPolicy);
        } catch (const yexception& ex) {
            LOG_ERROR("Exception while executing batch request: %s", ex.what());
        }

        for (auto it = InProgress_.begin(); it != InProgress_.end();) {
            auto& item = *it;

            IYtPollerItem::EStatus status = item->OnRequestExecuted();

            if (status == IYtPollerItem::PollBreak) {
                it = InProgress_.erase(it);
            } else {
                ++it;
            }
        }

        IncDebugMetric(AsStringBuf("yt_poller_top_loop_repeat_count"));
    }
}

void* TYtPoller::WatchLoopProc(void* data)
{
    static_cast<TYtPoller*>(data)->WatchLoop();
    return nullptr;
}

void TYtPoller::Stop()
{
    {
        auto g = Guard(Lock_);
        IsRunning_ = false;
        HasData_.Signal();
    }
    WaiterThread_.Join();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
