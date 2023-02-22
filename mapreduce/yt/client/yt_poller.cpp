#include "yt_poller.h"

#include <mapreduce/yt/raw_client/raw_batch_request.h>
#include <mapreduce/yt/raw_client/raw_requests.h>

#include <mapreduce/yt/common/debug_metrics.h>
#include <mapreduce/yt/common/retry_lib.h>
#include <mapreduce/yt/common/wait_proxy.h>

#include <mapreduce/yt/http/retry_request.h>

#include <mapreduce/yt/interface/config.h>

#include <mapreduce/yt/interface/logging/yt_log.h>

namespace NYT {
namespace NDetail {

using namespace NRawClient;

////////////////////////////////////////////////////////////////////////////////

TYtPoller::TYtPoller(TAuth auth, const IClientRetryPolicyPtr& retryPolicy)
    : Auth_(std::move(auth))
    , ClientRetryPolicy_(retryPolicy)
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


void TYtPoller::Stop()
{
    {
        auto g = Guard(Lock_);
        if (!IsRunning_) {
            return;
        }
        IsRunning_ = false;
        HasData_.Signal();
    }
    WaiterThread_.Join();
}

void TYtPoller::DiscardQueuedItems()
{
    for (auto& item : Pending_) {
        item->OnItemDiscarded();
    }
    for (auto& item : InProgress_) {
        item->OnItemDiscarded();
    }
}

void TYtPoller::WatchLoop()
{
    TInstant nextRequest = TInstant::Zero();
    while (true) {
        {
            auto g = Guard(Lock_);
            if (IsRunning_ && Pending_.empty() && InProgress_.empty()) {
                TWaitProxy::Get()->WaitCondVar(HasData_, Lock_);
            }

            if (!IsRunning_) {
                DiscardQueuedItems();
                return;
            }

            {
                auto ug = Unguard(Lock_);  // allow adding new items into Pending_
                TWaitProxy::Get()->SleepUntil(nextRequest);
                nextRequest = TInstant::Now() + TConfig::Get()->WaitLockPollInterval;
            }
            if (!Pending_.empty()) {
                InProgress_.splice(InProgress_.end(), Pending_);
            }
            Y_VERIFY(!InProgress_.empty());
        }

        TRawBatchRequest rawBatchRequest;

        for (auto& item : InProgress_) {
            item->PrepareRequest(&rawBatchRequest);
        }

        try {
            ExecuteBatch(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, rawBatchRequest);
        } catch (const yexception& ex) {
            YT_LOG_ERROR("Exception while executing batch request: %v", ex.what());
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

        IncDebugMetric(TStringBuf("yt_poller_top_loop_repeat_count"));
    }
}

void* TYtPoller::WatchLoopProc(void* data)
{
    static_cast<TYtPoller*>(data)->WatchLoop();
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
