#include "lock.h"

#include "yt_poller.h"

#include <mapreduce/yt/http/retry_request.h>

#include <mapreduce/yt/raw_client/raw_batch_request.h>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

class TLockPollerItem
    : public IYtPollerItem
{
public:
    TLockPollerItem(const TLockId& lockId, NThreading::TPromise<void> acquired)
        : LockStateYPath_("//sys/locks/" + GetGuidAsString(lockId) + "/@state")
        , Acquired_(acquired)
    { }

    virtual void PrepareRequest(TRawBatchRequest* batchRequest) override
    {
        LockState_ = batchRequest->Get(TTransactionId(), LockStateYPath_, TGetOptions());
    }

    virtual EStatus OnRequestExecuted() override
    {
        try {
            const auto& state = LockState_.GetValue().AsString();
            if (state == "acquired") {
                Acquired_.SetValue();
                return PollBreak;
            }
        } catch (const TErrorResponse& e) {
            if (!IsRetriable(e)) {
                Acquired_.SetException(std::current_exception());
                return PollBreak;
            }
        }
        return PollContinue;
    }

private:
    const TString LockStateYPath_;
    NThreading::TPromise<void> Acquired_;

    NThreading::TFuture<TNode> LockState_;
};

////////////////////////////////////////////////////////////////////////////////

TLock::TLock(const TLockId& lockId)
    : LockId_(lockId)
    , Acquired_(NThreading::MakeFuture())
    , Client_(nullptr)
{ }

TLock::TLock(const TLockId& lockId, TClientPtr client)
    : LockId_(lockId)
    , Client_(std::move(client))
{ }

const TLockId& TLock::GetId() const
{
    return LockId_;
}

const NThreading::TFuture<void>& TLock::GetAcquiredFuture() const
{
    if (!Acquired_) {
        auto promise = NThreading::NewPromise<void>();
        Client_->GetYtPoller().Watch(::MakeIntrusive<TLockPollerItem>(LockId_, promise));
        Acquired_ = promise.GetFuture();
    }
    return *Acquired_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
