#include "relative_replication_throttler.h"

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/misc/ring_queue.h>

namespace NYT::NTabletNode {

using namespace NTransactionClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TRelativeReplicationThrottler
    : public IRelativeReplicationThrottler
{
public:
    explicit TRelativeReplicationThrottler(TRelativeReplicationThrottlerConfigPtr config)
        : Config_(std::move(config))
    { }

    void OnReplicationBatchProcessed(TTimestamp recordTimestamp) override
    {
        if (!Config_->Enable) {
            return;
        }

        auto recordTime = TimestampToInstant(recordTimestamp).second;
        auto now = TInstant::Now();

        if (Queue_.empty()) {
            Queue_.push({recordTime, now});
            AllowedTime_ = TInstant::Zero();
            return;
        }

        if (recordTime - Queue_.back().LogRowRecordTime > Config_->ActivationThreshold) {
            Queue_.clear();
            Queue_.push({recordTime, now});
            AllowedTime_ = TInstant::Zero();
            return;
        }

        auto recordDelta = recordTime - Queue_.front().LogRowRecordTime;
        double correction = 1.0 * (std::ssize(Queue_) + 1) / std::ssize(Queue_);
        AllowedTime_ = std::max(
            AllowedTime_,
            Queue_.front().ReplicationTime + recordDelta / Config_->Ratio * correction);

        Queue_.push({recordTime, now});

        while (std::ssize(Queue_) > Config_->MaxTimestampsToKeep) {
            Queue_.pop();
        }

        while (std::ssize(Queue_) > 1) {
            auto delta = Queue_.back().ReplicationTime - Queue_.front().ReplicationTime;
            if (delta > Config_->WindowSize) {
                Queue_.pop();
            } else {
                break;
            }
        }
    }

    TFuture<void> Throttle() const override
    {
        if (!Config_->Enable) {
            return VoidFuture;
        }

        auto now = TInstant::Now();
        if (now > AllowedTime_) {
            return VoidFuture;
        }

        return TDelayedExecutor::MakeDelayed(AllowedTime_ - now);
    }

private:
    const TRelativeReplicationThrottlerConfigPtr Config_;

    struct TEntry
    {
        TInstant LogRowRecordTime;
        TInstant ReplicationTime;
    };

    TRingQueue<TEntry> Queue_;
    TInstant AllowedTime_ = TInstant::Zero();
};

////////////////////////////////////////////////////////////////////////////////

IRelativeReplicationThrottlerPtr CreateRelativeReplicationThrottler(
    TRelativeReplicationThrottlerConfigPtr config)
{
    return New<TRelativeReplicationThrottler>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
