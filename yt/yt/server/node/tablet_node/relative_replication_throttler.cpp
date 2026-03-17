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

    void OnReplicationBatchProcessed(
        TTimestamp firstRecordTimestamp,
        TTimestamp lastRecordTimestamp) override
    {
        if (!Config_->Enable) {
            return;
        }

        auto firstRecordTime = TimestampToInstant(firstRecordTimestamp).second;
        auto lastRecordTime = TimestampToInstant(lastRecordTimestamp).second;
        auto now = TInstant::Now();

        if (Queue_.empty()) {
            Queue_.push({lastRecordTime, now});
            AllowedTime_ = TInstant::Zero();
            return;
        }

        if (firstRecordTime - Queue_.back().LogRowRecordTime > Config_->ActivationThreshold) {
            Queue_.clear();
            Queue_.push({lastRecordTime, now});
            AllowedTime_ = TInstant::Zero();
            return;
        }

        auto recordDelta = lastRecordTime - Queue_.front().LogRowRecordTime;
        AllowedTime_ = std::max(
            AllowedTime_,
            Queue_.front().ReplicationTime + recordDelta / GetCorrectedRatio());

        Queue_.push({lastRecordTime, now});

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
            return OKFuture;
        }

        auto now = TInstant::Now();
        if (now > AllowedTime_) {
            return OKFuture;
        }

        return TDelayedExecutor::MakeDelayed(AllowedTime_ - now);
    }

    TInstant GetMaxAllowedRecordTime(
        TInstant now,
        TTimestamp currentTimestamp,
        TDuration replicationTickPeriod) const override
    {
        if (!Config_->Enable) {
            return TInstant::Max();
        }

        if (Queue_.empty()) {
            return GetDefaultMaxAllowedInstant(currentTimestamp, replicationTickPeriod);
        }

        const auto& entry = Queue_.back();
        if (now <= entry.ReplicationTime) {
            return GetDefaultMaxAllowedInstant(currentTimestamp, replicationTickPeriod);
        }

        return entry.LogRowRecordTime + (now - entry.ReplicationTime) * GetCorrectedRatio();
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

    TInstant GetDefaultMaxAllowedInstant(TTimestamp currentTimestamp, TDuration replicationTickPeriod) const
    {
        return TimestampToInstant(currentTimestamp).second + replicationTickPeriod * Config_->Ratio;
    }

    double GetCorrectedRatio() const
    {
        i64 queueSize = std::ssize(Queue_);
        return Config_->Ratio * (1.0 *  queueSize / (queueSize + 1));
    }
};

////////////////////////////////////////////////////////////////////////////////

IRelativeReplicationThrottlerPtr CreateRelativeReplicationThrottler(
    TRelativeReplicationThrottlerConfigPtr config)
{
    return New<TRelativeReplicationThrottler>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
