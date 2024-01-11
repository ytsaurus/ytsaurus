#pragma once

#include "action_queue.h"

namespace NYT::NLsm::NTesting {

////////////////////////////////////////////////////////////////////////////////

class TThrottler
{
public:
    explicit TThrottler(IActionQueuePtr actionQueue, i64 limit = 0)
        : Capacity_(limit)
        , ActionQueue_(std::move(actionQueue))
    {
        UpdateLimits();
    }

    bool IsOverdraft()
    {
        UpdateLimits();
        return Limit_ < 0;
    }

    i64 GetAvailable()
    {
        UpdateLimits();
        return std::max<i64>(Limit_, 0);
    }

    void Acquire(i64 value)
    {
        UpdateLimits();
        Limit_ -= value;
    }

    void SetLimit(i64 value)
    {
        Capacity_ = value;
        Limit_ = std::max<i64>(Limit_, 0);
    }

private:
    i64 Capacity_;
    const IActionQueuePtr ActionQueue_;

    i64 Limit_ = 0;
    TInstant LastUpdated_ = TInstant::Zero();

    void UpdateLimits()
    {
        auto timeDelta = std::min(TDuration::Seconds(1), ActionQueue_->GetNow() - LastUpdated_);
        i64 increase = 1.0 * Capacity_ * timeDelta.GetValue() / TDuration::Seconds(1).GetValue();
        Limit_ = std::min(Capacity_, Limit_ + increase);
        LastUpdated_ = ActionQueue_->GetNow();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
