#include "action_queue.h"

#include <yt/yt/core/actions/bind.h>

namespace NYT::NLsm::NTesting {

////////////////////////////////////////////////////////////////////////////////

class TActionQueue
    : public IActionQueue
{
public:
    explicit TActionQueue(TInstant now)
        : Now_(now)
    { }

    void Schedule(TClosure callback, TDuration delay) override
    {
        Callbacks_.push(TDelayedCallback{callback, Now_ + delay});
    }

    void Run() override
    {
        RunUntil(TInstant::Max());
    }

    void RunUntil(TInstant until) override
    {
        while (!Callbacks_.empty()) {
            if (Stopped_) {
                return;
            }

            auto top = Callbacks_.top();
            if (top.Time > until) {
                break;
            }

            Callbacks_.pop();
            Now_ = top.Time;
            // YT_LOG_INFO("Running callback (Time: %v)", Now_);
            top.Callback();
        }

        Now_ = until;
    }

    void RunFor(TDuration duration) override
    {
        RunUntil(Now_ + duration);
    }

    void Stop() override
    {
        Stopped_ = true;
    }

    bool IsStopped() const override
    {
        return Stopped_;
    }

    TInstant GetNow() const override
    {
        return Now_;
    }

private:
    struct TDelayedCallback
    {
        TClosure Callback;
        TInstant Time;

        bool operator<(const TDelayedCallback& other) const
        {
            return Time > other.Time;
        }
    };

    std::priority_queue<TDelayedCallback> Callbacks_;
    TInstant Now_;
    bool Stopped_ = false;
};

////////////////////////////////////////////////////////////////////////////////

IActionQueuePtr CreateActionQueue(TInstant now)
{
    return New<TActionQueue>(now);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
