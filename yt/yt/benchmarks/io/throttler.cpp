#include "throttler.h"

#include <yt/yt/core/profiling/timing.h>

#include <util/random/random.h>

#include <utility>
#include <deque>

namespace NYT::NIOTest {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

class TBasicThrottler
    : public IThrottler
{
public:
    TBasicThrottler(TThrottlerConfigPtr config)
        : Config_(std::move(config))
    { }

    bool IsAvailable(i64 value) override
    {
        Drain();
        value = ConvertValue(value);
        i64 limit = Config_->Limit;
        i64 current = Value_;
        return current <= limit && value <= limit - current;
    }

    void Acquire(i64 value) override
    {
        Drain();
        value = ConvertValue(value);
        Value_ += value;
        Values_.emplace_back(GetCpuInstant(), value);
    }

protected:
    virtual i64 ConvertValue(i64 value) = 0;

private:
    const TThrottlerConfigPtr Config_;
    std::deque<std::pair<TCpuInstant, i64>> Values_;
    i64 Value_ = 0;

    void Drain()
    {
        auto now = GetCpuInstant();
        auto period = DurationToCpuDuration(Config_->Period);
        while (!Values_.empty() && now - Values_.front().first >= period) {
            Value_ -= Values_.front().second;
            Values_.pop_front();
        }
    }
};

class TThroughputThrottler
    : public TBasicThrottler
{
public:
    TThroughputThrottler(TThrottlerConfigPtr config)
        : TBasicThrottler(std::move(config))
    { }

protected:
    i64 ConvertValue(i64 value) override
    {
        return value;
    }
};

class TIopsThrottler
    : public TBasicThrottler
{
public:
    TIopsThrottler(TThrottlerConfigPtr config)
        : TBasicThrottler(std::move(config))
    { }

protected:
    i64 ConvertValue(i64 /*value*/) override
    {
        return 1;
    }
};

////////////////////////////////////////////////////////////////////////////////

IThrottlerPtr CreateThrottler(const TString& type, TThrottlerConfigPtr config)
{
    switch (ParseEnum<EThrottlerType>(type)) {
        case EThrottlerType::Throughput:
            return New<TThroughputThrottler>(std::move(config));

        case EThrottlerType::Iops:
            return New<TIopsThrottler>(std::move(config));

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

class TCombinedThrottler
    : public IThrottler
{
public:
    TCombinedThrottler(std::vector<IThrottlerPtr> throttlers)
        : Throtters_(std::move(throttlers))
    { }

    bool IsAvailable(i64 value) override
    {
        for (auto& throttler : Throtters_) {
            if (!throttler->IsAvailable(value)) {
                return false;
            }
        }
        return true;
    }

    void Acquire(i64 value) override
    {
        for (auto& throttler : Throtters_) {
            throttler->Acquire(value);
        }
    }

private:
    std::vector<IThrottlerPtr> Throtters_;
};

IThrottlerPtr CreateCombinedThrottler(const TCombinedThrottlerConfig& throttlerConfigs)
{
    std::vector<IThrottlerPtr> throttlers;
    for (const auto& [type, config] : throttlerConfigs) {
        throttlers.push_back(CreateThrottler(type, config));
    }

    return New<TCombinedThrottler>(std::move(throttlers));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
