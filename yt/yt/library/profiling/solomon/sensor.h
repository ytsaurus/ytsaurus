#pragma once

#include <yt/yt/library/profiling/impl.h>
#include <yt/yt/library/profiling/summary.h>

#include <util/system/spinlock.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

class TSimpleGauge
    : public IGaugeImpl
{
public:
    virtual void Update(double value) override;

    double GetValue();

private:
    std::atomic<double> Value_ = 0.0;
};

////////////////////////////////////////////////////////////////////////////////

class TSimpleCounter
    : public ICounterImpl
{
public:
    virtual void Increment(i64 delta) override;
    
    i64 GetValue();

private:
    std::atomic<i64> Value_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TSimpleTimeCounter
    : public ITimeCounterImpl
{
public:
    virtual void Add(TDuration delta) override;
    
    TDuration GetValue();

private:
    std::atomic<TDuration::TValue> Value_{0};
};

////////////////////////////////////////////////////////////////////////////////

class TSimpleSummary
    : public ISummaryImpl
{
public:
    virtual void Record(double value) override;

    TSummarySnapshot<double> GetValue();
    TSummarySnapshot<double> GetValueAndReset();

private:
    TAdaptiveLock Lock_;
    TSummarySnapshot<double> Value_;
};

////////////////////////////////////////////////////////////////////////////////

class TSimpleTimer
    : public ITimerImpl
{
public:
    virtual void Record(TDuration value) override;

    TSummarySnapshot<TDuration> GetValue();
    TSummarySnapshot<TDuration> GetValueAndReset();

private:
    TAdaptiveLock Lock_;
    TSummarySnapshot<TDuration> Value_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
