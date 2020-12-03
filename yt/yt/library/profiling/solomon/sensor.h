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

static_assert(sizeof(TSimpleGauge) == 24);

////////////////////////////////////////////////////////////////////////////////

class TSimpleTimeGauge
    : public ITimeGaugeImpl
{
public:
    virtual void Update(TDuration value) override;

    TDuration GetValue();

private:
    std::atomic<TDuration::TValue> Value_ = 0.0;
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

static_assert(sizeof(TSimpleCounter) == 24);

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

static_assert(sizeof(TSimpleTimeCounter) == 24);

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TSimpleSummary
    : public ISummaryImplBase<T>
{
public:
    virtual void Record(T value) override;

    virtual TSummarySnapshot<T> GetValue() override;
    virtual TSummarySnapshot<T> GetValueAndReset() override;

private:
    TSpinLock Lock_;
    TSummarySnapshot<T> Value_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
