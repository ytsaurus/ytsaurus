#pragma once

#include "histogram_snapshot.h"

#include <yt/yt/library/profiling/impl.h>
#include <yt/yt/library/profiling/summary.h>

#include <util/system/spinlock.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

class TSimpleGauge
    : public IGaugeImpl
    , public ISummaryImpl
{
public:
    virtual void Update(double value) override;

    virtual double GetValue() override;

    virtual void Record(double value) override;

    virtual TSummarySnapshot<double> GetSummary() override;
    virtual TSummarySnapshot<double> GetSummaryAndReset() override;

private:
    std::atomic<double> Value_ = 0.0;
};

////////////////////////////////////////////////////////////////////////////////

class TSimpleTimeGauge
    : public ITimeGaugeImpl
{
public:
    virtual void Update(TDuration value) override;

    virtual TDuration GetValue() override;

private:
    std::atomic<TDuration::TValue> Value_ = 0.0;
};

////////////////////////////////////////////////////////////////////////////////

class TSimpleCounter
    : public ICounterImpl
{
public:
    virtual void Increment(i64 delta) override;
    
    virtual i64 GetValue() override;

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

    virtual TSummarySnapshot<T> GetSummary() override;
    virtual TSummarySnapshot<T> GetSummaryAndReset() override;

private:
    TSpinLock Lock_;
    TSummarySnapshot<T> Value_;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(THistogram)

class THistogram
    : public ISummaryImplBase<TDuration>
{
public:
    THistogram(const TSensorOptions& options);

    virtual void Record(TDuration value) override;

    THistogramSnapshot GetSnapshotAndReset();

private:
    std::vector<TDuration> Bounds_;
    std::vector<std::atomic<int>> Buckets_;

    // These to methods are not used.
    virtual TSummarySnapshot<TDuration> GetSummary() override;
    virtual TSummarySnapshot<TDuration> GetSummaryAndReset() override;
};

DEFINE_REFCOUNTED_TYPE(THistogram)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
