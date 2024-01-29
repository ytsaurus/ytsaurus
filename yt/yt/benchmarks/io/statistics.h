#pragma once

#include "iotest.h"
#include "rusage.h"
#include "meters.h"

#include <library/cpp/yt/containers/enum_indexed_array.h>

#include <util/datetime/base.h>

namespace NYT::NIOTest {

////////////////////////////////////////////////////////////////////////////////

struct TAggregateLatency
{
    TDuration MinLatency;
    TDuration MaxLatency;
    TDuration SumLatency;
    TDuration SumSquareLatency;
    i64 Count = 0;

    TAggregateLatency();

    void Update(TDuration latency);

    TAggregateLatency& operator+=(const TAggregateLatency& other);
};

struct TLatencyHistogram
{
    static constexpr double Scale = 1.1;
    std::vector<i64> Bins;

    void Update(TDuration latency);

    TLatencyHistogram& operator+=(const TLatencyHistogram& other);
};

struct TOperationTypeStatistics
{
    TAggregateLatency Latency;
    TLatencyHistogram LatencyHistogram;
    i64 BytesTransmitted = 0;
    i64 OperationCount = 0;

    void Update(const TOperation& operation, TDuration latency);

    TOperationTypeStatistics& operator+=(const TOperationTypeStatistics& other);
};

struct TQuantumStatistics
{
    TEnumIndexedArray<EOperationType, TOperationTypeStatistics> Operation;
    i64 OperationCount = 0;
    TRusage Rusage;

    TQuantumStatistics();

    void Update(const TOperation& operation, TDuration latency);
    void Finish();

    TQuantumStatistics& operator+=(const TQuantumStatistics& other);

private:
    TRusageMeter RusageMeter_;
};

struct TTimeSeries
{
    TInstant Start;
    TDuration Granularity;
    std::vector<TQuantumStatistics> Quants;

    TTimeSeries(TInstant start, TDuration granularity);

    void Update(const TOperation& operation, TDuration latency);
    void Finish();

    TTimeSeries& operator+=(const TTimeSeries& other);
};

struct TWholeRusage
{
    TRusage ProcessRusage;
    TRusage KernelPollerRusage;
    TRusage ProcessAndPollerRusage;

    TWholeRusage();
    TWholeRusage(TRusage processRusage, TRusage pollerRusage);

    TWholeRusage& operator+=(const TWholeRusage& other);
};

struct TRusageTimeSeries
{
    TInstant Start;
    TDuration Granularity;
    std::vector<TWholeRusage> Rusages;
    TWholeRusage Rusage;

    TRusageTimeSeries(TInstant start, TDuration granularity);

    void Update(TRusage processRusage, TRusage pollerRusage);

    TRusageTimeSeries& operator+=(const TRusageTimeSeries& other);
};

struct TStatistics
{
    TQuantumStatistics Total;
    TTimeSeries TimeSeries;
    TRusageTimeSeries RusageTimeSeries;

    explicit TStatistics(TInstant start, TDuration granularity = TDuration::Seconds(1));

    void Update(const TOperation& operation, TDuration latency);
    void UpdateRusage(TRusage processRusage, TRusage pollerRusage);
    void Finish();

    TStatistics& operator+=(const TStatistics& other);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
