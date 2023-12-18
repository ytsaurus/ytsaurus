#include "statistics.h"

#include <yt/yt/core/profiling/timing.h>

#include <cmath>

namespace NYT::NIOTest {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TAggregateLatency::TAggregateLatency()
    : MinLatency(TDuration::Max())
{ }

void TAggregateLatency::Update(TDuration latency)
{
    MinLatency = std::min(MinLatency, latency);
    MaxLatency = std::max(MaxLatency, latency);
    SumLatency += latency;
    SumSquareLatency += TDuration::FromValue(latency.GetValue() * latency.GetValue());
    ++Count;
}

TAggregateLatency& TAggregateLatency::operator+=(const TAggregateLatency& other)
{
    MinLatency = std::min(MinLatency, other.MinLatency);
    MaxLatency = std::max(MaxLatency, other.MaxLatency);
    SumLatency += other.SumLatency;
    SumSquareLatency += other.SumSquareLatency;
    Count += other.Count;
    return *this;
};

////////////////////////////////////////////////////////////////////////////////

void TLatencyHistogram::Update(TDuration latency)
{
    auto microseconds = latency.MicroSeconds();
    int index = microseconds >= Scale
        ? std::log(microseconds) / std::log(Scale)
        : 0;
    if (std::ssize(Bins) <= index) {
        Bins.resize(index + 1);
    }
    ++Bins[index];
}

TLatencyHistogram& TLatencyHistogram::operator+=(const TLatencyHistogram& other)
{
    if (Bins.size() < other.Bins.size()) {
        Bins.resize(other.Bins.size());
    }
    for (int index = 0; index < std::ssize(other.Bins); ++index) {
        Bins[index] += other.Bins[index];
    }
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

void TOperationTypeStatistics::Update(const TOperation& operation, TDuration latency)
{
    Latency.Update(latency);
    LatencyHistogram.Update(latency);
    BytesTransmitted += operation.Size;
    ++OperationCount;
}

TOperationTypeStatistics& TOperationTypeStatistics::operator+=(const TOperationTypeStatistics& other)
{
    Latency += other.Latency;
    LatencyHistogram += other.LatencyHistogram;
    BytesTransmitted += other.BytesTransmitted;
    OperationCount += other.OperationCount;
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

TQuantumStatistics::TQuantumStatistics()
    : RusageMeter_(ERusageWho::Thread)
{ }

void TQuantumStatistics::Update(const TOperation& operation, TDuration latency)
{
    Operation[operation.Type].Update(operation, latency);
    ++OperationCount;
}

void TQuantumStatistics::Finish()
{
    Rusage = RusageMeter_.Tick();
}

TQuantumStatistics& TQuantumStatistics::operator+=(const TQuantumStatistics& other)
{
    Rusage += other.Rusage;
    OperationCount += other.OperationCount;
    for (auto type : TEnumTraits<EOperationType>::GetDomainValues()) {
        Operation[type] += other.Operation[type];
    }
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

TTimeSeries::TTimeSeries(TInstant start, TDuration granularity)
    : Start(start)
    , Granularity(granularity)
    , Quants{1}
{ }

void TTimeSeries::Update(const TOperation& operation, TDuration latency)
{
    i64 index = (CpuInstantToInstant(GetCpuInstant()) - Start) / Granularity;
    if (std::ssize(Quants) <= index) {
        Finish();
        Quants.resize(index + 1);
    }
    Quants[index].Update(operation, latency);
}

void TTimeSeries::Finish()
{
    if (!Quants.empty()) {
        Quants.back().Finish();
    }
}

TTimeSeries& TTimeSeries::operator+=(const TTimeSeries& other)
{
    if (Quants.size() < other.Quants.size()) {
        Quants.resize(other.Quants.size());
    }
    for (int index = 0; index < std::ssize(other.Quants); ++index) {
        Quants[index] += other.Quants[index];
    }
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

TWholeRusage::TWholeRusage()
{ }

TWholeRusage::TWholeRusage(TRusage processRusage, TRusage pollerRusage)
    : ProcessRusage(processRusage)
    , KernelPollerRusage(pollerRusage)
    , ProcessAndPollerRusage(processRusage + pollerRusage)
{ }

TWholeRusage& TWholeRusage::operator+=(const TWholeRusage& other)
{
    ProcessRusage += other.ProcessRusage;
    KernelPollerRusage += other.KernelPollerRusage;
    ProcessAndPollerRusage += other.ProcessAndPollerRusage;
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

TRusageTimeSeries::TRusageTimeSeries(TInstant start, TDuration granularity)
    : Start(start)
    , Granularity(granularity)
{ }

void TRusageTimeSeries::Update(TRusage processRusage, TRusage pollerRusage)
{
    i64 index = (CpuInstantToInstant(GetCpuInstant()) - Start) / Granularity;
    if (std::ssize(Rusages) <= index) {
        Rusages.resize(index + 1);
    }

    auto rusage = TWholeRusage(processRusage, pollerRusage);
    Rusages[index] += rusage;
    Rusage += rusage;
}

TRusageTimeSeries& TRusageTimeSeries::operator+=(const TRusageTimeSeries& other)
{
    if (Rusages.size() < other.Rusages.size()) {
        Rusages.resize(other.Rusages.size());
    }
    for (int index = 0; index < std::ssize(other.Rusages); ++index) {
        Rusages[index] += other.Rusages[index];
    }
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

TStatistics::TStatistics(TInstant start, TDuration granularity)
    : TimeSeries(start, granularity)
    , RusageTimeSeries(start, granularity)
{ }

void TStatistics::Update(const TOperation& operation, TDuration latency)
{
    Total.Update(operation, latency);
    TimeSeries.Update(operation, latency);
}

void TStatistics::UpdateRusage(TRusage processRusage, TRusage pollerRusage)
{
    RusageTimeSeries.Update(processRusage, pollerRusage);
}

void TStatistics::Finish()
{
    Total.Finish();
    TimeSeries.Finish();
}

TStatistics& TStatistics::operator+=(const TStatistics& other)
{
    Total += other.Total;
    TimeSeries += other.TimeSeries;
    RusageTimeSeries += other.RusageTimeSeries;
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
