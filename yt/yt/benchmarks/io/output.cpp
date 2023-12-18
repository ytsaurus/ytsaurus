#include "output.h"
#include "configuration.h"

namespace NYT::NIOTest {

////////////////////////////////////////////////////////////////////////////////

TFormattedOutput::TFormattedOutput()
{
    RegisterParameter("name", Name);
    RegisterParameter("info", Info);
    RegisterParameter("uname", Uname);
    RegisterParameter("config", Config);
    RegisterParameter("epochs", Epochs);
}

TFormattedEpoch::TFormattedEpoch()
{
    RegisterParameter("epoch_name", EpochName);
    RegisterParameter("config", Config);
    RegisterParameter("generations", Generations);
}

TFormattedGeneration::TFormattedGeneration()
{
    RegisterParameter("configuration", Configuration);
    RegisterParameter("iterations", Iterations);
    RegisterParameter("iteration_count", IterationCount);
    RegisterParameter("sum_for_all_iterations", AllIterationStatistics);
}

TFormattedConfiguration::TFormattedConfiguration()
{
    RegisterParameter("driver", Driver);
    RegisterParameter("threads", Threads);
    RegisterParameter("block_size_log", BlockSizeLog);
    RegisterParameter("range", Zone);
    RegisterParameter("pattern", Pattern);
    RegisterParameter("read_percentage", ReadPercentage);
    RegisterParameter("direct", Direct);
    RegisterParameter("sync", Sync);
    RegisterParameter("fallocate", Fallocate);
    RegisterParameter("oneshot", Oneshot);
    RegisterParameter("validate", Validate);
    RegisterParameter("loop", Loop);
    RegisterParameter("shot_count", ShotCount);
    RegisterParameter("time_limit", TimeLimit);
    RegisterParameter("transfer_limit", TransferLimit);
}

TFormattedConfigurationPtr TFormattedConfiguration::From(const TConfigurationPtr& configuration)
{
    auto formatted = New<TFormattedConfiguration>();
    formatted->Driver = TFormattedDriver::From(configuration->Driver);
    formatted->Threads = configuration->Threads;
    formatted->BlockSizeLog = configuration->BlockSizeLog;
    formatted->Zone = configuration->Zone;
    formatted->Pattern = configuration->Pattern;
    formatted->ReadPercentage = configuration->ReadPercentage;
    formatted->Direct = configuration->Direct;
    formatted->Sync = configuration->Sync;
    formatted->Fallocate = configuration->Fallocate;
    formatted->Oneshot = configuration->Oneshot;
    formatted->Validate = configuration->Validate;
    formatted->Loop = configuration->Loop;
    formatted->ShotCount = configuration->ShotCount;
    formatted->TimeLimit = configuration->TimeLimit;
    formatted->TransferLimit = configuration->TransferLimit;

    //std::vector<TFile> Files;
    return formatted;
}

TFormattedDriver::TFormattedDriver()
{
    RegisterParameter("type", Type);
    RegisterParameter("config", Config);
}

TFormattedDriverPtr TFormattedDriver::From(const TDriverDescription& driver)
{
    auto formatted = New<TFormattedDriver>();
    formatted->Type = driver.Type;
    formatted->Config = driver.Config;
    return formatted;
}

TFormattedIteration::TFormattedIteration()
{
    RegisterParameter("start", Start);
    RegisterParameter("duration", Duration);
    RegisterParameter("threads_aggregated", ThreadsAggregated);
    RegisterParameter("process_rusage", ProcessRusage);
    RegisterParameter("threads", Threads);
}

TFormattedRusageTimeSeries::TFormattedRusageTimeSeries()
{
    RegisterParameter("timestamps", Timestamps);
    RegisterParameter("rusage", Rusage);
}

TFormattedRusageTimeSeriesPtr TFormattedRusageTimeSeries::From(const TRusageTimeSeries& timeseries)
{
    auto formatted = New<TFormattedRusageTimeSeries>();
    formatted->Rusage = TFormattedWholeRusage::From(timeseries.Rusage);

    for (int index = 0; index < std::ssize(timeseries.Rusages); ++index) {
        formatted->Timestamps.emplace(
            (timeseries.Start + timeseries.Granularity * index).MilliSeconds(),
            TFormattedWholeRusage::From(timeseries.Rusages[index]));
    }
    return formatted;
}

TFormattedWholeRusage::TFormattedWholeRusage()
{
    RegisterParameter("process", Process);
    RegisterParameter("kernel_poller", KernelPoller);
    RegisterParameter("process_and_poller", ProcessAndPoller);
}

TFormattedWholeRusagePtr TFormattedWholeRusage::From(TWholeRusage wholeRusage)
{
    auto formatted = New<TFormattedWholeRusage>();
    formatted->Process = TFormattedRusage::From(wholeRusage.ProcessRusage);
    formatted->KernelPoller = TFormattedRusage::From(wholeRusage.KernelPollerRusage);
    formatted->ProcessAndPoller = TFormattedRusage::From(wholeRusage.ProcessAndPollerRusage);
    return formatted;
}

TFormattedRusage::TFormattedRusage()
{
    RegisterParameter("user_time", UserTime);
    RegisterParameter("system_time", SystemTime);
}

TFormattedRusagePtr TFormattedRusage::From(TRusage rusage)
{
    auto formatted = New<TFormattedRusage>();
    formatted->UserTime = rusage.UserTime.MicroSeconds();
    formatted->SystemTime = rusage.SystemTime.MicroSeconds();
    return formatted;
}

TFormattedStatistics::TFormattedStatistics()
{
    RegisterParameter("total", Total);
}

TFormattedStatisticsPtr TFormattedStatistics::From(const TStatistics& statistics)
{
    auto formatted = New<TFormattedStatistics>();
    formatted->Total = TFormattedQuantum::From(statistics.Total);
    formatted->Update(statistics.TimeSeries);
    return formatted;
}

TFormattedTimeSeries::TFormattedTimeSeries()
{
    RegisterParameter("timestamps", Timestamps);
}

void TFormattedTimeSeries::Update(const TTimeSeries& timeseries)
{
    for (int index = 0; index < std::ssize(timeseries.Quants); ++index) {
        if (timeseries.Quants[index].OperationCount > 0) {
            Timestamps.emplace(
                (timeseries.Start + timeseries.Granularity * index).MilliSeconds(),
                TFormattedQuantum::From(timeseries.Quants[index]));
        }
    }
}

TFormattedTimeSeriesPtr TFormattedTimeSeries::From(const TTimeSeries& timeseries)
{
    auto formatted = New<TFormattedTimeSeries>();
    formatted->Update(timeseries);
    return formatted;
}

TFormattedQuantum::TFormattedQuantum()
{
    RegisterParameter("rusage", Rusage);
    RegisterParameter("operations", Operations);
}

TFormattedQuantumPtr TFormattedQuantum::From(const TQuantumStatistics& quantum)
{
    auto formatted = New<TFormattedQuantum>();
    formatted->Rusage = TFormattedRusage::From(quantum.Rusage);
    for (auto type : TEnumTraits<EOperationType>::GetDomainValues()) {
        if (quantum.Operation[type].OperationCount > 0){
            formatted->Operations.emplace(
                TEnumTraits<EOperationType>::ToString(type),
                TFormattedOperationType::From(quantum.Operation[type]));
        }
    }
    return formatted;
}

TFormattedOperationType::TFormattedOperationType()
{
    RegisterParameter("latency", Latency);
    RegisterParameter("latency_histogram", LatencyHistogram);
    RegisterParameter("bytes_transmitted", BytesTransmitted);
    RegisterParameter("operation_count", OperationCount);
}

TFormattedOperationTypePtr TFormattedOperationType::From(const TOperationTypeStatistics& operation)
{
    auto formatted = New<TFormattedOperationType>();
    formatted->Latency = TFormattedLatency::From(operation.Latency);
    formatted->LatencyHistogram = TFormattedLatencyHistogram::From(operation.LatencyHistogram);
    formatted->BytesTransmitted = operation.BytesTransmitted;
    formatted->OperationCount = operation.OperationCount;
    return formatted;
}

TFormattedLatencyHistogram::TFormattedLatencyHistogram()
{
    RegisterParameter("scale", Scale);
    RegisterParameter("histogram", Histogram);
}

TFormattedLatencyHistogramPtr TFormattedLatencyHistogram::From(const TLatencyHistogram& histogram)
{
    auto formatted = New<TFormattedLatencyHistogram>();
    auto scale = histogram.Scale;
    formatted->Scale = scale;
    for (int index = 0; index < std::ssize(histogram.Bins); ++index) {
        if (histogram.Bins[index] != 0) {
            formatted->Histogram.emplace(
                static_cast<i64>(std::exp(std::log(scale) * index)),
                histogram.Bins[index]);
        }
    }
    return formatted;
}

TFormattedLatency::TFormattedLatency()
{
    RegisterParameter("avg", Avg);
    RegisterParameter("min", Min);
    RegisterParameter("max", Max);
    RegisterParameter("sum", Sum);
    RegisterParameter("sum_squared", SumSquare);
    RegisterParameter("count", Count);
}

TFormattedLatencyPtr TFormattedLatency::From(const TAggregateLatency& latency)
{
    auto formatted = New<TFormattedLatency>();
    formatted->Min = latency.MinLatency.MicroSeconds();
    formatted->Max = latency.MaxLatency.MicroSeconds();
    formatted->Sum = latency.SumLatency.MicroSeconds();
    formatted->SumSquare = latency.SumSquareLatency.MicroSeconds();
    formatted->Count = latency.Count;
    formatted->Avg = formatted->Sum / formatted->Count;
    return formatted;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
