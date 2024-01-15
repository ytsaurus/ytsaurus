#include "output.h"
#include "configuration.h"

namespace NYT::NIOTest {

////////////////////////////////////////////////////////////////////////////////

void TFormattedOutput::Register(TRegistrar registrar)
{
    registrar.Parameter("name", &TThis::Name);
    registrar.Parameter("info", &TThis::Info);
    registrar.Parameter("uname", &TThis::Uname);
    registrar.Parameter("config", &TThis::Config);
    registrar.Parameter("epochs", &TThis::Epochs);
}

void TFormattedEpoch::Register(TRegistrar registrar)
{
    registrar.Parameter("epoch_name", &TThis::EpochName);
    registrar.Parameter("config", &TThis::Config);
    registrar.Parameter("generations", &TThis::Generations);
}

void TFormattedGeneration::Register(TRegistrar registrar)
{
    registrar.Parameter("configuration", &TThis::Configuration);
    registrar.Parameter("iterations", &TThis::Iterations);
    registrar.Parameter("iteration_count", &TThis::IterationCount);
    registrar.Parameter("sum_for_all_iterations", &TThis::AllIterationStatistics);
}

void TFormattedConfiguration::Register(TRegistrar registrar)
{
    registrar.Parameter("driver", &TThis::Driver);
    registrar.Parameter("threads", &TThis::Threads);
    registrar.Parameter("block_size_log", &TThis::BlockSizeLog);
    registrar.Parameter("range", &TThis::Zone);
    registrar.Parameter("pattern", &TThis::Pattern);
    registrar.Parameter("read_percentage", &TThis::ReadPercentage);
    registrar.Parameter("direct", &TThis::Direct);
    registrar.Parameter("sync", &TThis::Sync);
    registrar.Parameter("fallocate", &TThis::Fallocate);
    registrar.Parameter("oneshot", &TThis::Oneshot);
    registrar.Parameter("validate", &TThis::Validate);
    registrar.Parameter("loop", &TThis::Loop);
    registrar.Parameter("shot_count", &TThis::ShotCount);
    registrar.Parameter("time_limit", &TThis::TimeLimit);
    registrar.Parameter("transfer_limit", &TThis::TransferLimit);
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

void TFormattedDriver::Register(TRegistrar registrar)
{
    registrar.Parameter("type", &TThis::Type);
    registrar.Parameter("config", &TThis::Config);
}

TFormattedDriverPtr TFormattedDriver::From(const TDriverDescription& driver)
{
    auto formatted = New<TFormattedDriver>();
    formatted->Type = driver.Type;
    formatted->Config = driver.Config;
    return formatted;
}

void TFormattedIteration::Register(TRegistrar registrar)
{
    registrar.Parameter("start", &TThis::Start);
    registrar.Parameter("duration", &TThis::Duration);
    registrar.Parameter("threads_aggregated", &TThis::ThreadsAggregated);
    registrar.Parameter("process_rusage", &TThis::ProcessRusage);
    registrar.Parameter("threads", &TThis::Threads);
}

void TFormattedRusageTimeSeries::Register(TRegistrar registrar)
{
    registrar.Parameter("timestamps", &TThis::Timestamps);
    registrar.Parameter("rusage", &TThis::Rusage);
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

void TFormattedWholeRusage::Register(TRegistrar registrar)
{
    registrar.Parameter("process", &TThis::Process);
    registrar.Parameter("kernel_poller", &TThis::KernelPoller);
    registrar.Parameter("process_and_poller", &TThis::ProcessAndPoller);
}

TFormattedWholeRusagePtr TFormattedWholeRusage::From(TWholeRusage wholeRusage)
{
    auto formatted = New<TFormattedWholeRusage>();
    formatted->Process = TFormattedRusage::From(wholeRusage.ProcessRusage);
    formatted->KernelPoller = TFormattedRusage::From(wholeRusage.KernelPollerRusage);
    formatted->ProcessAndPoller = TFormattedRusage::From(wholeRusage.ProcessAndPollerRusage);
    return formatted;
}

void TFormattedRusage::Register(TRegistrar registrar)
{
    registrar.Parameter("user_time", &TThis::UserTime);
    registrar.Parameter("system_time", &TThis::SystemTime);
}

TFormattedRusagePtr TFormattedRusage::From(TRusage rusage)
{
    auto formatted = New<TFormattedRusage>();
    formatted->UserTime = rusage.UserTime.MicroSeconds();
    formatted->SystemTime = rusage.SystemTime.MicroSeconds();
    return formatted;
}

void TFormattedStatistics::Register(TRegistrar registrar)
{
    registrar.Parameter("total", &TThis::Total);
}

TFormattedStatisticsPtr TFormattedStatistics::From(const TStatistics& statistics)
{
    auto formatted = New<TFormattedStatistics>();
    formatted->Total = TFormattedQuantum::From(statistics.Total);
    formatted->Update(statistics.TimeSeries);
    return formatted;
}

void TFormattedTimeSeries::Register(TRegistrar registrar)
{
    registrar.Parameter("timestamps", &TThis::Timestamps);
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

void TFormattedQuantum::Register(TRegistrar registrar)
{
    registrar.Parameter("rusage", &TThis::Rusage);
    registrar.Parameter("operations", &TThis::Operations);
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

void TFormattedOperationType::Register(TRegistrar registrar)
{
    registrar.Parameter("latency", &TThis::Latency);
    registrar.Parameter("latency_histogram", &TThis::LatencyHistogram);
    registrar.Parameter("bytes_transmitted", &TThis::BytesTransmitted);
    registrar.Parameter("operation_count", &TThis::OperationCount);
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

void TFormattedLatencyHistogram::Register(TRegistrar registrar)
{
    registrar.Parameter("scale", &TThis::Scale);
    registrar.Parameter("histogram", &TThis::Histogram);
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

void TFormattedLatency::Register(TRegistrar registrar)
{
    registrar.Parameter("avg", &TThis::Avg);
    registrar.Parameter("min", &TThis::Min);
    registrar.Parameter("max", &TThis::Max);
    registrar.Parameter("sum", &TThis::Sum);
    registrar.Parameter("sum_squared", &TThis::SumSquare);
    registrar.Parameter("count", &TThis::Count);
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
