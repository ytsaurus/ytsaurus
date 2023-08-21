#include "config.h"

#include <yt/yt/core/misc/fs.h>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

void TIOTrackerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(false);
    registrar.Parameter("enable_raw", &TThis::EnableRaw)
        .Default(false);
    registrar.Parameter("enable_aggr", &TThis::EnableAggr)
        .Default(true);
    registrar.Parameter("enable_path", &TThis::EnablePath)
        .Default(false);
    registrar.Parameter("queue_size_limit", &TThis::QueueSizeLimit)
        .Default(10'000);
    registrar.Parameter("aggregation_size_limit", &TThis::AggregationSizeLimit)
        .Default(1'000'000);
    registrar.Parameter("aggregation_period", &TThis::AggregationPeriod)
        .Default(TDuration::Minutes(15));
    registrar.Parameter("period_quant", &TThis::PeriodQuant)
        .Default(TDuration::MilliSeconds(50));
    registrar.Parameter("path_aggregate_tags", &TThis::PathAggregateTags)
        .Default();
    registrar.Parameter("enable_event_dequeue", &TThis::EnableEventDequeue)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TCongestionDetectorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("probes_enabled", &TThis::ProbesEnabled)
        .Default(true);

    registrar.Parameter("probes_per_round", &TThis::ProbesPerRound)
        .GreaterThan(0)
        .Default(10);

    registrar.Parameter("probes_interval", &TThis::ProbesInterval)
        .Default(TDuration::MilliSeconds(200));

    registrar.Parameter("packet_size", &TThis::PacketSize)
        .GreaterThanOrEqual(512)
        .Default(16_KB);

    registrar.Parameter("max_in_flight_probe_count", &TThis::MaxInFlightProbeCount)
        .Default(1000);

    registrar.Parameter("probe_deadline", &TThis::ProbeDeadline)
        .Default(TDuration::MilliSeconds(100));

    registrar.Parameter("overload_threshold", &TThis::OverloadThreshold)
        .InRange(0, 100)
        .Default(25);

    registrar.Parameter("heavy_overload_threshold", &TThis::HeavyOverloadThreshold)
        .InRange(0, 100)
        .Default(50);

    registrar.Parameter("user_request_overload_threshold", &TThis::UserRequestOverloadThreshold)
        .Default(TDuration::MilliSeconds(100));

    registrar.Parameter("user_request_heavy_overload_threshold", &TThis::UserRequestHeavyOverloadThreshold)
        .Default(TDuration::MilliSeconds(200));

    registrar.Parameter("user_request_failed_probes_threshold", &TThis::UserRequestFailedProbesThreshold)
        .GreaterThan(0)
        .Default(3);
}

////////////////////////////////////////////////////////////////////////////////

void TGentleLoaderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("congestion_detector", &TThis::CongestionDetector)
        .DefaultNew();

    registrar.Parameter("default_read_to_write_ratio", &TThis::DefaultReadToWriteRatio)
        .InRange(0, 100)
        .Default(75);

    registrar.Parameter("adaptive_read_to_write_ratio_threshold", &TThis::AdaptiveReadToWriteRatioThreshold)
        .GreaterThan(0)
        .Default(100_GB);

    registrar.Parameter("load_adjusting_interval", &TThis::LoadAdjustingInterval)
        .Default(TDuration::MilliSeconds(200));

    registrar.Parameter("segment_size", &TThis::SegmentSize)
        .GreaterThan(0)
        .Default(32);

    registrar.Parameter("max_window_size", &TThis::MaxWindowSize)
        .GreaterThan(0)
        .Default(10'000);

    registrar.Parameter("packet_size", &TThis::PacketSize)
        .GreaterThan(0)
        .Default(128_KB);

    registrar.Parameter("max_in_flight_count", &TThis::MaxInFlightCount)
        .GreaterThan(0)
        .Default(5000);

    registrar.Parameter("writer_count", &TThis::WriterCount)
        .GreaterThan(0)
        .Default(100);

    registrar.Parameter("reader_count", &TThis::ReaderCount)
        .GreaterThan(0)
        .Default(100);

    registrar.Parameter("max_write_file_size", &TThis::MaxWriteFileSize)
        .GreaterThan(4_KB)
        .Default(512_MB);

    registrar.Parameter("preallocate_write_files", &TThis::PreallocateWriteFiles)
        .Default();

    registrar.Parameter("writers_folder", &TThis::WriterDirectory)
        .NonEmpty()
        .Default("writes_bench");

    registrar.Parameter("remove_written_files", &TThis::RemoveWrittenFiles)
        .Default(true);

    registrar.Parameter("flush_after_write", &TThis::FlushAfterWrite)
        .Default(false);

    registrar.Parameter("stale_file_count_per_writer", &TThis::StaleFileCountPerWriter)
        .Default(0);

    registrar.Parameter("wait_after_congested", &TThis::WaitAfterCongested)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("simulated_request_latency", &TThis::SimulatedRequestLatency)
        .Default();

    registrar.Parameter("use_direct_io", &TThis::UseDirectIO)
        .Default(true);

    registrar.Parameter("max_write_rate", &TThis::MaxWriteRate)
        .GreaterThan(0)
        .Default(96_MB);

    registrar.Parameter("limit_max_window_sizes_by_max_write_rate", &TThis::LimitMaxWindowSizesByMaxWriteRate)
        .Default(false);

    registrar.Parameter("initial_window_size", &TThis::InitialWindowSize)
        .Default(1);

    registrar.Parameter("initial_slow_start_threshold", &TThis::InitialSlowStartThreshold)
        .Default(0);

    registrar.Parameter("window_period", &TThis::WindowPeriod)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
