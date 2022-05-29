#include "config.h"

#include <yt/yt/core/misc/fs.h>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

TIOTrackerConfig::TIOTrackerConfig()
{
    RegisterParameter("enable", Enable)
        .Default(false);
    RegisterParameter("enable_raw", EnableRaw)
        .Default(false);
    RegisterParameter("enable_aggr", EnableAggr)
        .Default(true);
    RegisterParameter("enable_path", EnablePath)
        .Default(false);
    RegisterParameter("queue_size_limit", QueueSizeLimit)
        .Default(10'000);
    RegisterParameter("aggregation_size_limit", AggregationSizeLimit)
        .Default(1'000'000);
    RegisterParameter("aggregation_period", AggregationPeriod)
        .Default(TDuration::Minutes(15));
    RegisterParameter("period_quant", PeriodQuant)
        .Default(TDuration::MilliSeconds(50));
    RegisterParameter("path_aggregate_tags", PathAggregateTags)
        .Default();
    RegisterParameter("enable_event_dequeue", EnableEventDequeue)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

TCongestionDetectorConfig::TCongestionDetectorConfig()
{
    RegisterParameter("probes_enabled", ProbesEnabled)
        .Default(true);

    RegisterParameter("probes_per_round", ProbesPerRound)
        .GreaterThan(0)
        .Default(10);

    RegisterParameter("probes_interval", ProbesInterval)
        .Default(TDuration::MilliSeconds(200));

    RegisterParameter("packet_size", PacketSize)
        .GreaterThanOrEqual(512)
        .Default(16_KB);

    RegisterParameter("max_in_flight_probe_count", MaxInFlightProbeCount)
        .Default(1000);

    RegisterParameter("probe_deadline", ProbeDeadline)
        .Default(TDuration::MilliSeconds(100));

    RegisterParameter("overload_threshold", OverloadThreshold)
        .InRange(0, 100)
        .Default(25);

    RegisterParameter("heavy_overload_threshold", HeavyOverloadThreshold)
        .InRange(0, 100)
        .Default(50);

    RegisterParameter("user_request_overload_threshold", UserRequestOverloadThreshold)
        .Default(TDuration::MilliSeconds(100));

    RegisterParameter("user_request_heavy_overload_threshold", UserRequestHeavyOverloadThreshold)
        .Default(TDuration::MilliSeconds(200));

    RegisterParameter("user_reques_failed_probes_threshold", UserRequestFailedProbesThreshold)
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

    registrar.Parameter("writers_count", &TThis::WritersCount)
        .GreaterThan(0)
        .Default(100);

    registrar.Parameter("readers_count", &TThis::ReadersCount)
        .GreaterThan(0)
        .Default(100);

    registrar.Parameter("max_write_file_size", &TThis::MaxWriteFileSize)
        .GreaterThan(4_KB)
        .Default(512_MB);

    registrar.Parameter("writers_folder", &TThis::WritersFolder)
        .NonEmpty()
        .Default("writes_bench");

    registrar.Parameter("remove_written_files", &TThis::RemoveWrittenFiles)
        .Default(true);

    registrar.Parameter("flush_after_write", &TThis::FlushAfterWrite)
        .Default(false);

    registrar.Parameter("stale_files_count_per_writer", &TThis::StaleFilesCountPerWriter)
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

    registrar.Parameter("initial_window_size", &TThis::InitialWindowSize)
        .Default(1);

    registrar.Parameter("initial_slow_start_threshold", &TThis::InitialSlowStartThreshold)
        .Default(0);

    registrar.Parameter("window_period", &TThis::WindowPeriod)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
