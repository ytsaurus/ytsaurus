#include "config.h"

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

TIOTrackerConfig::TIOTrackerConfig()
{
    RegisterParameter("enable", Enable)
        .Default(false);
    RegisterParameter("enable_raw", EnableRaw)
        .Default(false);
    RegisterParameter("queue_size_limit", QueueSizeLimit)
        .Default(10'000);
    RegisterParameter("aggregation_size_limit", AggregationSizeLimit)
        .Default(1'000'000);
    RegisterParameter("aggregation_period", AggregationPeriod)
        .Default(TDuration::Minutes(15));
    RegisterParameter("period_quant", PeriodQuant)
        .Default(TDuration::MilliSeconds(50));
    RegisterParameter("enable_event_dequeue", EnableEventDequeue)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

TCongestionDetectorConfig::TCongestionDetectorConfig()
{
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

TGentleLoaderConfig::TGentleLoaderConfig()
{
    RegisterParameter("congestion_detector", CongestionDetector)
        .DefaultNew();

    RegisterParameter("read_to_write_ratio", ReadToWriteRatio)
        .InRange(0, 100)
        .Default(75);

    RegisterParameter("segment_size", SegmentSize)
        .GreaterThan(0)
        .Default(10);

    RegisterParameter("max_window_size", MaxWindowSize)
        .GreaterThan(0)
        .Default(10'000);

    RegisterParameter("packet_size", PacketSize)
        .GreaterThan(0)
        .Default(128_KB);

    RegisterParameter("max_in_flight_count", MaxInFlightCount)
        .GreaterThan(0)
        .Default(5000);

    RegisterParameter("writers_count", WritersCount)
        .GreaterThan(0)
        .Default(100);

    RegisterParameter("max_write_file_size", MaxWriteFileSize)
        .GreaterThan(4_KB)
        .Default(512_MB);

    RegisterParameter("writers_folder", WritersFolder)
        .NonEmpty()
        .Default("writes_bench");

    RegisterParameter("wait_after_congested", WaitAfterCongested)
        .Default(TDuration::Seconds(10));

    RegisterParameter("simulated_request_latency", SimulatedRequestLatency)
        .Default();

    RegisterParameter("use_direct_io", UseDirectIO)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
