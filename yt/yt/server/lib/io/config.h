#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

class TIOTrackerConfig
    : public NYTree::TYsonSerializable
{
public:
    //! If set to true, logging of IO events is enabled.
    bool Enable;

    //! If set to true, raw IO events can be logged. Otherwise, only aggregated events are logged.
    bool EnableRaw;

    //! Queue size for IO events that were enqueued but were not logged. If the queue size exceeds
    //! its limit, incoming events will be dropped.
    int QueueSizeLimit;

    //! Number of aggregated IO events kept in memory. The events which don't fit into this limit
    //! are dropped.
    int AggregationSizeLimit;

    //! Period during which the events are aggregated. When the period is finished, all the aggregated
    //! events are flushed into the log.
    TDuration AggregationPeriod;

    //! Period used to poll the queue for new events.
    TDuration PeriodQuant;

    //! If set to true, the events will be dequeued and processed, otherwise they will stay in the queue.
    //! This option is used only for testing and must be always set to true in production.
    bool EnableEventDequeue;

    TIOTrackerConfig();
};

DEFINE_REFCOUNTED_TYPE(TIOTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCongestionDetectorConfig
    : public NYTree::TYsonSerializable
{
    bool ProbesEnabled;

    // How many probes to make before making decision.
    i32 ProbesPerRound;

    // Time between probes;
    TDuration ProbesInterval;

    // Probe read size.
    i32 PacketSize;

    // Probe read request timeout.
    TDuration ProbeDeadline;

    // Limiting in-flight probes count to arbitrary large value.
    i32 MaxInFlightProbeCount;

    // Failed probes percentages.
    i32 OverloadThreshold;

    // Failed probes percentages.
    i32 HeavyOverloadThreshold;

    // User interactive overload 99p latency.
    TDuration UserRequestOverloadThreshold;

    // User interactive overload 99p latency.
    TDuration UserRequestHeavyOverloadThreshold;

    // Consecutive user failed probes count.
    i32 UserRequestFailedProbesThreshold;

    TCongestionDetectorConfig();
};

DEFINE_REFCOUNTED_TYPE(TCongestionDetectorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TGentleLoaderConfig
    : public NYTree::TYsonSerializable
{
    TCongestionDetectorConfigPtr CongestionDetector;

    // Read/Write requests sizes.
    i32 PacketSize;

    // IO request count currently in flight.
    i32 MaxInFlightCount;

    // 100 means only reads, 0 - only writes.
    ui8 DefaultReadToWriteRatio;

    // Use accumulated statistics to derive read/write ratio after
    // this bytes count transmitted (read+written).
    i64 AdaptiveReadToWriteRatioThreshold;

    TDuration LoadAdjustingInterval;

    // Window increments/decrements are done in terms of segments.
    // Measured in packets.
    i32 SegmentSize;

    // Sane maximum window value.
    i32 MaxWindowSize;

    // Each writer corresponds to one open file for write.
    i32 WritersCount;

    // Each reader corresponds to one open file for read.
    i32 ReadersCount;

    i32 MaxWriteFileSize;

    // Subfolder to create temporary files.
    TString WritersFolder;

    // Cleanup written files after testing finished.
    bool RemoveWrittenFiles;

    // Don't send load request for this period after congested.
    TDuration WaitAfterCongested;

    // Slowdown IO requests for testing purposes.
    // NB. use only in integration tests.
    TDuration SimulatedRequestLatency;

    bool UseDirectIO;

    // Max write rate limmit (mb/s)
    i64 MaxWriteRate;

    // The followings are very low-level settings (do not use if unsure).
    i32 InitialWindowSize;
    i32 InitialSlowStartThreshold;
    TDuration WindowVerificationPeriod;

    TGentleLoaderConfig();
};

DEFINE_REFCOUNTED_TYPE(TGentleLoaderConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
