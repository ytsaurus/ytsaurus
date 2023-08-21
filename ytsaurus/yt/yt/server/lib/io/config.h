#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

class TIOTrackerConfig
    : public NYTree::TYsonStruct
{
public:
    //! If set to true, logging of IO events is enabled.
    bool Enable;

    //! If set to true, raw IO events can be logged.
    bool EnableRaw;

    //! If set to true, aggregate IO events can be logged.
    bool EnableAggr;

    //! If set to true, logs aggregated by path are enabled.
    bool EnablePath;

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

    //! Allowed tags for logs aggregated by path.
    THashSet<TString> PathAggregateTags;

    //! If set to true, the events will be dequeued and processed, otherwise they will stay in the queue.
    //! This option is used only for testing and must be always set to true in production.
    bool EnableEventDequeue;

    REGISTER_YSON_STRUCT(TIOTrackerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TIOTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCongestionDetectorConfig
    : public NYTree::TYsonStruct
{
    bool ProbesEnabled;

    // How many probes to make before making decision.
    int ProbesPerRound;

    // Time between probes;
    TDuration ProbesInterval;

    // Probe read size.
    int PacketSize;

    // Probe read request timeout.
    TDuration ProbeDeadline;

    // Limiting in-flight probes count to arbitrary large value.
    int MaxInFlightProbeCount;

    // Failed probes percentages.
    int OverloadThreshold;

    // Failed probes percentages.
    int HeavyOverloadThreshold;

    // User interactive overload 99p latency.
    TDuration UserRequestOverloadThreshold;

    // User interactive overload 99p latency.
    TDuration UserRequestHeavyOverloadThreshold;

    // Consecutive user failed probes count.
    int UserRequestFailedProbesThreshold;

    REGISTER_YSON_STRUCT(TCongestionDetectorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCongestionDetectorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TGentleLoaderConfig
    : public NYTree::TYsonStruct
{
    TCongestionDetectorConfigPtr CongestionDetector;

    // Read/Write requests sizes.
    int PacketSize;

    // IO request count currently in flight.
    int MaxInFlightCount;

    // 100 means only reads, 0 - only writes.
    int DefaultReadToWriteRatio;

    // Use accumulated statistics to derive read/write ratio after
    // this bytes count transmitted (read+written).
    i64 AdaptiveReadToWriteRatioThreshold;

    TDuration LoadAdjustingInterval;

    // Window increments/decrements are done in terms of segments.
    // Measured in packets.
    int SegmentSize;

    // Sane maximum window value, i.e. max IO per second.
    int MaxWindowSize;

    // Each writer corresponds to one open file for write.
    int WriterCount;

    // Each reader corresponds to one open file for read.
    int ReaderCount;

    int MaxWriteFileSize;

    bool PreallocateWriteFiles;

    // Subfolder to create temporary files.
    TString WriterDirectory;

    // Cleanup written files after testing finished.
    bool RemoveWrittenFiles;

    // Useful for emulating journal workloads.
    bool FlushAfterWrite;

    // Hold this count of written files before remove.
    // Helps to simulate disk space utilization.
    int StaleFileCountPerWriter;

    // Don't send load request for this period after congested.
    TDuration WaitAfterCongested;

    // Slowdown IO requests for testing purposes.
    // NB. use only in integration tests.
    TDuration SimulatedRequestLatency;

    bool UseDirectIO;

    // Max write rate limit (mb/s)
    i64 MaxWriteRate;

    // If true, read request will not disproportionally outgrow write requests.
    bool LimitMaxWindowSizesByMaxWriteRate;

    // The followings are very low-level settings (do not use if unsure).
    int InitialWindowSize;
    int InitialSlowStartThreshold;
    TDuration WindowPeriod;

    REGISTER_YSON_STRUCT(TGentleLoaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TGentleLoaderConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
