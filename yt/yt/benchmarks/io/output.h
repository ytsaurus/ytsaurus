#pragma once

#include "iotest.h"
#include "statistics.h"
#include "configuration.h"

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NIOTest {

////////////////////////////////////////////////////////////////////////////////

using TMicroSeconds = i64;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TFormattedOutput)
DECLARE_REFCOUNTED_STRUCT(TFormattedEpoch)
DECLARE_REFCOUNTED_STRUCT(TFormattedConfiguration)
DECLARE_REFCOUNTED_STRUCT(TFormattedDriver)
DECLARE_REFCOUNTED_STRUCT(TFormattedGeneration)
DECLARE_REFCOUNTED_STRUCT(TFormattedIteration)
DECLARE_REFCOUNTED_STRUCT(TFormattedRusageTimeSeries)
DECLARE_REFCOUNTED_STRUCT(TFormattedWholeRusage)
DECLARE_REFCOUNTED_STRUCT(TFormattedRusage)
DECLARE_REFCOUNTED_STRUCT(TFormattedStatistics)
DECLARE_REFCOUNTED_STRUCT(TFormattedTimeSeries)
DECLARE_REFCOUNTED_STRUCT(TFormattedQuantum)
DECLARE_REFCOUNTED_STRUCT(TFormattedOperationType)
DECLARE_REFCOUNTED_STRUCT(TFormattedLatencyHistogram)
DECLARE_REFCOUNTED_STRUCT(TFormattedLatency)

struct TFormattedOutput
    : public NYTree::TYsonSerializable
{
    TString Name;
    TString Info;
    TString Uname;
    NYTree::TYsonSerializablePtr Config;
    std::vector<TFormattedEpochPtr> Epochs;

    TFormattedOutput();
};

DEFINE_REFCOUNTED_TYPE(TFormattedOutput)

struct TFormattedEpoch
    : public NYTree::TYsonSerializable
{
    TString EpochName;
    NYTree::TYsonSerializablePtr Config;
    std::vector<TFormattedGenerationPtr> Generations;

    // TODO(savrus) Add storages for files

    TFormattedEpoch();
};

DEFINE_REFCOUNTED_TYPE(TFormattedEpoch)

struct TFormattedGeneration
    : public NYTree::TYsonSerializable
{
    TFormattedConfigurationPtr Configuration;
    std::vector<TFormattedIterationPtr> Iterations;
    int IterationCount;
    TFormattedStatisticsPtr AllIterationStatistics;

    TFormattedGeneration();
};

DEFINE_REFCOUNTED_TYPE(TFormattedGeneration)

struct TFormattedConfiguration
    : public NYTree::TYsonSerializable
{
    TFormattedDriverPtr Driver;
    int Threads;
    size_t BlockSizeLog;
    std::pair<int, int> Zone;
    EPattern Pattern;
    int ReadPercentage;
    bool Direct;
    ESyncMode Sync;
    EFallocateMode Fallocate;
    bool Oneshot;
    bool Validate;
    int Loop;
    std::optional<i64> ShotCount;

    std::optional<TDuration> TimeLimit;
    std::optional<i64> TransferLimit;

    TFormattedConfiguration();

    static TFormattedConfigurationPtr From(const TConfigurationPtr& configuration);
};

DEFINE_REFCOUNTED_TYPE(TFormattedConfiguration)

struct TFormattedDriver
    : public NYTree::TYsonSerializable
{
    EDriverType Type;
    NYTree::INodePtr Config;

    TFormattedDriver();

    static TFormattedDriverPtr From(const TDriverDescription& driver);
};

DEFINE_REFCOUNTED_TYPE(TFormattedDriver)

struct TFormattedIteration
    : public  NYTree::TYsonSerializable
{
    TInstant Start;
    TDuration Duration;
    TFormattedStatisticsPtr ThreadsAggregated;
    TFormattedRusageTimeSeriesPtr ProcessRusage;
    std::vector<TFormattedStatisticsPtr> Threads;

    TFormattedIteration();
};

DEFINE_REFCOUNTED_TYPE(TFormattedIteration)

struct TFormattedRusageTimeSeries
    : public NYTree::TYsonSerializable
{
    THashMap<i64, TFormattedWholeRusagePtr> Timestamps;
    TFormattedWholeRusagePtr Rusage;

    TFormattedRusageTimeSeries();

    static TFormattedRusageTimeSeriesPtr From(const TRusageTimeSeries& timeseries);
};

DEFINE_REFCOUNTED_TYPE(TFormattedRusageTimeSeries)

struct TFormattedWholeRusage
    : public  NYTree::TYsonSerializable
{
    TFormattedRusagePtr Process;
    TFormattedRusagePtr KernelPoller;
    TFormattedRusagePtr ProcessAndPoller;

    TFormattedWholeRusage();

    static TFormattedWholeRusagePtr From(TWholeRusage wholeRusage);
};

DEFINE_REFCOUNTED_TYPE(TFormattedWholeRusage)

struct TFormattedRusage
    : public  NYTree::TYsonSerializable
{
    TMicroSeconds UserTime;
    TMicroSeconds SystemTime;

    TFormattedRusage();

    static TFormattedRusagePtr From(TRusage rusage);
};

DEFINE_REFCOUNTED_TYPE(TFormattedRusage)

struct TFormattedTimeSeries
    : public NYTree::TYsonSerializable
{
    THashMap<i64, TFormattedQuantumPtr> Timestamps;

    TFormattedTimeSeries();

    void Update(const TTimeSeries& timeseries);

    static TFormattedTimeSeriesPtr From(const TTimeSeries& timeseries);
};

DEFINE_REFCOUNTED_TYPE(TFormattedTimeSeries)

struct TFormattedStatistics
    : public TFormattedTimeSeries
{
    TFormattedQuantumPtr Total;

    TFormattedStatistics();

    static TFormattedStatisticsPtr From(const TStatistics& statistics);
};

DEFINE_REFCOUNTED_TYPE(TFormattedStatistics)

struct TFormattedQuantum
    : public NYTree::TYsonSerializable
{
    TFormattedRusagePtr Rusage;
    THashMap<TString, TFormattedOperationTypePtr> Operations;

    TFormattedQuantum();

    static TFormattedQuantumPtr From(const TQuantumStatistics& quantum);
};

DEFINE_REFCOUNTED_TYPE(TFormattedQuantum)

struct TFormattedOperationType
    : public NYTree::TYsonSerializable
{
    // TODO(savrus) Put histogram inside latency
    TFormattedLatencyPtr Latency;
    TFormattedLatencyHistogramPtr LatencyHistogram;
    i64 BytesTransmitted;
    i64 OperationCount;

    TFormattedOperationType();

    static TFormattedOperationTypePtr From(const TOperationTypeStatistics& operation);
};

DEFINE_REFCOUNTED_TYPE(TFormattedOperationType)

struct TFormattedLatencyHistogram
    : public NYTree::TYsonSerializable
{
    double Scale;
    THashMap<i64, i64> Histogram;

    TFormattedLatencyHistogram();

    static TFormattedLatencyHistogramPtr From(const TLatencyHistogram& histogram);
};

DEFINE_REFCOUNTED_TYPE(TFormattedLatencyHistogram)

struct TFormattedLatency
    : public NYTree::TYsonSerializable
{
    TMicroSeconds Avg;
    TMicroSeconds Min;
    TMicroSeconds Max;
    TMicroSeconds Sum;
    TMicroSeconds SumSquare;
    i64 Count = 0;

    TFormattedLatency();

    static TFormattedLatencyPtr From(const TAggregateLatency& latency);
};

DEFINE_REFCOUNTED_TYPE(TFormattedLatency)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
