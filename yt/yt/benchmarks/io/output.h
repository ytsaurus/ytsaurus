#pragma once

#include "iotest.h"
#include "statistics.h"
#include "configuration.h"

#include <yt/yt/core/ytree/yson_struct.h>

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
    : public NYTree::TYsonStruct
{
    TString Name;
    TString Info;
    TString Uname;
    NYTree::TYsonStructPtr Config;
    std::vector<TFormattedEpochPtr> Epochs;

    REGISTER_YSON_STRUCT(TFormattedOutput);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFormattedOutput)

struct TFormattedEpoch
    : public NYTree::TYsonStruct
{
    TString EpochName;
    NYTree::TYsonStructPtr Config;
    std::vector<TFormattedGenerationPtr> Generations;

    // TODO(savrus) Add storages for files

    REGISTER_YSON_STRUCT(TFormattedEpoch);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFormattedEpoch)

struct TFormattedGeneration
    : public NYTree::TYsonStruct
{
    TFormattedConfigurationPtr Configuration;
    std::vector<TFormattedIterationPtr> Iterations;
    int IterationCount;
    TFormattedStatisticsPtr AllIterationStatistics;

    REGISTER_YSON_STRUCT(TFormattedGeneration);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFormattedGeneration)

struct TFormattedConfiguration
    : public NYTree::TYsonStruct
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

    static TFormattedConfigurationPtr From(const TConfigurationPtr& configuration);

    REGISTER_YSON_STRUCT(TFormattedConfiguration);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFormattedConfiguration)

struct TFormattedDriver
    : public NYTree::TYsonStruct
{
    EDriverType Type;
    NYTree::INodePtr Config;

    static TFormattedDriverPtr From(const TDriverDescription& driver);

    REGISTER_YSON_STRUCT(TFormattedDriver);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFormattedDriver)

struct TFormattedIteration
    : public  NYTree::TYsonStruct
{
    TInstant Start;
    TDuration Duration;
    TFormattedStatisticsPtr ThreadsAggregated;
    TFormattedRusageTimeSeriesPtr ProcessRusage;
    std::vector<TFormattedStatisticsPtr> Threads;

    REGISTER_YSON_STRUCT(TFormattedIteration);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFormattedIteration)

struct TFormattedRusageTimeSeries
    : public NYTree::TYsonStruct
{
    THashMap<i64, TFormattedWholeRusagePtr> Timestamps;
    TFormattedWholeRusagePtr Rusage;

    static TFormattedRusageTimeSeriesPtr From(const TRusageTimeSeries& timeseries);

    REGISTER_YSON_STRUCT(TFormattedRusageTimeSeries);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFormattedRusageTimeSeries)

struct TFormattedWholeRusage
    : public  NYTree::TYsonStruct
{
    TFormattedRusagePtr Process;
    TFormattedRusagePtr KernelPoller;
    TFormattedRusagePtr ProcessAndPoller;

    static TFormattedWholeRusagePtr From(TWholeRusage wholeRusage);

    REGISTER_YSON_STRUCT(TFormattedWholeRusage);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFormattedWholeRusage)

struct TFormattedRusage
    : public  NYTree::TYsonStruct
{
    TMicroSeconds UserTime;
    TMicroSeconds SystemTime;

    static TFormattedRusagePtr From(TRusage rusage);

    REGISTER_YSON_STRUCT(TFormattedRusage);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFormattedRusage)

struct TFormattedTimeSeries
    : public NYTree::TYsonStruct
{
    THashMap<i64, TFormattedQuantumPtr> Timestamps;

    static TFormattedTimeSeriesPtr From(const TTimeSeries& timeseries);

    void Update(const TTimeSeries& timeseries);

    REGISTER_YSON_STRUCT(TFormattedTimeSeries);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFormattedTimeSeries)

struct TFormattedStatistics
    : public TFormattedTimeSeries
{
    TFormattedQuantumPtr Total;

    static TFormattedStatisticsPtr From(const TStatistics& statistics);

    REGISTER_YSON_STRUCT(TFormattedStatistics);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFormattedStatistics)

struct TFormattedQuantum
    : public NYTree::TYsonStruct
{
    TFormattedRusagePtr Rusage;
    THashMap<TString, TFormattedOperationTypePtr> Operations;

    static TFormattedQuantumPtr From(const TQuantumStatistics& quantum);

    REGISTER_YSON_STRUCT(TFormattedQuantum);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFormattedQuantum)

struct TFormattedOperationType
    : public NYTree::TYsonStruct
{
    // TODO(savrus) Put histogram inside latency
    TFormattedLatencyPtr Latency;
    TFormattedLatencyHistogramPtr LatencyHistogram;
    i64 BytesTransmitted;
    i64 OperationCount;

    static TFormattedOperationTypePtr From(const TOperationTypeStatistics& operation);

    REGISTER_YSON_STRUCT(TFormattedOperationType);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFormattedOperationType)

struct TFormattedLatencyHistogram
    : public NYTree::TYsonStruct
{
    double Scale;
    THashMap<i64, i64> Histogram;

    static TFormattedLatencyHistogramPtr From(const TLatencyHistogram& histogram);

    REGISTER_YSON_STRUCT(TFormattedLatencyHistogram);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFormattedLatencyHistogram)

struct TFormattedLatency
    : public NYTree::TYsonStruct
{
    TMicroSeconds Avg;
    TMicroSeconds Min;
    TMicroSeconds Max;
    TMicroSeconds Sum;
    TMicroSeconds SumSquare;
    i64 Count = 0;

    static TFormattedLatencyPtr From(const TAggregateLatency& latency);

    REGISTER_YSON_STRUCT(TFormattedLatency);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFormattedLatency)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
