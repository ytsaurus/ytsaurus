#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NLsm::NTesting {

////////////////////////////////////////////////////////////////////////////////

struct TWriterSpec
    : public NYTree::TYsonStruct
{
    bool AlmostIncreasing;
    i64 IncreasingBegin;
    i64 IncreasingEnd;
    i64 IncreasingSplay;

    bool BulkInsert;
    i64 BulkInsertChunkCount;

    bool DeleteAfterInsert;
    i64 DeletionWindowSize;

    i64 WriteRate;

    i64 TotalDataSize;
    std::optional<i64> MinValueSize;
    std::optional<i64> MaxValueSize;

    std::optional<i64> MinKey;
    std::optional<i64> MaxKey;

    i64 SleepTime;

    REGISTER_YSON_STRUCT(TWriterSpec);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("almost_increasing", &TThis::AlmostIncreasing)
            .Default(false);
        registrar.Parameter("increasing_begin", &TThis::IncreasingBegin)
            .Default(0);
        registrar.Parameter("increasing_end", &TThis::IncreasingEnd)
            .Default(10000);
        registrar.Parameter("increasing_splay", &TThis::IncreasingSplay)
            .Default(100);
        registrar.Parameter("bulk_insert", &TThis::BulkInsert)
            .Default(false);
        registrar.Parameter("bulk_insert_chunk_count", &TThis::BulkInsertChunkCount)
            .Default(1);
        registrar.Parameter("delete_after_insert", &TThis::DeleteAfterInsert)
            .Default(false);
        registrar.Parameter("deletion_window_size", &TThis::DeletionWindowSize)
            .Default(100);
        registrar.Parameter("write_rate", &TThis::WriteRate)
            .Default(1_MB);
        registrar.Parameter("total_data_size", &TThis::TotalDataSize)
            .Default(1_GB);
        registrar.Parameter("min_value_size", &TThis::MinValueSize)
            .Default();
        registrar.Parameter("max_value_size", &TThis::MaxValueSize)
            .Default();
        registrar.Parameter("min_key", &TThis::MinKey)
            .Default();
        registrar.Parameter("max_key", &TThis::MaxKey)
            .Default();
        registrar.Parameter("sleep_time", &TThis::SleepTime)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TWriterSpec)

struct TInitialDataDescription
    : public NYTree::TYsonStruct
{
    std::vector<i64> Eden;
    std::vector<std::vector<i64>> Partitions;

    i64 MinKey;
    i64 MaxKey;
    i64 MinValueSize;
    i64 MaxValueSize;

    REGISTER_YSON_STRUCT(TInitialDataDescription);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("eden", &TThis::Eden)
            .Default();
        registrar.Parameter("partitions", &TThis::Partitions)
            .Default();
        registrar.Parameter("min_key", &TThis::MinKey)
            .Default(0);
        registrar.Parameter("max_key", &TThis::MaxKey)
            .Default(1'000'000'000);
        registrar.Parameter("min_value_size", &TThis::MinValueSize)
            .Default(10_KB);
        registrar.Parameter("max_value_size", &TThis::MaxValueSize)
            .Default(100_KB);
    }
};

DEFINE_REFCOUNTED_TYPE(TInitialDataDescription)

struct TMountConfigOptimizerConfig
    : public NYTree::TYsonStruct
{
    struct TParameter
        : public NYTree::TYsonStruct
    {
        double Min;
        double Max;
        double Weight;
        bool Integer;
        std::vector<TString> LessThan;
        std::vector<TString> LessThanOrEqual;

        REGISTER_YSON_STRUCT(TParameter);

    static void Register(TRegistrar registrar)
        {
            registrar.Parameter("min", &TThis::Min);
            registrar.Parameter("max", &TThis::Max);
            registrar.Parameter("weight", &TThis::Weight)
                .Default(1.0);
            registrar.Parameter("integer", &TThis::Integer)
                .Default(true);
            registrar.Parameter("less_than", &TThis::LessThan)
                .Default();
            registrar.Parameter("less_than_or_equal", &TThis::LessThanOrEqual)
                .Default();
        }
    };

    THashMap<TString, TIntrusivePtr<TParameter>> Parameters;

    int NumIterations;
    int NumDirections;
    double InitialStep;
    double StepMultiple;

    REGISTER_YSON_STRUCT(TMountConfigOptimizerConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("parameters", &TThis::Parameters);

        registrar.Parameter("num_iterations", &TThis::NumIterations)
            .Default(30);
        registrar.Parameter("num_directions", &TThis::NumDirections)
            .Default(5);
        registrar.Parameter("initial_step", &TThis::InitialStep)
            .Default(0.2);
        registrar.Parameter("step_multiple", &TThis::StepMultiple)
            .Default(0.9);
    }
};

DEFINE_REFCOUNTED_TYPE(TMountConfigOptimizerConfig)

struct TLsmSimulatorConfig
    : public NYTree::TYsonStruct
{
    TDuration LsmScanPeriod;
    TDuration FlushPeriod;

    i64 CompactionThrottler;
    i64 PartitioningThrottler;

    i64 MinKey;
    i64 MaxKey;
    i64 MinValueSize;
    i64 MaxValueSize;

    TString InsertionPolicy;

    std::vector<TWriterSpecPtr> Writers;
    TDuration InsertionPeriod;

    i64 IncreasingBegin;
    i64 IncreasingEnd;
    i64 IncreasingSplay;

    double CompressionRatio;

    TDuration CompactionDelay;
    TDuration PartitioningDelay;
    TDuration GetSamplesDelay;

    bool ReverseCompactionComparator;

    bool EnableStructureLogging;

    std::vector<TString> LoggingFields;
    std::vector<TString> LoggingFieldsFinal;
    TDuration LoggingPeriod;
    THashSet<int> ResetStatisticsAfterLoggers;
    bool SumCompactionAndPartitioningInLogging;
    int StatisticsWindowSize;

    TMountConfigOptimizerConfigPtr Optimizer;

    TInitialDataDescriptionPtr InitialData;

    REGISTER_YSON_STRUCT(TLsmSimulatorConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("lsm_scan_period", &TThis::LsmScanPeriod)
            .Default(TDuration::Seconds(1));
        registrar.Parameter("flush_period", &TThis::FlushPeriod)
            .Default(TDuration::MilliSeconds(1234));

        registrar.Parameter("compaction_throttler", &TThis::CompactionThrottler)
            .Default(100_MB);
        registrar.Parameter("partitioning_throttler", &TThis::PartitioningThrottler)
            .Default(100_MB);

        registrar.Parameter("min_key", &TThis::MinKey)
            .Default(0);
        registrar.Parameter("max_key", &TThis::MaxKey)
            .Default(1000);
        registrar.Parameter("min_value_size", &TThis::MinValueSize)
            .Default(10_KB);
        registrar.Parameter("max_value_size", &TThis::MaxValueSize)
            .Default(100_KB);

        registrar.Parameter("insertion_policy", &TThis::InsertionPolicy)
            .Default("random");

        registrar.Parameter("writers", &TThis::Writers)
            .Default({New<TWriterSpec>()});

        registrar.Parameter("insertion_period", &TThis::InsertionPeriod)
            .Default(TDuration::MilliSeconds(100));

        registrar.Parameter("increasing_begin", &TThis::IncreasingBegin)
            .Default(0);
        registrar.Parameter("increasing_end", &TThis::IncreasingEnd)
            .Default(10000);
        registrar.Parameter("increasing_splay", &TThis::IncreasingSplay)
            .Default(10);

        registrar.Parameter("compression_ratio", &TThis::CompressionRatio)
            .Default(1.0);

        registrar.Parameter("compaction_delay", &TThis::CompactionDelay)
            .Default(TDuration::Seconds(3));
        registrar.Parameter("partitioning_delay", &TThis::PartitioningDelay)
            .Default(TDuration::Seconds(3));
        registrar.Parameter("get_samples_delay", &TThis::GetSamplesDelay)
            .Default(TDuration::Seconds(1));

        registrar.Parameter("reverse_compaction_comparator", &TThis::ReverseCompactionComparator)
            .Default(false);

        registrar.Parameter("enable_structure_logging", &TThis::EnableStructureLogging)
            .Default(false);

        registrar.Parameter("logging_fields", &TThis::LoggingFields)
            .Default();
        registrar.Parameter("logging_fields_final", &TThis::LoggingFieldsFinal)
            .Default();
        registrar.Parameter("logging_period", &TThis::LoggingPeriod)
            .Default(TDuration::Seconds(1));
        registrar.Parameter("reset_statistics_after_loggers", &TThis::ResetStatisticsAfterLoggers)
            .Default();
        registrar.Parameter("sum_compaction_and_partitioning_in_logging", &TThis::SumCompactionAndPartitioningInLogging)
            .Default(false);
        registrar.Parameter("statistics_window_size", &TThis::StatisticsWindowSize)
            .Default(30);

        registrar.Parameter("optimizer", &TThis::Optimizer)
            .Default(); // Sic!

        registrar.Parameter("initial_data", &TThis::InitialData)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TLsmSimulatorConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
