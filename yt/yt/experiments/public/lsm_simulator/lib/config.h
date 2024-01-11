#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NLsm::NTesting {

////////////////////////////////////////////////////////////////////////////////

struct TWriterSpec
    : public NYTree::TYsonSerializable
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

    TWriterSpec()
    {
        RegisterParameter("almost_increasing", AlmostIncreasing)
            .Default(false);
        RegisterParameter("increasing_begin", IncreasingBegin)
            .Default(0);
        RegisterParameter("increasing_end", IncreasingEnd)
            .Default(10000);
        RegisterParameter("increasing_splay", IncreasingSplay)
            .Default(100);
        RegisterParameter("bulk_insert", BulkInsert)
            .Default(false);
        RegisterParameter("bulk_insert_chunk_count", BulkInsertChunkCount)
            .Default(1);
        RegisterParameter("delete_after_insert", DeleteAfterInsert)
            .Default(false);
        RegisterParameter("deletion_window_size", DeletionWindowSize)
            .Default(100);
        RegisterParameter("write_rate", WriteRate)
            .Default(1_MB);
        RegisterParameter("total_data_size", TotalDataSize)
            .Default(1_GB);
        RegisterParameter("min_value_size", MinValueSize)
            .Default();
        RegisterParameter("max_value_size", MaxValueSize)
            .Default();
        RegisterParameter("min_key", MinKey)
            .Default();
        RegisterParameter("max_key", MaxKey)
            .Default();
        RegisterParameter("sleep_time", SleepTime)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TWriterSpec)

struct TInitialDataDescription
    : public NYTree::TYsonSerializable
{
    std::vector<i64> Eden;
    std::vector<std::vector<i64>> Partitions;

    i64 MinKey;
    i64 MaxKey;
    i64 MinValueSize;
    i64 MaxValueSize;

    TInitialDataDescription()
    {
        RegisterParameter("eden", Eden)
            .Default();
        RegisterParameter("partitions", Partitions)
            .Default();
        RegisterParameter("min_key", MinKey)
            .Default(0);
        RegisterParameter("max_key", MaxKey)
            .Default(1'000'000'000);
        RegisterParameter("min_value_size", MinValueSize)
            .Default(10_KB);
        RegisterParameter("max_value_size", MaxValueSize)
            .Default(100_KB);
    }
};

DEFINE_REFCOUNTED_TYPE(TInitialDataDescription)

struct TMountConfigOptimizerConfig
    : public NYTree::TYsonSerializable
{
    struct TParameter
        : public NYTree::TYsonSerializable
    {
        double Min;
        double Max;
        double Weight;
        bool Integer;
        std::vector<TString> LessThan;
        std::vector<TString> LessThanOrEqual;

        TParameter()
        {
            RegisterParameter("min", Min);
            RegisterParameter("max", Max);
            RegisterParameter("weight", Weight)
                .Default(1.0);
            RegisterParameter("integer", Integer)
                .Default(true);
            RegisterParameter("less_than", LessThan)
                .Default();
            RegisterParameter("less_than_or_equal", LessThanOrEqual)
                .Default();
        }
    };

    THashMap<TString, TIntrusivePtr<TParameter>> Parameters;

    int NumIterations;
    int NumDirections;
    double InitialStep;
    double StepMultiple;

    TMountConfigOptimizerConfig()
    {
        RegisterParameter("parameters", Parameters);

        RegisterParameter("num_iterations", NumIterations)
            .Default(30);
        RegisterParameter("num_directions", NumDirections)
            .Default(5);
        RegisterParameter("initial_step", InitialStep)
            .Default(0.2);
        RegisterParameter("step_multiple", StepMultiple)
            .Default(0.9);
    }
};

DEFINE_REFCOUNTED_TYPE(TMountConfigOptimizerConfig)

struct TLsmSimulatorConfig
    : public NYTree::TYsonSerializable
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

    TLsmSimulatorConfig()
    {
        RegisterParameter("lsm_scan_period", LsmScanPeriod)
            .Default(TDuration::Seconds(1));
        RegisterParameter("flush_period", FlushPeriod)
            .Default(TDuration::MilliSeconds(1234));

        RegisterParameter("compaction_throttler", CompactionThrottler)
            .Default(100_MB);
        RegisterParameter("partitioning_throttler", PartitioningThrottler)
            .Default(100_MB);

        RegisterParameter("min_key", MinKey)
            .Default(0);
        RegisterParameter("max_key", MaxKey)
            .Default(1000);
        RegisterParameter("min_value_size", MinValueSize)
            .Default(10_KB);
        RegisterParameter("max_value_size", MaxValueSize)
            .Default(100_KB);

        RegisterParameter("insertion_policy", InsertionPolicy)
            .Default("random");

        RegisterParameter("writers", Writers)
            .Default({New<TWriterSpec>()});

        RegisterParameter("insertion_period", InsertionPeriod)
            .Default(TDuration::MilliSeconds(100));

        RegisterParameter("increasing_begin", IncreasingBegin)
            .Default(0);
        RegisterParameter("increasing_end", IncreasingEnd)
            .Default(10000);
        RegisterParameter("increasing_splay", IncreasingSplay)
            .Default(10);

        RegisterParameter("compression_ratio", CompressionRatio)
            .Default(1.0);

        RegisterParameter("compaction_delay", CompactionDelay)
            .Default(TDuration::Seconds(3));
        RegisterParameter("partitioning_delay", PartitioningDelay)
            .Default(TDuration::Seconds(3));
        RegisterParameter("get_samples_delay", GetSamplesDelay)
            .Default(TDuration::Seconds(1));

        RegisterParameter("reverse_compaction_comparator", ReverseCompactionComparator)
            .Default(false);

        RegisterParameter("enable_structure_logging", EnableStructureLogging)
            .Default(false);

        RegisterParameter("logging_fields", LoggingFields)
            .Default();
        RegisterParameter("logging_fields_final", LoggingFieldsFinal)
            .Default();
        RegisterParameter("logging_period", LoggingPeriod)
            .Default(TDuration::Seconds(1));
        RegisterParameter("reset_statistics_after_loggers", ResetStatisticsAfterLoggers)
            .Default();
        RegisterParameter("sum_compaction_and_partitioning_in_logging", SumCompactionAndPartitioningInLogging)
            .Default(false);
        RegisterParameter("statistics_window_size", StatisticsWindowSize)
            .Default(30);

        RegisterParameter("optimizer", Optimizer)
            .Default(); // Sic!

        RegisterParameter("initial_data", InitialData)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TLsmSimulatorConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
