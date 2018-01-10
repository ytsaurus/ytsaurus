#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/config.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkWriterConfig
    : public NChunkClient::TEncodingWriterConfig
{
public:
    i64 BlockSize;

    i64 MaxBufferSize;

    i64 MaxRowWeight;

    i64 MaxKeyWeight;

    //! This limits ensures that chunk index is dense enough
    //! e.g. to produce good slices for reduce.
    i64 MaxDataWeightBetweenBlocks;

    i64 MaxKeyFilterSize;

    double SampleRate;

    double KeyFilterFalsePositiveRate;

    TChunkWriterConfig()
    {
        // Allow very small blocks for testing purposes.
        RegisterParameter("block_size", BlockSize)
            .GreaterThanOrEqual((i64) 1024)
            .Default((i64) 16 * 1024 * 1024);

        RegisterParameter("max_buffer_size", MaxBufferSize)
            .GreaterThanOrEqual((i64) 5 * 1024 * 1024)
            .Default((i64) 16 * 1024 * 1024);

        RegisterParameter("max_row_weight", MaxRowWeight)
            .GreaterThanOrEqual((i64) 5 * 1024 * 1024)
            .LessThanOrEqual(MaxRowWeightLimit)
            .Default((i64) 16 * 1024 * 1024);

        RegisterParameter("max_key_weight", MaxKeyWeight)
            .GreaterThanOrEqual((i64) 1)
            .LessThanOrEqual(MaxKeyWeightLimit)
            .Default((i64) 16 * 1024);

        RegisterParameter("max_data_weight_between_blocks", MaxDataWeightBetweenBlocks)
            .GreaterThanOrEqual((i64) 1)
            .Default((i64) 2 * 1024 * 1024 * 1024);

        RegisterParameter("max_key_filter_size", MaxKeyFilterSize)
            .GreaterThan((i64) 0)
            .LessThanOrEqual((i64) 1024 * 1024)
            .Default((i64) 64 * 1024);

        RegisterParameter("sample_rate", SampleRate)
            .GreaterThan(0)
            .LessThanOrEqual(0.001)
            .Default(0.0001);

        RegisterParameter("key_filter_false_positive_rate", KeyFilterFalsePositiveRate)
            .GreaterThan(0)
            .LessThanOrEqual(1.0)
            .Default(0.03);
    }
};

DEFINE_REFCOUNTED_TYPE(TChunkWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TChunkWriterOptions
    : public virtual NChunkClient::TEncodingWriterOptions
{
public:
    bool ValidateSorted;
    bool ValidateRowWeight;
    bool ValidateKeyWeight;
    bool ValidateDuplicateIds;
    bool ValidateUniqueKeys;
    bool ExplodeOnValidationError;
    bool ValidateColumnCount;
    bool EvaluateComputedColumns;
    bool EnableSkynetSharing;
    bool ReturnBoundaryKeys;

    EOptimizeFor OptimizeFor;

    TChunkWriterOptions()
    {
        RegisterParameter("validate_sorted", ValidateSorted)
            .Default(true);
        RegisterParameter("validate_row_weight", ValidateRowWeight)
            .Default(false);
        RegisterParameter("validate_key_weight", ValidateKeyWeight)
            .Default(false);
        RegisterParameter("validate_duplicate_ids", ValidateDuplicateIds)
            .Default(false);
        RegisterParameter("validate_column_count", ValidateColumnCount)
            .Default(false);
        RegisterParameter("validate_unique_keys", ValidateUniqueKeys)
            .Default(false);
        RegisterParameter("explode_on_validation_error", ExplodeOnValidationError)
            .Default(false);
        RegisterParameter("optimize_for", OptimizeFor)
            .Default(EOptimizeFor::Lookup);
        RegisterParameter("evaluate_computed_columns", EvaluateComputedColumns)
            .Default(true);
        RegisterParameter("enable_skynet_sharing", EnableSkynetSharing)
            .Default(false);
        RegisterParameter("return_boundary_keys", ReturnBoundaryKeys)
            .Default(true);

        RegisterPostprocessor([&] () {
            if (ValidateUniqueKeys && !ValidateSorted) {
                THROW_ERROR_EXCEPTION("\"validate_unique_keys\" is allowed to be true only if \"validate_sorted\" is true");
            }
        });
    }

    void EnableValidationOptions()
    {
        ValidateDuplicateIds = true;
        ValidateRowWeight = true;
        ValidateKeyWeight = true;
        ValidateColumnCount = true;
    }
};

DEFINE_REFCOUNTED_TYPE(TChunkWriterOptions)

////////////////////////////////////////////////////////////////////////////////

class TTableWriterOptions
    : public TChunkWriterOptions
    , public NChunkClient::TMultiChunkWriterOptions
{ };

DEFINE_REFCOUNTED_TYPE(TTableWriterOptions)

////////////////////////////////////////////////////////////////////////////////

class TTableWriterConfig
    : public TChunkWriterConfig
    , public NChunkClient::TMultiChunkWriterConfig
{
public:
    TDuration UploadTransactionTimeout;

    TTableWriterConfig()
    {
        RegisterParameter("upload_transaction_timeout", UploadTransactionTimeout)
            .Default(TDuration::Seconds(15));
    }
};

DEFINE_REFCOUNTED_TYPE(TTableWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TBlobTableWriterConfig
    : public NTableClient::TTableWriterConfig
{
public:
    i64 MaxPartSize;

    TBlobTableWriterConfig()
    {
        RegisterParameter("max_part_size", MaxPartSize)
            .Default(4 * 1024 * 1024)
            .GreaterThanOrEqual(1 * 1024 * 1024)
            .LessThanOrEqual(MaxRowWeightLimit);
    }
};

DEFINE_REFCOUNTED_TYPE(TBlobTableWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TBufferedTableWriterConfig
    : public TTableWriterConfig
{
public:
    TDuration RetryBackoffTime;
    TDuration FlushPeriod;
    i64 RowBufferChunkSize;

    TBufferedTableWriterConfig()
    {
        RegisterParameter("retry_backoff_time", RetryBackoffTime)
            .Default(TDuration::Seconds(3));
        RegisterParameter("flush_period", FlushPeriod)
            .Default(TDuration::Seconds(60));
        RegisterParameter("row_buffer_chunk_size", RowBufferChunkSize)
            .Default(64 * 1024);
    }
};

DEFINE_REFCOUNTED_TYPE(TBufferedTableWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TChunkReaderConfig
    : public virtual NChunkClient::TBlockFetcherConfig
{
public:
    i64 MaxDataSizePerRead;

    TNullable<double> SamplingRate;
    TNullable<ui64> SamplingSeed;

    TChunkReaderConfig()
    {
        RegisterParameter("max_data_size_per_read", MaxDataSizePerRead)
            .GreaterThanOrEqual((i64) 1024 * 1024)
            .Default((i64) 16 * 1024 * 1024);

        RegisterParameter("sampling_rate", SamplingRate)
            .Default()
            .InRange(0, 1);

        RegisterParameter("sampling_seed", SamplingSeed)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TChunkReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TTableReaderConfig
    : public virtual NChunkClient::TMultiChunkReaderConfig
    , public virtual TChunkReaderConfig
{
public:
    bool SuppressAccessTracking;
    EUnavailableChunkStrategy UnavailableChunkStrategy;

    TTableReaderConfig()
    {
        RegisterParameter("suppress_access_tracking", SuppressAccessTracking)
            .Default(false);
        RegisterParameter("unavailable_chunk_strategy", UnavailableChunkStrategy)
            .Default(EUnavailableChunkStrategy::Restore);
    }
};

DEFINE_REFCOUNTED_TYPE(TTableReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TChunkReaderOptions
    : public virtual NYTree::TYsonSerializable
{
public:
    bool EnableTableIndex;
    bool EnableRangeIndex;
    bool EnableRowIndex;
    bool DynamicTable;

    TChunkReaderOptions()
    {
        RegisterParameter("enable_table_index", EnableTableIndex)
            .Default(false);

        RegisterParameter("enable_range_index", EnableRangeIndex)
            .Default(false);

        RegisterParameter("enable_row_index", EnableRowIndex)
            .Default(false);

        RegisterParameter("dynamic_table", DynamicTable)
            .Default(false);

        RegisterPostprocessor([&] () {
            if (EnableRangeIndex && !EnableRowIndex) {
                THROW_ERROR_EXCEPTION("\"enable_range_index\" must be set when \"enable_row_index\" is set");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TChunkReaderOptions)

////////////////////////////////////////////////////////////////////////////////

class TTableReaderOptions
    : public TChunkReaderOptions
    , public NChunkClient::TMultiChunkReaderOptions
{ };

DEFINE_REFCOUNTED_TYPE(TTableReaderOptions)

////////////////////////////////////////////////////////////////////////////////

class TRetentionConfig
    : public NYTree::TYsonSerializable
{
public:
    int MinDataVersions;
    int MaxDataVersions;
    TDuration MinDataTtl;
    TDuration MaxDataTtl;

    TRetentionConfig()
    {
        RegisterParameter("min_data_versions", MinDataVersions)
            .GreaterThanOrEqual(0)
            .Default(1);
        RegisterParameter("max_data_versions", MaxDataVersions)
            .GreaterThanOrEqual(0)
            .Default(1);
        RegisterParameter("min_data_ttl", MinDataTtl)
            .Default(TDuration::Minutes(30));
        RegisterParameter("max_data_ttl", MaxDataTtl)
            .Default(TDuration::Minutes(30));
    }
};

DEFINE_REFCOUNTED_TYPE(TRetentionConfig)

////////////////////////////////////////////////////////////////////////////////

class TTypeConversionConfig
    : public NYTree::TYsonSerializable
{
public:
    bool EnableTypeConversion;
    bool EnableStringToAllConversion;
    bool EnableAllToStringConversion;
    bool EnableIntegralTypeConversion;
    bool EnableIntegralToDoubleConversion;

    TTypeConversionConfig()
    {
        RegisterParameter("enable_type_conversion", EnableTypeConversion)
            .Default(false);
        RegisterParameter("enable_string_to_all_conversion", EnableStringToAllConversion)
            .Default(false);
        RegisterParameter("enable_all_to_string_conversion", EnableAllToStringConversion)
            .Default(false);
        RegisterParameter("enable_integral_type_conversion", EnableIntegralTypeConversion)
            .Default(true);
        RegisterParameter("enable_integral_to_double_conversion", EnableIntegralToDoubleConversion)
            .Default(false);
    }

    virtual void OnLoaded() override
    {
        if (EnableTypeConversion) {
            EnableStringToAllConversion = EnableAllToStringConversion = EnableIntegralTypeConversion = EnableIntegralToDoubleConversion = true;
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TTypeConversionConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
