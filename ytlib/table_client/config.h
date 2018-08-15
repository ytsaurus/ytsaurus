#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/config.h>

#include <yt/client/table_client/config.h>

namespace NYT {
namespace NTableClient {

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

        RegisterPostprocessor([&] {
            if (EnableTypeConversion) {
                EnableStringToAllConversion = true;
                EnableAllToStringConversion = true;
                EnableIntegralTypeConversion = true;
                EnableIntegralToDoubleConversion = true;
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TTypeConversionConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
