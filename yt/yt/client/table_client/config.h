#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_serializable.h>

#include <yt/yt/client/chunk_client/config.h>

#include <yt/yt/client/tablet_client/config.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TRetentionConfig
    : public NYTree::TYsonSerializable
{
public:
    int MinDataVersions;
    int MaxDataVersions;
    TDuration MinDataTtl;
    TDuration MaxDataTtl;
    bool IgnoreMajorTimestamp;

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
        RegisterParameter("ignore_major_timestamp", IgnoreMajorTimestamp)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TRetentionConfig)

TString ToString(const TRetentionConfigPtr& obj);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESamplingMode,
    ((Row)               (1))
    ((Block)             (2))
);

class TChunkReaderConfig
    : public virtual NChunkClient::TBlockFetcherConfig
{
public:
    std::optional<ESamplingMode> SamplingMode;
    std::optional<double> SamplingRate;
    std::optional<ui64> SamplingSeed;

    TChunkReaderConfig()
    {
        RegisterParameter("sampling_mode", SamplingMode)
            .Default();

        RegisterParameter("sampling_rate", SamplingRate)
            .Default()
            .InRange(0, 1);

        RegisterParameter("sampling_seed", SamplingSeed)
            .Default();

        RegisterPostprocessor([&] {
            if (SamplingRate && !SamplingMode) {
                SamplingMode = ESamplingMode::Row;
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TChunkReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TChunkWriterTestingOptions
    : public NYTree::TYsonSerializable
{
public:
    //! If true, unsupported chunk feature is added to chunk meta.
    bool AddUnsupportedFeature;

    TChunkWriterTestingOptions()
    {
        RegisterParameter("add_unsupported_feature", AddUnsupportedFeature)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TChunkWriterTestingOptions)

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

    TChunkWriterTestingOptionsPtr TestingOptions;

    TChunkWriterConfig()
    {
        // Allow very small blocks for testing purposes.
        RegisterParameter("block_size", BlockSize)
            .GreaterThan(0)
            .Default(16_MB);

        RegisterParameter("max_buffer_size", MaxBufferSize)
            .GreaterThan(0)
            .Default(16_MB);

        RegisterParameter("max_row_weight", MaxRowWeight)
            .GreaterThanOrEqual(5_MB)
            .LessThanOrEqual(MaxRowWeightLimit)
            .Default(16_MB);

        RegisterParameter("max_key_weight", MaxKeyWeight)
            .GreaterThan(0)
            .LessThanOrEqual(MaxKeyWeightLimit)
            .Default(16_KB);

        RegisterParameter("max_data_weight_between_blocks", MaxDataWeightBetweenBlocks)
            .GreaterThan(0)
            .Default(2_GB);

        RegisterParameter("max_key_filter_size", MaxKeyFilterSize)
            .GreaterThan(0)
            .LessThanOrEqual(1_MB)
            .Default(64_KB);

        RegisterParameter("sample_rate", SampleRate)
            .GreaterThan(0)
            .LessThanOrEqual(0.001)
            .Default(0.0001);

        RegisterParameter("key_filter_false_positive_rate", KeyFilterFalsePositiveRate)
            .GreaterThan(0)
            .LessThanOrEqual(1.0)
            .Default(0.03);

        RegisterParameter("testing_options", TestingOptions)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TChunkWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TTableReaderConfig
    : public virtual NChunkClient::TMultiChunkReaderConfig
    , public virtual TChunkReaderConfig
{
public:
    bool SuppressAccessTracking;
    bool SuppressExpirationTimeoutRenewal;
    EUnavailableChunkStrategy UnavailableChunkStrategy;
    std::optional<TDuration> MaxReadDuration;

    NTabletClient::TRetryingRemoteDynamicStoreReaderConfigPtr DynamicStoreReader;

    TTableReaderConfig()
    {
        RegisterParameter("suppress_access_tracking", SuppressAccessTracking)
            .Default(false);
        RegisterParameter("suppress_expiration_timeout_renewal", SuppressExpirationTimeoutRenewal)
            .Default(false);
        RegisterParameter("unavailable_chunk_strategy", UnavailableChunkStrategy)
            .Default(EUnavailableChunkStrategy::Restore);
        RegisterParameter("max_read_duration", MaxReadDuration)
            .Default();
        RegisterParameter("dynamic_store_reader", DynamicStoreReader)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TTableReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TTableWriterConfig
    : public TChunkWriterConfig
    , public NChunkClient::TMultiChunkWriterConfig
{ };

DEFINE_REFCOUNTED_TYPE(TTableWriterConfig)

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

class TChunkReaderOptions
    : public virtual NYTree::TYsonSerializable
{
public:
    bool EnableTableIndex;
    bool EnableRangeIndex;
    bool EnableRowIndex;
    bool DynamicTable;
    bool EnableTabletIndex;

    TChunkReaderOptions()
    {
        RegisterParameter("enable_table_index", EnableTableIndex)
            .Default(false);

        RegisterParameter("enable_range_index", EnableRangeIndex)
            .Default(false);

        RegisterParameter("enable_row_index", EnableRowIndex)
            .Default(false);

        RegisterParameter("enable_tablet_index", EnableTabletIndex)
            .Default(false);

        RegisterParameter("dynamic_table", DynamicTable)
            .Default(false);

        RegisterPostprocessor([&] () {
            if (EnableRangeIndex && !EnableRowIndex) {
                THROW_ERROR_EXCEPTION("\"enable_row_index\" must be set when \"enable_range_index\" is set");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TChunkReaderOptions)

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
    bool CastAnyToComposite = false;
    NYTree::INodePtr CastAnyToCompositeNode;

    ETableSchemaModification SchemaModification;

    EOptimizeFor OptimizeFor;

    //! Maximum number of heavy columns in approximate statistics.
    int MaxHeavyColumns;

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
        RegisterParameter("cast_any_to_composite", CastAnyToCompositeNode)
            .Default();
        RegisterParameter("schema_modification", SchemaModification)
            .Default(ETableSchemaModification::None);
        RegisterParameter("max_heavy_columns", MaxHeavyColumns)
            .Default(0);

        RegisterPostprocessor([&] {
            if (ValidateUniqueKeys && !ValidateSorted) {
                THROW_ERROR_EXCEPTION("\"validate_unique_keys\" is allowed to be true only if \"validate_sorted\" is true");
            }

            if (CastAnyToCompositeNode) {
                try {
                    CastAnyToComposite = NYTree::ConvertTo<bool>(CastAnyToCompositeNode);
                } catch (const TErrorException&) {
                    // COMPAT: Do nothing for backward compatibility.
                }
            }

            switch (SchemaModification) {
                case ETableSchemaModification::None:
                    break;

                case ETableSchemaModification::UnversionedUpdate:
                    if (!ValidateSorted || !ValidateUniqueKeys) {
                        THROW_ERROR_EXCEPTION(
                            "\"schema_modification\" is allowed to be %Qlv only if "
                            "\"validate_sorted\" and \"validate_unique_keys\" are true",
                            SchemaModification);
                    }
                    break;

                case ETableSchemaModification::UnversionedUpdateUnsorted:
                    THROW_ERROR_EXCEPTION("\"schema_modification\" is not allowed to be %Qlv",
                        SchemaModification);

                default:
                    YT_ABORT();
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

struct TRowBatchReadOptions
{
    //! The desired number of rows to read.
    //! This is just an estimate; not all readers support this limit.
    i64 MaxRowsPerRead = 10000;

    //! The desired data weight to read.
    //! This is just an estimate; not all readers support this limit.
    i64 MaxDataWeightPerRead = 16_MB;

    //! If true then the reader may return a columnar batch.
    //! If false then the reader must return a non-columnar batch.
    bool Columnar = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
