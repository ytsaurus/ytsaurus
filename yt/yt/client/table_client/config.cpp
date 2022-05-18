#include "config.h"

#include <yt/yt/client/tablet_client/config.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NTableClient {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TRetentionConfigPtr& obj)
{
    static const TString NullPtrName("<nullptr>");
    return obj
        ? NYson::ConvertToYsonString(obj, NYson::EYsonFormat::Text).ToString()
        : NullPtrName;
}

////////////////////////////////////////////////////////////////////////////////

void TRetentionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("min_data_versions", &TThis::MinDataVersions)
        .GreaterThanOrEqual(0)
        .Default(1);
    registrar.Parameter("max_data_versions", &TThis::MaxDataVersions)
        .GreaterThanOrEqual(0)
        .Default(1);
    registrar.Parameter("min_data_ttl", &TThis::MinDataTtl)
        .Default(TDuration::Minutes(30));
    registrar.Parameter("max_data_ttl", &TThis::MaxDataTtl)
        .Default(TDuration::Minutes(30));
    registrar.Parameter("ignore_major_timestamp", &TThis::IgnoreMajorTimestamp)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TChunkReaderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("sampling_mode", &TThis::SamplingMode)
        .Default();

    registrar.Parameter("sampling_rate", &TThis::SamplingRate)
        .Default()
        .InRange(0, 1);

    registrar.Parameter("sampling_seed", &TThis::SamplingSeed)
        .Default();

    registrar.Postprocessor([] (TThis* config) {
        if (config->SamplingRate && !config->SamplingMode) {
            config->SamplingMode = ESamplingMode::Row;
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TChunkWriterTestingOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("add_unsupported_feature", &TThis::AddUnsupportedFeature)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TChunkWriterConfig::Register(TRegistrar registrar)
{
    // Allow very small blocks for testing purposes.
    registrar.Parameter("block_size", &TThis::BlockSize)
        .GreaterThan(0)
        .Default(16_MB);

    registrar.Parameter("max_segment_value_count", &TThis::MaxSegmentValueCount)
        .GreaterThan(0)
        .Default(128 * 1024);

    registrar.Parameter("max_buffer_size", &TThis::MaxBufferSize)
        .GreaterThan(0)
        .Default(16_MB);

    registrar.Parameter("max_row_weight", &TThis::MaxRowWeight)
        .GreaterThanOrEqual(5_MB)
        .LessThanOrEqual(MaxRowWeightLimit)
        .Default(16_MB);

    registrar.Parameter("max_key_weight", &TThis::MaxKeyWeight)
        .GreaterThan(0)
        .LessThanOrEqual(MaxKeyWeightLimit)
        .Default(16_KB);

    registrar.Parameter("max_data_weight_between_blocks", &TThis::MaxDataWeightBetweenBlocks)
        .GreaterThan(0)
        .Default(2_GB);

    registrar.Parameter("max_key_filter_size", &TThis::MaxKeyFilterSize)
        .GreaterThan(0)
        .LessThanOrEqual(1_MB)
        .Default(64_KB);

    registrar.Parameter("sample_rate", &TThis::SampleRate)
        .GreaterThan(0)
        .LessThanOrEqual(0.001)
        .Default(0.0001);

    registrar.Parameter("key_filter_false_positive_rate", &TThis::KeyFilterFalsePositiveRate)
        .GreaterThan(0)
        .LessThanOrEqual(1.0)
        .Default(0.03);

    registrar.Parameter("testing_options", &TThis::TestingOptions)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TTableReaderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("suppress_access_tracking", &TThis::SuppressAccessTracking)
        .Default(false);
    registrar.Parameter("suppress_expiration_timeout_renewal", &TThis::SuppressExpirationTimeoutRenewal)
        .Default(false);
    registrar.Parameter("unavailable_chunk_strategy", &TThis::UnavailableChunkStrategy)
        .Default(EUnavailableChunkStrategy::Restore);
    registrar.Parameter("chunk_availability_policy", &TThis::ChunkAvailabilityPolicy)
        .Default(EChunkAvailabilityPolicy::Repairable);
    registrar.Parameter("max_read_duration", &TThis::MaxReadDuration)
        .Default();
    registrar.Parameter("dynamic_store_reader", &TThis::DynamicStoreReader)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

TTypeConversionConfig::TTypeConversionConfig()
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

////////////////////////////////////////////////////////////////////////////////

TInsertRowsFormatConfig::TInsertRowsFormatConfig()
{
    RegisterParameter("enable_null_to_yson_entity_conversion", EnableNullToYsonEntityConversion)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TChunkReaderOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_table_index", &TThis::EnableTableIndex)
        .Default(false);

    registrar.Parameter("enable_range_index", &TThis::EnableRangeIndex)
        .Default(false);

    registrar.Parameter("enable_row_index", &TThis::EnableRowIndex)
        .Default(false);

    registrar.Parameter("enable_tablet_index", &TThis::EnableTabletIndex)
        .Default(false);

    registrar.Parameter("dynamic_table", &TThis::DynamicTable)
        .Default(false);

    registrar.Parameter("enable_key_widening", &TThis::EnableKeyWidening)
        .Default(false);

    registrar.Postprocessor([] (TThis* config) {
        if (config->EnableRangeIndex && !config->EnableRowIndex) {
            THROW_ERROR_EXCEPTION("\"enable_row_index\" must be set when \"enable_range_index\" is set");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TChunkWriterOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("validate_sorted", &TThis::ValidateSorted)
        .Default(true);
    registrar.Parameter("validate_row_weight", &TThis::ValidateRowWeight)
        .Default(false);
    registrar.Parameter("validate_key_weight", &TThis::ValidateKeyWeight)
        .Default(false);
    registrar.Parameter("validate_duplicate_ids", &TThis::ValidateDuplicateIds)
        .Default(false);
    registrar.Parameter("validate_column_count", &TThis::ValidateColumnCount)
        .Default(false);
    registrar.Parameter("validate_unique_keys", &TThis::ValidateUniqueKeys)
        .Default(false);
    registrar.Parameter("explode_on_validation_error", &TThis::ExplodeOnValidationError)
        .Default(false);
    registrar.Parameter("optimize_for", &TThis::OptimizeFor)
        .Default(EOptimizeFor::Lookup);
    registrar.Parameter("evaluate_computed_columns", &TThis::EvaluateComputedColumns)
        .Default(true);
    registrar.Parameter("enable_skynet_sharing", &TThis::EnableSkynetSharing)
        .Default(false);
    registrar.Parameter("return_boundary_keys", &TThis::ReturnBoundaryKeys)
        .Default(true);
    registrar.Parameter("cast_any_to_composite", &TThis::CastAnyToCompositeNode)
        .Default();
    registrar.Parameter("schema_modification", &TThis::SchemaModification)
        .Default(ETableSchemaModification::None);
    registrar.Parameter("max_heavy_columns", &TThis::MaxHeavyColumns)
        .Default(0);

    registrar.Postprocessor([] (TThis* config) {
        if (config->ValidateUniqueKeys && !config->ValidateSorted) {
            THROW_ERROR_EXCEPTION("\"validate_unique_keys\" is allowed to be true only if \"validate_sorted\" is true");
        }

        if (config->CastAnyToCompositeNode) {
            try {
                config->CastAnyToComposite = NYTree::ConvertTo<bool>(config->CastAnyToCompositeNode);
            } catch (const std::exception&) {
                // COMPAT: Do nothing for backward compatibility.
            }
        }

        switch (config->SchemaModification) {
            case ETableSchemaModification::None:
                break;

            case ETableSchemaModification::UnversionedUpdate:
                if (!config->ValidateSorted || !config->ValidateUniqueKeys) {
                    THROW_ERROR_EXCEPTION(
                        "\"schema_modification\" is allowed to be %Qlv only if "
                        "\"validate_sorted\" and \"validate_unique_keys\" are true",
                        config->SchemaModification);
                }
                break;

            case ETableSchemaModification::UnversionedUpdateUnsorted:
                THROW_ERROR_EXCEPTION("\"schema_modification\" is not allowed to be %Qlv",
                    config->SchemaModification);

            default:
                YT_ABORT();
        }
    });
}

void TChunkWriterOptions::EnableValidationOptions()
{
    ValidateDuplicateIds = true;
    ValidateRowWeight = true;
    ValidateKeyWeight = true;
    ValidateColumnCount = true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
