#include "static_table_export.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TStaticTableExportParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("output_directory", &TThis::OutputDirectory);
    registrar.Parameter("window_period", &TThis::WindowPeriod)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("closure_delta", &TThis::ClosureDelta)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("compression_codec", &TThis::CompressionCodec)
        .Default(NCompression::ECodec::Zstd_4);
    registrar.Parameter("optimize_for", &TThis::OptimizeFor)
        .Default(NTableClient::EOptimizeFor::Scan);
    registrar.Parameter("max_unpublished_window_count", &TThis::MaxUnpublishedWindowCount)
        .Default(16)
        .InRange(2, 256);
    registrar.Parameter("writer_resource_id", &TThis::WriterResourceId);
    registrar.Parameter("table_name_utc_offset", &TThis::TableNameUtcOffset)
        .Default("+03:00");

    registrar.Postprocessor([] (TThis* parameters) {
        THROW_ERROR_EXCEPTION_IF(parameters->OutputDirectory.GetPath().empty(),
            "\"output_directory\" must not be empty");
        THROW_ERROR_EXCEPTION_UNLESS(parameters->WindowPeriod >= TDuration::Seconds(1) &&
                parameters->WindowPeriod == TDuration::Seconds(parameters->WindowPeriod.Seconds()),
            "\"window_period\" must be a positive whole number of seconds");
        THROW_ERROR_EXCEPTION_UNLESS(parameters->ClosureDelta < parameters->WindowPeriod * 2,
            "\"closure_delta\" must be less than two window periods");
        // A fixed offset rather than a region: a daylight saving shift would give two windows one name.
        const auto& offset = parameters->TableNameUtcOffset;
        const auto digit = [&] (int index) {
            return offset[index] >= '0' && offset[index] <= '9';
        };
        THROW_ERROR_EXCEPTION_UNLESS(offset.size() == 6 && (offset[0] == '+' || offset[0] == '-') &&
                digit(1) && digit(2) && offset[3] == ':' && digit(4) && digit(5),
            "\"table_name_utc_offset\" must look like \"+03:00\"")
            .With("table_name_utc_offset", offset);
        const int hours = (offset[1] - '0') * 10 + (offset[2] - '0');
        const int minutes = (offset[4] - '0') * 10 + (offset[5] - '0');
        THROW_ERROR_EXCEPTION_UNLESS(hours <= 14 && minutes < 60,
            "\"table_name_utc_offset\" is out of range")
            .With("table_name_utc_offset", offset);
        parameters->TableNameUtcOffsetSeconds = (offset[0] == '-' ? -1 : 1) * (hours * 3600 + minutes * 60);
    });
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicStaticTableExportParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("publication_period", &TThis::PublicationPeriod)
        .Default(TDuration::Seconds(1))
        .GreaterThan(TDuration::Zero());
    registrar.Parameter("retry_backoff", &TThis::RetryBackoff)
        .Default(TDuration::Seconds(5))
        .GreaterThan(TDuration::Zero());
    registrar.Parameter("table_ttl", &TThis::TableTtl)
        .Optional()
        .GreaterThan(TDuration::Zero());
}

////////////////////////////////////////////////////////////////////////////////

void TStaticTableExportWriterParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("max_raw_data_weight", &TThis::MaxRawDataWeight)
        .Default(8_GB)
        .GreaterThan(0);
    registrar.Parameter("max_parsed_data_weight", &TThis::MaxParsedDataWeight)
        .Default(2_GB)
        .GreaterThan(0);
    registrar.Parameter("max_unwritten_data_weight", &TThis::MaxUnwrittenDataWeight)
        .Default(256_MB)
        .GreaterThan(0);
    registrar.Parameter("idle_flush_period", &TThis::IdleFlushPeriod)
        .Default(TDuration::Seconds(30))
        .GreaterThan(TDuration::Zero());
    registrar.Parameter("max_stream_age", &TThis::MaxStreamAge)
        .Default(TDuration::Minutes(5))
        .GreaterThan(TDuration::Zero());
    registrar.Parameter("transaction_timeout", &TThis::TransactionTimeout)
        .Default(TDuration::Minutes(1))
        .GreaterThan(TDuration::Zero());
    registrar.Parameter("commit_timeout", &TThis::CommitTimeout)
        .Default(TDuration::Minutes(5))
        .GreaterThan(TDuration::Zero());
    registrar.Parameter("io_timeout", &TThis::IOTimeout)
        .Default(TDuration::Minutes(2))
        .GreaterThan(TDuration::Zero());
    registrar.Parameter("progress_refresh_period", &TThis::ProgressRefreshPeriod)
        .Default(TDuration::Seconds(5))
        .GreaterThan(TDuration::Zero());
    registrar.Parameter("retry_backoff", &TThis::RetryBackoff)
        .Default(TDuration::Seconds(1))
        .GreaterThan(TDuration::Zero());
    registrar.Parameter("table_writer_config", &TThis::TableWriterConfig)
        .DefaultCtor([] {
            auto config = New<NTableClient::TTableWriterConfig>();
            config->UploadReplicationFactor = 3;
            config->MinUploadReplicationFactor = 2;
            config->DesiredChunkWeight = 10_GB;
            config->DesiredChunkSize = 10_GB;
            return config;
        });
    registrar.Parameter("table_writer_count", &TThis::TableWriterCount)
        .Default(4)
        .InRange(1, 16);

    registrar.Postprocessor([] (TThis* parameters) {
        THROW_ERROR_EXCEPTION_UNLESS(parameters->IdleFlushPeriod < parameters->MaxStreamAge,
            "\"idle_flush_period\" must be less than \"max_stream_age\"");
        THROW_ERROR_EXCEPTION_UNLESS(parameters->TableWriterConfig->UploadReplicationFactor == 3 &&
                parameters->TableWriterConfig->MinUploadReplicationFactor == 2,
            "Static table export requires \"upload_replication_factor\" 3 and \"min_upload_replication_factor\" 2");
    });
}

////////////////////////////////////////////////////////////////////////////////

ui64 GetStaticTableExportWindow(TSystemTimestamp timestamp, TDuration period)
{
    return timestamp.Underlying() / period.Seconds() * period.Seconds();
}

std::optional<ui64> AssignStaticTableExportWindow(
    TSystemTimestamp writeTimestamp,
    std::optional<ui64> highestWindow,
    ui64 closedThrough,
    const TStaticTableExportParametersPtr& parameters)
{
    const auto period = parameters->WindowPeriod.Seconds();
    // Late messages and write time regressions go to the earliest window the partition may still extend.
    const auto window = std::max({
        GetStaticTableExportWindow(writeTimestamp, parameters->WindowPeriod),
        highestWindow.value_or(0),
        closedThrough + period,
    });
    if (window > closedThrough + parameters->MaxUnpublishedWindowCount * period) {
        return std::nullopt;
    }
    return window;
}

bool IsStaticTableExportWindowClosable(
    ui64 window,
    TSystemTimestamp eventWatermark,
    const TStaticTableExportParametersPtr& parameters)
{
    const auto closure = (parameters->ClosureDelta + TDuration::Seconds(1) - TDuration::MicroSeconds(1)).Seconds();
    return eventWatermark.Underlying() >= window + parameters->WindowPeriod.Seconds() + closure;
}

void ValidateStaticTableExportBand(const TStaticTableExportParametersPtr& parameters, TDuration driftBound)
{
    // Closure lags the watermark by two windows plus the closure delta, and publication by up to one more window.
    const auto required = driftBound + parameters->ClosureDelta + parameters->WindowPeriod * 3;
    THROW_ERROR_EXCEPTION_UNLESS(parameters->WindowPeriod * parameters->MaxUnpublishedWindowCount > required,
        "Static table export \"max_unpublished_window_count\" * \"window_period\" must exceed "
        "\"drift_bound\" + \"closure_delta\" + 3 * \"window_period\"")
        .With("max_unpublished_window_count", parameters->MaxUnpublishedWindowCount)
        .With("window_period", parameters->WindowPeriod)
        .With("drift_bound", driftBound)
        .With("required_band", required);
}

////////////////////////////////////////////////////////////////////////////////

std::string GetStaticTableExportTableName(ui64 windowStart, i64 utcOffsetSeconds)
{
    return TInstant::Seconds(windowStart + utcOffsetSeconds).FormatGmTime("%Y-%m-%dT%H:%M:%S");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
