#include "spec.h"

#include <yt/yt/flow/library/cpp/common/yt_path_option.h>

#include <util/datetime/systime.h>

namespace NYT::NFlow::NStaticTableConnector {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TArrivalOrderTableSinkParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("output_directory", &TThis::OutputDirectory)
        .AddOption(EYTPathOwnership::ExclusiveWrite);
    registrar.Parameter("table_period", &TThis::TablePeriod)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("table_ttl", &TThis::TableTtl);
    registrar.Parameter("table_name_format", &TThis::TableNameFormat)
        .Default("%Y-%m-%dT%H:%M:%SZ");
    registrar.Parameter("data_weight_column", &TThis::DataWeightColumn)
        .Default();

    registrar.Postprocessor([] (TThis* spec) {
        THROW_ERROR_EXCEPTION_UNLESS(spec->TablePeriod > TDuration::Zero(),
            "Table period must be positive");
        THROW_ERROR_EXCEPTION_UNLESS(spec->TableTtl > TDuration::Zero(),
            "Table TTL must be positive");
        // The Cypress child-count limit is 50000; while the slot sequence tracks the wall clock,
        // live tables are bounded by TableTtl / TablePeriod.
        THROW_ERROR_EXCEPTION_IF(spec->TableTtl.GetValue() > spec->TablePeriod.GetValue() * 40000,
            "Table TTL %v spans more than 40000 table periods %v: "
            "the output directory would exceed the Cypress child count limit",
            spec->TableTtl,
            spec->TablePeriod);
        THROW_ERROR_EXCEPTION_IF(spec->OutputDirectory.GetPath().empty(),
            "\"output_directory\" %v must not be empty",
            spec->OutputDirectory);
        THROW_ERROR_EXCEPTION_UNLESS(spec->TablePeriod == TDuration::Seconds(spec->TablePeriod.Seconds()),
            "Table period must be a whole number of seconds");
        THROW_ERROR_EXCEPTION_IF(spec->TableNameFormat.find('/') != std::string::npos,
            "Table name format %Qv must not contain path separators",
            spec->TableNameFormat);
        // The table name is the slot's only identity in Cypress, so the format must render the
        // timestamp losslessly: otherwise two slots would collide on one table name.
        const auto sample = TInstant::ParseIso8601("2345-06-17T18:29:40Z");
        const auto formatted = sample.FormatGmTime(spec->TableNameFormat.c_str());
        struct tm parsed
        { };
        const char* parseEnd = strptime(formatted.c_str(), spec->TableNameFormat.c_str(), &parsed);
        THROW_ERROR_EXCEPTION_IF(
            !parseEnd || *parseEnd != '\0' || TInstant::Seconds(TimeGM(&parsed)) != sample,
            "Table name format %Qv does not render the table timestamp losslessly",
            spec->TableNameFormat);
    });
}

void TDynamicArrivalOrderTableSinkParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("max_row_count", &TThis::MaxRowCount)
        .Default(10000)
        .GreaterThan(0);
    registrar.Parameter("max_data_weight", &TThis::MaxDataWeight)
        .Default(1_GB)
        .GreaterThan(0);
    registrar.Parameter("transaction_timeout", &TThis::TransactionTimeout)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("retry_backoff", &TThis::RetryBackoff)
        .Default(TDuration::Seconds(1))
        .GreaterThan(TDuration::Zero());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NStaticTableConnector
