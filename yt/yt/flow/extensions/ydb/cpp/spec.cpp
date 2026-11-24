#include "spec.h"

#include "attributes.h"
#include "helpers.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TYdbTopicSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("endpoint", &TThis::Endpoint)
        .NonEmpty();
    registrar.Parameter("database", &TThis::Database)
        .NonEmpty();
    registrar.Parameter("topic", &TThis::Topic)
        .NonEmpty();
    registrar.Parameter("client_controller_timeout", &TThis::ClientControllerTimeout)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("update_partition_count_period", &TThis::UpdatePartitionCountPeriod)
        .Default(TDuration::Seconds(60));
}

////////////////////////////////////////////////////////////////////////////////

void TYdbSourceParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("consumer", &TThis::Consumer);

    registrar.Parameter("max_memory_usage_bytes", &TThis::MaxMemoryUsageBytes)
        .Default(50_MB);
    registrar.Parameter("start_session_timeout", &TThis::StartSessionTimeout)
        .Default(TDuration::Seconds(300));

    registrar.Parameter("try_parse_flow_queue_meta", &TThis::TryParseFlowQueueMeta)
        .Default(true);
    registrar.Parameter("flow_queue_meta_name", &TThis::FlowQueueMetaName)
        .Default(DefaultFlowQueueMetaName);
    registrar.Parameter("ignore_malformed_flow_queue_meta", &TThis::IgnoreMalformedFlowQueueMeta)
        .Default(false);
    registrar.Parameter("attributes_column", &TThis::AttributesColumn)
        .Default();
    registrar.Parameter("partition_filter", &TThis::PartitionFilter)
        .Default();

    registrar.Postprocessor([] (TThis* parameters) {
        if (parameters->AttributesColumn &&
            (*parameters->AttributesColumn == "data" ||
                *parameters->AttributesColumn == "unparsed" ||
                *parameters->AttributesColumn == "offset" ||
                *parameters->AttributesColumn == "sub_offset"))
        {
            THROW_ERROR_EXCEPTION("Column %Qv is already present in the topic source schema",
                *parameters->AttributesColumn);
        }
        if (parameters->PartitionFilter) {
            for (const auto& [begin, end] : *parameters->PartitionFilter) {
                THROW_ERROR_EXCEPTION_IF(begin < 0 || end <= begin,
                    "Each partition_filter interval must satisfy 0 <= begin < end, got [%v, %v)",
                    begin,
                    end);
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicYdbSourceParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("malformed_message_policy", &TThis::MalformedMessagePolicy)
        .Default(EMalformedYdbMessagePolicy::Fail);
    registrar.Parameter("read_from_instant", &TThis::ReadFromInstant)
        .Default(std::nullopt);
    registrar.Parameter("max_time_lag", &TThis::MaxTimeLag)
        .Default(std::nullopt);
    registrar.Parameter("commit_timeout", &TThis::CommitTimeout)
        .Default(TDuration::Minutes(2));
    // Silence is measured against the probe cadence: the watchdog requests partition status once per
    // |UpdateInfoPeriod| (15 s by default), so a minute means several unanswered requests rather than one
    // late one. Transport-level breakage is already caught by the driver's gRPC keepalive, so this only
    // has to cover an application-level wedge.
    registrar.Parameter("session_silence_timeout", &TThis::SessionSilenceTimeout)
        .Default(TDuration::Minutes(1));
}

////////////////////////////////////////////////////////////////////////////////

void TYdbSourcePartitionSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("read_blocked", &TThis::ReadBlocked)
        .Default(false);
    registrar.Parameter("retired", &TThis::Retired)
        .Default(false);
    registrar.Parameter("final_offset_exclusive", &TThis::FinalOffsetExclusive)
        .Default();
    registrar.Parameter("completion_allowed", &TThis::CompletionAllowed)
        .Default(true);
    registrar.Parameter("topology_unavailable", &TThis::TopologyUnavailable)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TYdbSourcePartitionStatus::Register(TRegistrar registrar)
{
    registrar.Parameter("input_exhausted", &TThis::InputExhausted)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TYdbFramingSourceParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("framing_format", &TThis::FramingFormat)
        .Default(EFramingFormat::Auto);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicYdbFramingSourceParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TYdbMultiLineSourceParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("row_delimiter", &TThis::RowDelimiter)
        .Default('\n');
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicYdbMultiLineSourceParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TCommonYdbSinkParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("payload_column", &TThis::PayloadColumn)
        .Default("data");

    registrar.Parameter("attributes_column", &TThis::AttributesColumn)
        .Default();

    registrar.Parameter("max_memory_usage_bytes", &TThis::MaxMemoryUsageBytes)
        .Default(50_MB);

    registrar.Parameter("codec", &TThis::Codec)
        .Default("zstd");

    registrar.Parameter("compression_level", &TThis::CompressionLevel)
        .Default(4);

    registrar.Parameter("write_flow_queue_meta", &TThis::WriteFlowQueueMeta)
        .Default(false);

    registrar.Parameter("flow_queue_meta_name", &TThis::FlowQueueMetaName)
        .Default(DefaultFlowQueueMetaName);

    registrar.Postprocessor([] (TThis* spec) {
        ParseYdbCodec(spec->Codec);
    });
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicCommonYdbSinkParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("flow_queue_meta_heartbeat_period", &TThis::FlowQueueMetaHeartbeatPeriod)
        .Default(TDuration::Seconds(10));
}

////////////////////////////////////////////////////////////////////////////////

void TYdbSinkParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TDynamicYdbSinkParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TYdbFramingSinkParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("framing_format", &TThis::FramingFormat)
        .Default(EFramingFormat::Lenval);

    // Framing packs several messages into a single topic message, so there is no
    // non-arbitrary way to pick the attributes of the result.
    registrar.Postprocessor([] (TThis* parameters) {
        if (parameters->AttributesColumn) {
            THROW_ERROR_EXCEPTION("%v does not support the %Qv parameter",
                "NYT::NFlow::TYdbFramingSink",
                "attributes_column");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicYdbFramingSinkParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TYdbTwoPhaseCommitSinkParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("framing_format", &TThis::FramingFormat)
        .Optional();
    registrar.Postprocessor([] (TThis* parameters) {
        if (parameters->FramingFormat && parameters->AttributesColumn) {
            THROW_ERROR_EXCEPTION("Framing does not support the %Qv parameter", "attributes_column");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicYdbTwoPhaseCommitSinkParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("max_rows_per_batch", &TThis::MaxRowsPerBatch)
        .InRange(1, 2000)
        .Default(1000);
    registrar.Parameter("max_bytes_per_batch", &TThis::MaxBytesPerBatch)
        .InRange(1, 10_MB)
        .Default(5_MB);
    registrar.Parameter("preparation_attempt_timeout", &TThis::PreparationAttemptTimeout)
        .GreaterThan(TDuration::Zero())
        .Default(TDuration::Seconds(90));
    registrar.Parameter("preparation_target_backoff", &TThis::PreparationTargetBackoff)
        .Default(TDuration::Seconds(30));
}

////////////////////////////////////////////////////////////////////////////////

void TYdbAtLeastOnceSinkParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TDynamicYdbAtLeastOnceSinkParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TYdbAtLeastOnceFramingSinkParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("framing_format", &TThis::FramingFormat)
        .Default(EFramingFormat::Lenval);

    // Framing packs several messages into a single topic message, so there is no
    // non-arbitrary way to pick the attributes of the result.
    registrar.Postprocessor([] (TThis* parameters) {
        if (parameters->AttributesColumn) {
            THROW_ERROR_EXCEPTION("%v does not support the %Qv parameter",
                "NYT::NFlow::TYdbAtLeastOnceFramingSink",
                "attributes_column");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicYdbAtLeastOnceFramingSinkParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("max_rows_per_batch", &TThis::MaxRowsPerBatch)
        .InRange(1, 2000)
        .Default(1000);
    registrar.Parameter("max_bytes_per_batch", &TThis::MaxBytesPerBatch)
        .InRange(1, 10_MB)
        .Default(5_MB);
}

////////////////////////////////////////////////////////////////////////////////

void TYdbSinkControllerParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TDynamicYdbSinkControllerParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
