#include "spec.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TKafkaTopicSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("topic", &TThis::Topic)
        .NonEmpty();
}

////////////////////////////////////////////////////////////////////////////////

void TKafkaInfoSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("update_partition_count_period", &TThis::UpdatePartitionCountPeriod)
        .GreaterThan(TDuration::Zero())
        .Default(TDuration::Seconds(60));
    registrar.Parameter("metadata_timeout", &TThis::MetadataTimeout)
        .GreaterThan(TDuration::Zero())
        .Default(TDuration::Seconds(30));
}

////////////////////////////////////////////////////////////////////////////////

void TKafkaSourceParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("group_id", &TThis::GroupId)
        .NonEmpty();
    registrar.Parameter("partition_filter", &TThis::PartitionFilter)
        .Default();
    registrar.Parameter("max_buffer_bytes", &TThis::MaxBufferBytes)
        .GreaterThan(0)
        .Default(64ll * 1024 * 1024);
    registrar.Parameter("use_consumer_group_offset", &TThis::UseConsumerGroupOffset)
        .Default(true);

    registrar.Postprocessor([] (TThis* parameters) {
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

void TDynamicKafkaSourceParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("poll_timeout", &TThis::PollTimeout)
        .GreaterThan(TDuration::Zero())
        .Default(TDuration::MilliSeconds(200));
    registrar.Parameter("watermark_update_period", &TThis::WatermarkUpdatePeriod)
        .GreaterThan(TDuration::Zero())
        .Default(TDuration::Seconds(5));
    registrar.Parameter("malformed_message_policy", &TThis::MalformedMessagePolicy)
        .Default(EMalformedKafkaMessagePolicy::Fail);
}

////////////////////////////////////////////////////////////////////////////////

void TCommonKafkaSinkParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("payload_column", &TThis::PayloadColumn)
        .Default("data");
    registrar.Parameter("key_column", &TThis::KeyColumn)
        .Default();
    registrar.Parameter("message_id_header", &TThis::MessageIdHeader)
        .Default();
}

void TKafkaSinkParameters::Register(TRegistrar /*registrar*/)
{ }

void TDynamicKafkaSinkParameters::Register(TRegistrar /*registrar*/)
{ }

void TAtLeastOnceKafkaSinkParameters::Register(TRegistrar /*registrar*/)
{ }

void TDynamicAtLeastOnceKafkaSinkParameters::Register(TRegistrar /*registrar*/)
{ }

void TKafkaSinkControllerParameters::Register(TRegistrar /*registrar*/)
{ }

void TDynamicKafkaSinkControllerParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
