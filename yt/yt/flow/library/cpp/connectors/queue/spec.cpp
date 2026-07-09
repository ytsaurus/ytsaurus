#include "spec.h"

#include <yt/yt/flow/library/cpp/common/yt_path_option.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TQueueInfoSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("queue_path", &TThis::QueuePath)
        .AddOption(EYTPathOwnership::ReadOnly);

    registrar.Parameter("update_partition_count_period", &TThis::UpdatePartitionCountPeriod)
        .Default(TDuration::Seconds(60));
}

////////////////////////////////////////////////////////////////////////////////

void TQueueSourceParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("consumer_path", &TThis::ConsumerPath)
        .AddOption(EYTPathOwnership::SharedWrite);

    registrar.Parameter("try_parse_flow_queue_meta", &TThis::TryParseFlowQueueMeta)
        .Default(true);
    registrar.Parameter("flow_queue_meta_column", &TThis::FlowQueueMetaColumn)
        .Default("flow_queue_meta");
    registrar.Parameter("ignore_malformed_flow_queue_meta", &TThis::IgnoreMalformedFlowQueueMeta)
        .Default(false);
    registrar.Parameter("partition_filter", &TThis::PartitionFilter)
        .Default();

    registrar.Postprocessor([] (TQueueSourceParameters* parameters) {
        THROW_ERROR_EXCEPTION_IF(!parameters->ConsumerPath.GetCluster().has_value(), "ConsumerPath should have cluster");
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

void TDynamicQueueSourceParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("pull_queue_timeout", &TThis::PullQueueTimeout)
        .Default(TDuration::Seconds(60));
}

////////////////////////////////////////////////////////////////////////////////

void TCommonQueueSinkParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("write_flow_queue_meta", &TThis::WriteFlowQueueMeta)
        .Default(false);

    registrar.Parameter("flow_queue_meta_column", &TThis::FlowQueueMetaColumn)
        .Default("flow_queue_meta");
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicCommonQueueSinkParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("flow_queue_meta_heartbeat_period", &TThis::FlowQueueMetaHeartbeatPeriod)
        .Default(TDuration::Seconds(10));
}

////////////////////////////////////////////////////////////////////////////////

void TSyncQueueSinkParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("column_filter", &TThis::ColumnFilter)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicSyncQueueSinkParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TAsyncQueueWriterParametersBase::Register(TRegistrar registrar)
{
    registrar.Parameter("producer_path", &TThis::ProducerPath)
        .AddOption(EYTPathOwnership::SharedWrite);
    registrar.Parameter("require_sync_replica", &TThis::RequireSyncReplica)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TAsyncQueueWriterParameters::Register(TRegistrar registrar)
{
    registrar.Postprocessor([] (TAsyncQueueWriterParameters* parameters) {
        THROW_ERROR_EXCEPTION_IF(!parameters->QueuePath.GetCluster().has_value(), "QueuePath should have cluster");
        THROW_ERROR_EXCEPTION_IF(!parameters->ProducerPath.GetCluster().has_value(), "ProducerPath should have cluster");
        THROW_ERROR_EXCEPTION_IF(parameters->QueuePath.GetCluster() != parameters->ProducerPath.GetCluster(),
            "ProducerPath and QueuePath should have the same cluster")
    });
}

////////////////////////////////////////////////////////////////////////////////

void TAsyncMultiClusterQueueWriterParameters::Register(TRegistrar registrar)
{
    // Force manual use clusters until merge TAsyncQueueSink with TAsyncMultiClusterQueueSink.
    registrar.Parameter("use_clusters", &TThis::UseClusters)
        .Default(false);

    registrar.Postprocessor([] (TAsyncMultiClusterQueueWriterParameters* parameters) {
        if (parameters->UseClusters) {
            THashSet<std::string> queueClusters;
            THashSet<std::string> producerClusters;
            if (parameters->QueuePath.GetCluster().has_value()) {
                queueClusters.insert(*parameters->QueuePath.GetCluster());
            }
            if (parameters->QueuePath.GetClusters().has_value()) {
                auto clusters = parameters->QueuePath.GetClusters();
                queueClusters.insert(std::make_move_iterator(clusters->begin()), std::make_move_iterator(clusters->end()));
            }
            if (parameters->ProducerPath.GetCluster().has_value()) {
                producerClusters.insert(*parameters->ProducerPath.GetCluster());
            }
            if (parameters->ProducerPath.GetClusters().has_value()) {
                auto clusters = parameters->ProducerPath.GetClusters();
                producerClusters.insert(std::make_move_iterator(clusters->begin()), std::make_move_iterator(clusters->end()));
            }
            THROW_ERROR_EXCEPTION_IF(queueClusters.empty(), "QueuePath should have cluster or clusters");
            THROW_ERROR_EXCEPTION_IF(producerClusters.empty(), "ProducerPath should have cluster or clusters");
            THROW_ERROR_EXCEPTION_IF(queueClusters != producerClusters, "QueuePath and ProducerPath should have same set of clusters");
        } else {
            THROW_ERROR_EXCEPTION_IF(!parameters->QueuePath.GetCluster().has_value(), "QueuePath should have cluster");
            THROW_ERROR_EXCEPTION_IF(!parameters->ProducerPath.GetCluster().has_value(), "ProducerPath should have cluster");
            THROW_ERROR_EXCEPTION_IF(parameters->QueuePath.GetCluster() != parameters->ProducerPath.GetCluster(),
                "ProducerPath and QueuePath should have the same cluster")
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicAsyncQueueWriterParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("write_period", &TThis::WritePeriod)
        .Default(TDuration::MilliSeconds(100));
    registrar.Parameter("max_rows_per_write", &TThis::MaxRowsPerWrite)
        .Default(1'000);
    registrar.Parameter("max_bytes_per_write", &TThis::MaxBytesPerWrite)
        .Default(1_MB);
    registrar.Parameter("backoff_duration", &TThis::BackoffDuration)
        .Default(TDuration::Seconds(1));
}

////////////////////////////////////////////////////////////////////////////////

void TAsyncQueueSinkParametersBase::Register(TRegistrar registrar)
{
    registrar.Parameter("column_filter", &TThis::ColumnFilter)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TAsyncQueueSinkParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TAsyncMultiClusterQueueSinkParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TDynamicAsyncQueueSinkParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TQueueSinkControllerParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TDynamicQueueSinkControllerParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
