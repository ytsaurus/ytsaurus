#include "spec.h"

#include <yt/yt/flow/library/cpp/common/yt_path_option.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

void ValidateSingleCluster(const TAsyncQueueWriterParametersBase& parameters)
{
    THROW_ERROR_EXCEPTION_IF(!parameters.QueuePath.GetCluster(), "QueuePath should have cluster");
    THROW_ERROR_EXCEPTION_IF(!parameters.ProducerPath.GetCluster(), "ProducerPath should have cluster");
    THROW_ERROR_EXCEPTION_IF(parameters.QueuePath.GetCluster() != parameters.ProducerPath.GetCluster(),
        "ProducerPath and QueuePath should have the same cluster");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TQueueInfoSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("queue_path", &TThis::QueuePath)
        .AddOption(EYTPathOwnership::ReadOnly);

    registrar.Parameter("update_partition_count_period", &TThis::UpdatePartitionCountPeriod)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("update_partition_count_retry_min_backoff", &TThis::UpdatePartitionCountRetryMinBackoff)
        .GreaterThan(TDuration::Zero())
        .Default(TDuration::Seconds(1));
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

void TQueueSinkTabletRoutingParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("tablet_index_expression", &TThis::TabletIndexExpression)
        .Default();
    registrar.Parameter("tablet_index_routing_hash_expression", &TThis::TabletIndexRoutingHashExpression)
        .Default();
    registrar.Parameter("tablet_index_routing_hash_policy", &TThis::TabletIndexRoutingHashPolicy)
        .Default();
    registrar.Parameter("tablet_count", &TThis::TabletCount)
        .Default();

    registrar.Postprocessor([] (TQueueSinkTabletRoutingParameters* parameters) {
        THROW_ERROR_EXCEPTION_IF(
            parameters->TabletIndexExpression && parameters->TabletIndexRoutingHashExpression,
            "At most one of \"tablet_index_expression\" and \"tablet_index_routing_hash_expression\" may be set");

        if (parameters->TabletIndexExpression) {
            THROW_ERROR_EXCEPTION_IF(
                parameters->TabletIndexRoutingHashPolicy || parameters->TabletCount,
                "\"tablet_index_routing_hash_policy\" and \"tablet_count\" are not applicable with "
                "\"tablet_index_expression\"");
        } else if (parameters->TabletIndexRoutingHashExpression) {
            THROW_ERROR_EXCEPTION_UNLESS(
                parameters->TabletIndexRoutingHashPolicy.has_value(),
                "\"tablet_index_routing_hash_policy\" is required with \"tablet_index_routing_hash_expression\"");
            THROW_ERROR_EXCEPTION_IF(
                parameters->TabletCount.has_value() && *parameters->TabletCount <= 0,
                "\"tablet_count\" must be positive, got %v",
                *parameters->TabletCount);
        } else {
            THROW_ERROR_EXCEPTION_IF(
                parameters->TabletIndexRoutingHashPolicy || parameters->TabletCount,
                "\"tablet_index_routing_hash_policy\" and \"tablet_count\" require one of "
                "\"tablet_index_expression\" / \"tablet_index_routing_hash_expression\" to be set");
        }
    });
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
        ValidateSingleCluster(*parameters);
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
        .Default(TDuration::Seconds(1))
        .GreaterThan(TDuration::Zero());
}

////////////////////////////////////////////////////////////////////////////////

void ValidateAsyncSinkTabletRoutingUnsupported(const TQueueSinkTabletRoutingParameters& parameters)
{
    THROW_ERROR_EXCEPTION_IF(
        parameters.TabletIndexExpression || parameters.TabletIndexRoutingHashExpression ||
            parameters.TabletIndexRoutingHashPolicy || parameters.TabletCount,
        "Tablet routing is not supported on async queue sinks yet (tracked by YTFLOW-766); "
        "use a sync queue sink");
}

void TAtMostOnceStrategyParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("enabled", &TThis::Enabled)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TAtMostOnceStrategyDynamicParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("suspend_destruction_duration", &TThis::SuspendDestructionDuration)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("total_queue_bytes_limit", &TThis::TotalQueueBytesLimit)
        .Default(NYTree::TSize(100_MB));
}

////////////////////////////////////////////////////////////////////////////////

void TAsyncQueueSinkParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("column_filter", &TThis::ColumnFilter)
        .Default();
    registrar.Parameter("at_most_once_strategy", &TThis::AtMostOnceStrategy)
        .Optional();

    registrar.Postprocessor([] (TThis* parameters) {
        ValidateAsyncSinkTabletRoutingUnsupported(*parameters);
        parameters->ValidateClusters();
    });
}

////////////////////////////////////////////////////////////////////////////////

void TAsyncQueueSinkParameters::ValidateClusters() const
{
    ValidateSingleCluster(*this);
}

////////////////////////////////////////////////////////////////////////////////

void TAsyncMultiClusterQueueSinkParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("use_clusters", &TThis::UseClusters)
        .Default(false);
}

void TAsyncMultiClusterQueueSinkParameters::ValidateClusters() const
{
    auto getClusters = [] (const NYPath::TRichYPath& path) {
        THashSet<std::string> result;
        if (auto cluster = path.GetCluster()) {
            result.insert(*cluster);
        }
        if (auto clusters = path.GetClusters()) {
            for (const auto& cluster : *clusters) {
                THROW_ERROR_EXCEPTION_UNLESS(result.insert(cluster).second,
                    "Duplicate cluster %Qv in path %v",
                    cluster,
                    path);
            }
        }
        return result;
    };
    auto queueClusters = getClusters(QueuePath);
    auto producerClusters = getClusters(ProducerPath);
    if (UseClusters) {
        THROW_ERROR_EXCEPTION_IF(queueClusters.empty(), "QueuePath should have cluster or clusters");
        THROW_ERROR_EXCEPTION_IF(producerClusters.empty(), "ProducerPath should have cluster or clusters");
        THROW_ERROR_EXCEPTION_IF(queueClusters != producerClusters, "QueuePath and ProducerPath should have same set of clusters");
    } else {
        ValidateSingleCluster(*this);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicAsyncQueueSinkParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("at_most_once_strategy", &TThis::AtMostOnceStrategy)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TQueueSinkControllerParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TDynamicQueueSinkControllerParameters::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
