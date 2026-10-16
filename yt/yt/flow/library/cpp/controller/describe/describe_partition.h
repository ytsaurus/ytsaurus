#pragma once

#include "common.h"

namespace NYT::NFlow::NDescribe {

////////////////////////////////////////////////////////////////////////////////

struct TExtendedPartitionDescription
    : public TPartitionDescription
{
    // Retryable errors, HeavyHitters, Partition, PartitionEphemeralState, PartitionJobStatus, .
    std::vector<TMessage> Messages;
    // TODO: JobOrchid.

    std::string TracingAddress;

    REGISTER_YSON_STRUCT_LITE(TExtendedPartitionDescription);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

TExtendedPartitionDescription DescribePartition(
    const TFlowViewPtr& flowView,
    const TPartitionId& partitionId,
    const TErrorOr<NYson::TYsonString>& jobOrchid);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe
