#include "options.h"

#include "helpers.h"
#include "spec.h"
#include "state.h"

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TYdbTopologyOptions MakeYdbTopologyOptions(const TYdbTopicSpec& spec)
{
    return {
        .DiscoveryEntries = {{.Endpoint = spec.Endpoint, .Database = spec.Database}},
        .Database = spec.Database,
        .Topic = spec.Topic,
        .ClientTimeout = spec.ClientControllerTimeout,
        .UpdatePartitionCountPeriod = spec.UpdatePartitionCountPeriod,
    };
}

std::shared_ptr<const TYdbSourceOptions> MakeYdbSourceOptions(
    const TYdbSourceParameters& parameters,
    TYdbPayloadSplitter splitPayload)
{
    return std::make_shared<const TYdbSourceOptions>(TYdbSourceOptions{
        .Topology = MakeYdbTopologyOptions(parameters),
        .Consumer = parameters.Consumer,
        .ReceivePartitionEnd = true,
        .MaxMemoryUsageBytes = parameters.MaxMemoryUsageBytes,
        .StartSessionTimeout = parameters.StartSessionTimeout,
        .UpdateInfoPeriod = parameters.UpdateInfoPeriod,
        .TryParseFlowQueueMeta = parameters.TryParseFlowQueueMeta,
        .FlowQueueMetaName = parameters.FlowQueueMetaName,
        .IgnoreMalformedFlowQueueMeta = parameters.IgnoreMalformedFlowQueueMeta,
        .AttributesColumn = parameters.AttributesColumn,
        .PartitionFilter = parameters.PartitionFilter,
        .SplitPayload = std::move(splitPayload),
        .SerializeUnparsed = BIND(&SerializeUnparsedYdbPayload),
    });
}

std::shared_ptr<const TYdbSourceDynamicOptions> MakeYdbSourceDynamicOptions(
    const TDynamicYdbSourceParameters& parameters)
{
    return std::make_shared<const TYdbSourceDynamicOptions>(TYdbSourceDynamicOptions{
        .MalformedMessagePolicy = parameters.MalformedMessagePolicy,
        .ReadFromInstant = parameters.ReadFromInstant,
        .MaxTimeLag = parameters.MaxTimeLag,
        .CommitTimeout = parameters.CommitTimeout,
        .SessionSilenceTimeout = parameters.SessionSilenceTimeout,
        .RestartToken = NYTree::ConvertToNode(parameters),
    });
}

std::shared_ptr<const TYdbSinkOptions> MakeYdbSinkOptions(
    const TCommonYdbSinkParameters& parameters,
    std::optional<EFramingFormat> framingFormat)
{
    return std::make_shared<const TYdbSinkOptions>(TYdbSinkOptions{
        .Topology = MakeYdbTopologyOptions(parameters),
        .Endpoint = parameters.Endpoint,
        .DisplayEndpoint = parameters.Endpoint,
        .PayloadColumn = parameters.PayloadColumn,
        .AttributesColumn = parameters.AttributesColumn,
        .MaxMemoryUsageBytes = parameters.MaxMemoryUsageBytes,
        .Codec = parameters.Codec,
        .CompressionLevel = parameters.CompressionLevel,
        .WriteFlowQueueMeta = parameters.WriteFlowQueueMeta,
        .FlowQueueMetaName = parameters.FlowQueueMetaName,
        .FramingFormat = framingFormat,
    });
}

std::shared_ptr<const TYdbBatchingOptions> MakeYdbBatchingOptions(i64 maxRowsPerBatch, i64 maxBytesPerBatch)
{
    return std::make_shared<const TYdbBatchingOptions>(TYdbBatchingOptions{
        .MaxRowsPerBatch = maxRowsPerBatch,
        .MaxBytesPerBatch = maxBytesPerBatch,
    });
}

std::shared_ptr<const TYdbSinkControllerDynamicOptions> MakeYdbSinkControllerDynamicOptions(
    const TDynamicCommonYdbSinkParameters& parameters)
{
    return std::make_shared<const TYdbSinkControllerDynamicOptions>(TYdbSinkControllerDynamicOptions{
        .FlowQueueMetaHeartbeatPeriod = parameters.FlowQueueMetaHeartbeatPeriod,
    });
}

std::shared_ptr<const TYdbTwoPhaseCommitOptions> MakeYdbTwoPhaseCommitOptions(
    const TYdbTwoPhaseCommitSinkParameters& parameters)
{
    return std::make_shared<const TYdbTwoPhaseCommitOptions>(TYdbTwoPhaseCommitOptions{
        .Sink = *MakeYdbSinkOptions(parameters, parameters.FramingFormat),
        .WriterIdentityType = YdbTwoPhaseCommitWriterIdentityType,
    });
}

std::shared_ptr<const TYdbTwoPhaseCommitDynamicOptions> MakeYdbTwoPhaseCommitDynamicOptions(
    const TDynamicYdbTwoPhaseCommitSinkParameters& parameters)
{
    return std::make_shared<const TYdbTwoPhaseCommitDynamicOptions>(TYdbTwoPhaseCommitDynamicOptions{
        .Batching = *MakeYdbBatchingOptions(parameters.MaxRowsPerBatch, parameters.MaxBytesPerBatch),
        .PreparationAttemptTimeout = parameters.PreparationAttemptTimeout,
        .PreparationTargetBackoff = parameters.PreparationTargetBackoff,
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
