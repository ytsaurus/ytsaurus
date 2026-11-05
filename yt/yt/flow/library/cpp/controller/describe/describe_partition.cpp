#include "describe_partition.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/internal_urls.h>

namespace NYT::NFlow::NDescribe {

using namespace NLogging;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

constexpr auto& DefaultTracingAddress = NInternalUrls::TracingAddress;

void TExtendedPartitionDescription::Register(TRegistrar registrar)
{
    registrar.Parameter("messages", &TThis::Messages)
        .Default();
    registrar.Parameter("tracing_address", &TThis::TracingAddress)
        .Default(std::string(DefaultTracingAddress));
}

////////////////////////////////////////////////////////////////////////////////

TExtendedPartitionDescription DescribePartition(
    const TFlowViewPtr& flowView,
    const TPartitionId& partitionId,
    const TErrorOr<TYsonString>& jobOrchid)
{
    TExtendedPartitionDescription description;
    FillPartitionDescription(GetPartitionIntermediateDescription(flowView, partitionId), description, description.Messages);

    {
        auto& message = description.Messages.emplace_back();
        message.Level = ELogLevel::Info;
        message.Text = "Flow view partition";
        message.Yson = ConvertToYsonString(flowView->State->ExecutionSpec->Layout->Partitions.at(partitionId));
    }

    {
        auto it = flowView->Feedback->PartitionJobStatuses.find(partitionId);
        if (it != flowView->Feedback->PartitionJobStatuses.end()) {
            auto& message = description.Messages.emplace_back();
            message.Level = ELogLevel::Info;
            message.Text = "Flow view partition job status (includes partition status)";
            message.Yson = ConvertToYsonString(it->second);
        }
    }

    {
        auto it = flowView->EphemeralState->Partitions.find(partitionId);
        if (it != flowView->EphemeralState->Partitions.end()) {
            auto& message = description.Messages.emplace_back();
            message.Level = ELogLevel::Info;
            message.Text = "Flow view partition ephemeral state (includes dynamic partition spec)";
            message.Yson = ConvertToYsonString(it->second);
        }
    }

    {
        auto& message = description.Messages.emplace_back();
        message.Text = "Job orchid";
        if (jobOrchid.IsOK()) {
            message.Level = ELogLevel::Info;
            message.Yson = ConvertToYsonString(jobOrchid.Value());
        } else {
            message.Level = ELogLevel::Warning;
            message.Error = jobOrchid;
        }
    }

    return description;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe
