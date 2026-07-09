#include "computation.h"

#include "flow_view.h"
#include "message.h"
#include "spec.h"

namespace NYT::NFlow {

using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

IClientPtr TComputationContextBase::GetClient() const
{
    THROW_ERROR_EXCEPTION_UNLESS(PipelinePath.GetCluster().has_value(), "Pipeline path must have cluster");
    return ClientsCache->GetClient(*PipelinePath.GetCluster());
}

IResourcePtr TComputationContextBase::GetStaticResource(const char resourceId[])
{
    return GetStaticResource(TResourceId(resourceId));
}

IResourcePtr TComputationContextBase::GetStaticResource(const TResourceId& resourceId)
{
    auto resource = GetOrDefault(StaticResources, resourceId);
    if (!resource) {
        THROW_ERROR_EXCEPTION("Static resource is not found")
            << TErrorAttribute("resource_id", resourceId);
    }
    return resource;
}

////////////////////////////////////////////////////////////////////////////////

void IComputationRunContext::MarkPersisted(const std::vector<TInputMessageConstPtr>& messages)
{
    std::vector<TMessageId> messageIds;
    messageIds.reserve(messages.size());
    for (const auto& message : messages) {
        messageIds.push_back(message->MessageId);
    }
    MarkPersisted(std::span<const TMessageId>(messageIds));
}

void IComputationRunContext::MarkPersisted(TMessageId messageId)
{
    MarkPersisted(std::span<const TMessageId>(&messageId, 1));
}

////////////////////////////////////////////////////////////////////////////////

void TComputationOrchidState::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TComputationStatus::Register(TRegistrar registrar)
{
    registrar.Parameter("node_traverse", &TThis::NodeTraverse)
        .Default();
    registrar.Parameter("partition_status", &TThis::PartitionStatus)
        .Default();
    registrar.Parameter("epoch_part_times", &TThis::EpochPartTimes)
        .Default();
    registrar.Parameter("input_limits", &TThis::InputLimits)
        .Default();
    registrar.Parameter("output_limits", &TThis::OutputLimits)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void IComputation::TParametersBase::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void IComputation::TDynamicParametersBase::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void IComputation::TDynamicPartitionSpecBase::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
