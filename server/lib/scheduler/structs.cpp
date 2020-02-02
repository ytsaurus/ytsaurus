#include "structs.h"

#include <yt/core/ytree/fluent.h>

namespace NYT::NScheduler {

using namespace NControllerAgent;

////////////////////////////////////////////////////////////////////////////////

TJobStartDescriptor::TJobStartDescriptor(
    TJobId id,
    EJobType type,
    const TJobResourcesWithQuota& resourceLimits,
    bool interruptible)
    : Id(id)
    , Type(type)
    , ResourceLimits(resourceLimits)
    , Interruptible(interruptible)
{ }

////////////////////////////////////////////////////////////////////////////////

void TControllerScheduleJobResult::RecordFail(EScheduleJobFailReason reason)
{
    ++Failed[reason];
}

bool TControllerScheduleJobResult::IsBackoffNeeded() const
{
    return
        !StartDescriptor &&
        Failed[EScheduleJobFailReason::NotEnoughResources] == 0 &&
        Failed[EScheduleJobFailReason::NoLocalJobs] == 0 &&
        Failed[EScheduleJobFailReason::NodeBanned] == 0 &&
        Failed[EScheduleJobFailReason::DataBalancingViolation] == 0;
}

bool TControllerScheduleJobResult::IsScheduleStopNeeded() const
{
    return
        Failed[EScheduleJobFailReason::NotEnoughChunkLists] > 0 ||
        Failed[EScheduleJobFailReason::JobSpecThrottling] > 0;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NControllerAgent::NProto::TPoolTreeControllerSettingsMap* protoPoolTreeControllerSettingsMap,
    const TPoolTreeControllerSettingsMap& poolTreeControllerSettingsMap)
{
    for (const auto& [treeName, settings] : poolTreeControllerSettingsMap) {
        auto* protoTreeSettings = protoPoolTreeControllerSettingsMap->add_tree_settings();
        protoTreeSettings->set_tree_name(treeName);
        ToProto(protoTreeSettings->mutable_scheduling_tag_filter(), settings.SchedulingTagFilter);
        protoTreeSettings->set_tentative(settings.Tentative);
    }
}

void FromProto(
    TPoolTreeControllerSettingsMap* poolTreeControllerSettingsMap,
    const NControllerAgent::NProto::TPoolTreeControllerSettingsMap& protoPoolTreeControllerSettingsMap)
{
    for (const auto& protoTreeSettings : protoPoolTreeControllerSettingsMap.tree_settings()) {
        TSchedulingTagFilter filter;
        FromProto(&filter, protoTreeSettings.scheduling_tag_filter());
        poolTreeControllerSettingsMap->emplace(
            protoTreeSettings.tree_name(),
            TPoolTreeControllerSettings{
                .SchedulingTagFilter = filter,
                .Tentative = protoTreeSettings.tentative()
            });
    }
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NScheduler::NProto::TSchedulerToAgentJobEvent::TPreemptedFor* proto, const TPreemptedFor& preemptedFor)
{
    ToProto(proto->mutable_job_id(), preemptedFor.JobId);
    ToProto(proto->mutable_operation_id(), preemptedFor.OperationId);
}

void FromProto(TPreemptedFor* preemptedFor, const NScheduler::NProto::TSchedulerToAgentJobEvent::TPreemptedFor& proto)
{
    FromProto(&preemptedFor->JobId, proto.job_id());
    FromProto(&preemptedFor->OperationId, proto.operation_id());
}

void Serialize(const TPreemptedFor& preemptedFor, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("job_id").Value(preemptedFor.JobId)
            .Item("operation_id").Value(preemptedFor.OperationId)
        .EndMap();
}

void Deserialize(TPreemptedFor& preemptedFor, const NYTree::INodePtr& node)
{
    Deserialize(preemptedFor.JobId, node->AsMap()->GetChild("job_id"));
    Deserialize(preemptedFor.OperationId, node->AsMap()->GetChild("operation_id"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
