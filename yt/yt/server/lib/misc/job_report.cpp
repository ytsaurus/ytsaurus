#include "job_report.h"

#include <yt/yt/server/lib/job_agent/estimate_size_helpers.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/misc/statistics.h>

namespace NYT {

using namespace NYTree;
using namespace NYson;
using namespace NCoreDump;

////////////////////////////////////////////////////////////////////////////////

TJobEvent::TJobEvent(NJobTrackerClient::EJobState state)
    : Timestamp_(Now())
    , State_(state)
{ }

TJobEvent::TJobEvent(NExecNode::EJobPhase phase)
    : Timestamp_(Now())
    , Phase_(phase)
{ }

TJobEvent::TJobEvent(NJobTrackerClient::EJobState state, NExecNode::EJobPhase phase)
    : Timestamp_(Now())
    , State_(state)
    , Phase_(phase)
{ }

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TJobEvents& events, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginList()
        .DoFor(events, [] (TFluentList fluent, const TJobEvent& event) {
            fluent.Item().BeginMap()
                .Item("time").Value(event.Timestamp())
                .OptionalItem("state", event.State())
                .OptionalItem("phase", event.Phase())
            .EndMap();
        })
        .EndList();
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TJobInterruptionInfo& interruptionInfo, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("interruption_reason").Value(interruptionInfo.InterruptionReason)
            .OptionalItem("interruption_timeout", interruptionInfo.InterruptionTimeout)
            .OptionalItem("preemption_reason", interruptionInfo.PreemptionReason)
            .DoIf(interruptionInfo.PreemptedFor.has_value(), [&interruptionInfo] (TFluentMap fluent) {
                fluent.Item("preempted_for").BeginMap()
                    .Item("allocation_id").Value(interruptionInfo.PreemptedFor->AllocationId)
                    .Item("operation_id").Value(interruptionInfo.PreemptedFor->OperationId)
                .EndMap();
            })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

size_t TJobReport::EstimateSize() const
{
    return NJobAgent::EstimateSizes(
        OperationId_.Underlying(),
        JobId_.Underlying(),
        Type_,
        State_,
        StartTime_,
        FinishTime_,
        Error_,
        Spec_,
        SpecVersion_,
        Statistics_,
        Events_,
        InterruptionInfo_);
}

TJobReport TJobReport::ExtractSpec() const
{
    TJobReport copy;
    copy.JobId_ = JobId_;
    copy.Spec_ = Spec_;
    copy.SpecVersion_ = SpecVersion_;
    copy.Type_ = Type_;
    return copy;
}

TJobReport TJobReport::ExtractStderr() const
{
    TJobReport copy;
    copy.JobId_ = JobId_;
    copy.OperationId_ = OperationId_;
    copy.Stderr_ = Stderr_;
    return copy;
}

TJobReport TJobReport::ExtractFailContext() const
{
    TJobReport copy;
    copy.JobId_ = JobId_;
    copy.OperationId_ = OperationId_;
    copy.FailContext_ = FailContext_;
    return copy;
}

TJobReport TJobReport::ExtractIds() const
{
    TJobReport copy;
    copy.JobId_ = JobId_;
    copy.OperationId_ = OperationId_;
    return copy;
}

TJobReport TJobReport::ExtractProfile() const
{
    TJobReport copy;
    copy.JobId_ = JobId_;
    copy.OperationId_ = OperationId_;
    copy.Profile_ = Profile_;
    return copy;
}

bool TJobReport::IsEmpty() const
{
    bool somethingSpecified =
        Type_ || State_ || StartTime_ || FinishTime_ || Error_ || InterruptionInfo_ || Spec_ || SpecVersion_ ||
        Statistics_ || Events_ || Stderr_ || StderrSize_ || FailContext_ || Profile_ || JobCookie_ ||
        CoreInfos_ || HasCompetitors_ || HasProbingCompetitors_ || MonitoringDescriptor_ || ExecAttributes_ || ControllerState_;
    return !somethingSpecified;
}

////////////////////////////////////////////////////////////////////////////////

void TGpuDevice::Register(TRegistrar registrar)
{
    registrar.Parameter("device_number", &TThis::DeviceNumber)
        .Default();
    registrar.Parameter("device_name", &TThis::DeviceName)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TExecAttributes::Register(TRegistrar registrar)
{
    registrar.Parameter("slot_index", &TThis::SlotIndex)
        .Default(-1);
    registrar.Parameter("ip_addresses", &TThis::IPAddresses)
        .Default();
    registrar.Parameter("sandbox_path", &TThis::SandboxPath)
        .Default();
    registrar.Parameter("medium_name", &TThis::MediumName)
        .Default();
    registrar.Parameter("job_proxy_socket_path", &TThis::JobProxySocketPath)
        .Default();
    registrar.Parameter("gpu_devices", &TThis::GpuDevices)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
