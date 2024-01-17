#include "helpers.h"

#include "config.h"

#include <yt/yt/server/lib/scheduler/proto/allocation_tracker_service.pb.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/security_client/acl.h>

namespace NYT::NScheduler {

using namespace NSecurityClient;
using namespace NObjectClient;
using namespace NNodeTrackerClient;
using namespace NConcurrency;
using namespace NYTree;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

TAllocationId GenerateAllocationId(TCellTag tag, TNodeId nodeId)
{
    return TAllocationId(MakeId(
        EObjectType::SchedulerJob,
        tag,
        RandomNumber<ui64>(),
        nodeId.Underlying()));
}

TNodeId NodeIdFromAllocationId(TAllocationId allocationId)
{
    return TNodeId(allocationId.Underlying().Parts32[0]);
}

////////////////////////////////////////////////////////////////////////////////

TSerializableAccessControlList MakeOperationArtifactAcl(const TSerializableAccessControlList& acl)
{
    TSerializableAccessControlList result;
    for (auto ace : acl.Entries) {
        if (Any(ace.Permissions & EPermission::Read)) {
            ace.Permissions = EPermission::Read;
            result.Entries.push_back(std::move(ace));
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

void ValidateInfinibandClusterName(const TString& name)
{
    if (name.empty()) {
        THROW_ERROR_EXCEPTION("Infiniband cluster name cannot be empty");
    }
}

////////////////////////////////////////////////////////////////////////////////

void Delay(TDuration delay, EDelayType delayType)
{
    switch (delayType) {
        case EDelayType::Async:
            TDelayedExecutor::WaitForDuration(delay);
            break;
        case EDelayType::Sync:
            Sleep(delay);
            break;
        default:
            YT_ABORT();
    }
}

void MaybeDelay(const TDelayConfigPtr& delayConfig)
{
    if (delayConfig) {
        Delay(delayConfig->Duration, delayConfig->Type);
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(NProto::NNode::TAllocationToAbort* protoAllocationToAbort, const TAllocationToAbort& allocationToAbort)
{
    ToProto(protoAllocationToAbort->mutable_allocation_id(), allocationToAbort.AllocationId);
    if (allocationToAbort.AbortReason) {
        protoAllocationToAbort->set_abort_reason(NYT::ToProto<int>(*allocationToAbort.AbortReason));
    }
}

void FromProto(TAllocationToAbort* allocationToAbort, const NProto::NNode::TAllocationToAbort& protoAllocationToAbort)
{
    FromProto(&allocationToAbort->AllocationId, protoAllocationToAbort.allocation_id());
    if (protoAllocationToAbort.has_abort_reason()) {
        allocationToAbort->AbortReason = NYT::FromProto<NScheduler::EAbortReason>(protoAllocationToAbort.abort_reason());
    }
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
