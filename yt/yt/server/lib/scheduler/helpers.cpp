#include "helpers.h"

#include "config.h"

#include <yt/yt/ytlib/api/native/client.h>

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

TJobId GenerateJobId(TCellTag tag, TNodeId nodeId)
{
    return MakeId(
        EObjectType::SchedulerJob,
        tag,
        RandomNumber<ui64>(),
        nodeId);
}

TNodeId NodeIdFromJobId(TJobId jobId)
{
    return jobId.Parts32[0];
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

} // namespace NYT::NScheduler
