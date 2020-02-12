#include "helpers.h"

#include <yt/ytlib/api/native/client.h>

#include <yt/client/object_client/helpers.h>

#include <yt/client/security_client/acl.h>

#include <yt/library/re2/re2.h>

namespace NYT::NScheduler {

using namespace NSecurityClient;
using namespace NObjectClient;
using namespace NNodeTrackerClient;
using namespace NConcurrency;
using namespace NYTree;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

const int PoolNameMaxLength = 100;
const char* PoolNameRegex = "[A-Za-z0-9-_]+";

TError CheckPoolName(const TString& poolName)
{
    if (poolName == NScheduler::RootPoolName) {
        return TError("Pool name cannot be equal to root pool name")
            << TErrorAttribute("RootPoolName", RootPoolName);
    }

    if (poolName.length() > PoolNameMaxLength) {
        return TError("Pool name is too long")
            << TErrorAttribute("length", poolName.length())
            << TErrorAttribute("max_length", PoolNameMaxLength);
    }

    static NRe2::TRe2Ptr regex = New<NRe2::TRe2>(PoolNameRegex);
    if (!NRe2::TRe2::FullMatch(NRe2::StringPiece(poolName), *regex)) {
        return TError("Name must match regular expression %Qv", PoolNameRegex);
    }

    return TError();
}

void ValidatePoolName(const TString& poolName)
{
    CheckPoolName(poolName).ThrowOnError();
}

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

} // namespace NYT::NScheduler
