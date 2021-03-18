#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/logging/public.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

TJobId GenerateJobId(NObjectClient::TCellTag tag, NNodeTrackerClient::TNodeId nodeId);
NNodeTrackerClient::TNodeId NodeIdFromJobId(TJobId jobId);

////////////////////////////////////////////////////////////////////////////////

NSecurityClient::TSerializableAccessControlList MakeOperationArtifactAcl(const NSecurityClient::TSerializableAccessControlList& acl);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
