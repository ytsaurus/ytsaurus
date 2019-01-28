#pragma once

#include "public.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/core/ytree/public.h>

#include <yt/core/logging/public.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

TJobId GenerateJobId(NObjectClient::TCellTag tag, NNodeTrackerClient::TNodeId nodeId);
NNodeTrackerClient::TNodeId NodeIdFromJobId(TJobId jobId);

////////////////////////////////////////////////////////////////////////////////


void ValidateOperationAccess(
    const TString& user,
    TOperationId operationId,
    EAccessType accessType,
    const NYTree::INodePtr& acl,
    const NApi::NNative::IClientPtr& client,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
