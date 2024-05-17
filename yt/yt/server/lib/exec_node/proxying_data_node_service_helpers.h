#pragma once

#include "public.h"

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/chunk_client/block.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

using TRefCountedChunkSpec = TRefCountedProto<NChunkClient::NProto::TChunkSpec>;
DECLARE_REFCOUNTED_TYPE(TRefCountedChunkSpec)

////////////////////////////////////////////////////////////////////////////////

THashMap<NChunkClient::TChunkId, TRefCountedChunkSpecPtr> ModifyChunkSpecForJobInputCache(
    NNodeTrackerClient::TNodeId nodeId,
    EJobType jobType,
    NControllerAgent::NProto::TJobSpecExt* jobSpecExt);

void PatchProxiedChunkSpecs(NControllerAgent::NProto::TJobSpec* jobSpecProto);

////////////////////////////////////////////////////////////////////////////////

} // namespace NY::NExecNode
