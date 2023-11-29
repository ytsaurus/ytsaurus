#pragma once

#include <library/cpp/yt/misc/guid.h>

#include <yt/yt/client/scheduler/public.h>

#include <yt/yt/client/job_tracker_client/public.h>

#include <yt/yt/core/misc/common.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TImmutableChunkMeta;
using TImmutableChunkMetaPtr = std::unique_ptr<TImmutableChunkMeta>;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_STRONG_TYPEDEF(TJobId, TGuid);

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TJobSpec;
class TReqHeartbeat;
class TRspHeartbeat;
class TJobResult;
class TJobStatus;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
