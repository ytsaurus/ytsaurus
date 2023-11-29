#pragma once

#include "public.h"

#include <yt/yt/client/scheduler/public.h>

#include <yt/yt/server/lib/chunk_server/proto/job_tracker_service.pb.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TJobToAbort
{
    TJobId JobId;
};

struct TJobToRemove
{
    TJobId JobId;
};

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(NProto::TJobToAbort* protoJobToAbort, const NChunkServer::TJobToAbort& jobToAbort);

void FromProto(NChunkServer::TJobToAbort* jobToAbort, const NProto::TJobToAbort& protoJobToAbort);

void ToProto(NProto::TJobToRemove* protoJob, const NChunkServer::TJobToRemove& jobToRemove);

void FromProto(NChunkServer::TJobToRemove* jobToRemove, const NProto::TJobToRemove& protoJobToRemove);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

void AddJobToAbort(NProto::TRspHeartbeat* response, const TJobToAbort& jobToAbort);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
