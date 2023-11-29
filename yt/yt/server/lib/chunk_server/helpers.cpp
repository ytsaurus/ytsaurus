#include "helpers.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(NProto::TJobToAbort* protoJobToAbort, const NChunkServer::TJobToAbort& jobToAbort)
{
    ToProto(protoJobToAbort->mutable_job_id(), jobToAbort.JobId);
}

void FromProto(NChunkServer::TJobToAbort* jobToAbort, const NProto::TJobToAbort& protoJobToAbort)
{
    FromProto(&jobToAbort->JobId, protoJobToAbort.job_id());
}

void ToProto(NProto::TJobToRemove* protoJobToRemove, const NChunkServer::TJobToRemove& jobToRemove)
{
    ToProto(protoJobToRemove->mutable_job_id(), jobToRemove.JobId);
}

void FromProto(NChunkServer::TJobToRemove* jobToRemove, const NProto::TJobToRemove& protoJobToRemove)
{
    jobToRemove->JobId = NYT::FromProto<TJobId>(protoJobToRemove.job_id());
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

void AddJobToAbort(NProto::TRspHeartbeat* response, const TJobToAbort& jobToAbort)
{
    ToProto(response->add_jobs_to_abort(), jobToAbort);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
