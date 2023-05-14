#pragma once

#include "public.h"

#include <yt/yt/server/lib/controller_agent/proto/job_tracker_service.pb.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/tracing/public.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

TAllocationId AllocationIdFromJobId(TJobId jobId);

////////////////////////////////////////////////////////////////////////////////

TString JobTypeAsKey(EJobType jobType);

////////////////////////////////////////////////////////////////////////////////

struct TReleaseJobFlags
{
    bool ArchiveJobSpec = false;
    bool ArchiveStderr = false;
    bool ArchiveFailContext = false;
    bool ArchiveProfile = false;

    bool IsNonTrivial() const;
    bool IsTrivial() const;

    void Persist(const TStreamPersistenceContext& context);
};

struct TJobToRelease
{
    TJobId JobId;
    TReleaseJobFlags ReleaseFlags = {};
};

TString ToString(const TReleaseJobFlags& releaseFlags);

struct TJobToAbort
{
    TJobId JobId;
    NScheduler::EAbortReason AbortReason;
};

struct TJobToStore
{
    TJobId JobId;
};

struct TJobToConfirm
{
    TJobId JobId;
};

////////////////////////////////////////////////////////////////////////////////

NTableClient::TTableSchemaPtr RenameColumnsInSchema(
    TStringBuf name,
    const NTableClient::TTableSchemaPtr& schema,
    bool isDynamic,
    const NTableClient::TColumnRenameDescriptors& renameDescriptors,
    bool changeStableName);

////////////////////////////////////////////////////////////////////////////////

void PackBaggageFromJobSpec(
    const NTracing::TTraceContextPtr& traceContext,
    const NProto::TJobSpec& jobSpec,
    TOperationId operationId,
    TJobId jobId);

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(
    NProto::TJobToRemove* protoJobToRemove,
    const NControllerAgent::TJobToRelease& jobToRelease);

void FromProto(
    NControllerAgent::TJobToRelease* jobToRelease,
    const NProto::TJobToRemove& protoJobToRemove);

void ToProto(
    NProto::TReleaseJobFlags* protoReleaseJobFlags,
    const NControllerAgent::TReleaseJobFlags& releaseJobFlags);

void FromProto(
    NControllerAgent::TReleaseJobFlags* releaseJobFlags,
    const NProto::TReleaseJobFlags& protoReleaseJobFlags);

void ToProto(
    NProto::TJobToAbort* protoJobToAbort,
    const NControllerAgent::TJobToAbort& jobToAbort);
void FromProto(
    NControllerAgent::TJobToAbort* jobToAbort,
    const NProto::TJobToAbort& protoJobToAbort);

void ToProto(
    NProto::TJobToStore* protoJobToStore,
    const NControllerAgent::TJobToStore& jobToStore);
void FromProto(
    NControllerAgent::TJobToStore* jobToStore,
    const NProto::TJobToStore& protoJobToStore);

void ToProto(
    NProto::TJobToConfirm* protoJobToConfirm,
    const NControllerAgent::TJobToConfirm& jobToConfirm);
void FromProto(
    NControllerAgent::TJobToConfirm* jobToConfirm,
    const NProto::TJobToConfirm& protoJobToConfirm);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
