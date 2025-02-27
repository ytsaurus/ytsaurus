#pragma once

#include "public.h"

#include <yt/yt/server/lib/controller_agent/proto/job_tracker_service.pb.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/controller_agent/persistence.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/phoenix/type_decl.h>
#include <yt/yt/core/phoenix/type_def.h>

#include <yt/yt/core/tracing/public.h>

namespace NYT::NControllerAgent {

static constexpr TStringBuf DockerAuthEnv("docker_auth");

////////////////////////////////////////////////////////////////////////////////

NNodeTrackerClient::TNodeId NodeIdFromJobId(TJobId jobId);

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

    PHOENIX_DECLARE_TYPE(TReleaseJobFlags, 0xb71fbb3f);
};

struct TJobToRelease
{
    TJobId JobId;
    TReleaseJobFlags ReleaseFlags = {};
};

void FormatValue(TStringBuilderBase* builder, const TReleaseJobFlags& releaseFlags, TStringBuf spec);

struct TJobToAbort
{
    TJobId JobId;
    NScheduler::EAbortReason AbortReason;
    bool Graceful = false;
    bool RequestNewJob = false;
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

void ValidateJobShellAccess(
    const NApi::NNative::IClientPtr& client,
    const std::string& user,
    const TString& jobShellName,
    const std::vector<TString>& jobShellOwners);

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
    TJobId jobId,
    EJobType jobType);

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
