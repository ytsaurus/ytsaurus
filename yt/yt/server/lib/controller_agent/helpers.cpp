#include "helpers.h"

#include <yt/yt/server/lib/controller_agent/serialize.h>

#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/controller_agent/helpers.h>
#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/scheduler/proto/resources.pb.h>

#include <yt/yt/ytlib/security_client/helpers.h>

#include <yt/yt/ytlib/table_client/schema.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/misc/io_tags.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/column_rename_descriptor.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/guid.h>
#include <yt/yt/core/misc/phoenix.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <util/generic/cast.h>

namespace NYT::NControllerAgent {

using namespace NTableClient;
using namespace NObjectClient;
using namespace NSecurityClient;
using namespace NScheduler;
using namespace NApi;
using namespace NApi::NNative;

using NNodeTrackerClient::TNodeId;

////////////////////////////////////////////////////////////////////////////////

TNodeId NodeIdFromJobId(TJobId jobId)
{
    return NodeIdFromAllocationId(AllocationIdFromJobId(jobId));
}

////////////////////////////////////////////////////////////////////////////////

TString JobTypeAsKey(EJobType jobType)
{
    return Format("%lv", jobType);
}

////////////////////////////////////////////////////////////////////////////////

bool TReleaseJobFlags::IsNonTrivial() const
{
    return ArchiveJobSpec || ArchiveStderr || ArchiveFailContext || ArchiveProfile;
}

bool TReleaseJobFlags::IsTrivial() const
{
    return !IsNonTrivial();
}

void TReleaseJobFlags::Persist(const TStreamPersistenceContext& context)
{
    using namespace NYT::NControllerAgent;
    using NYT::Persist;

    Persist(context, ArchiveStderr);
    Persist(context, ArchiveJobSpec);
    Persist(context, ArchiveFailContext);
    Persist(context, ArchiveProfile);
}

TString ToString(const TReleaseJobFlags& releaseFlags)
{
    return Format(
        "ArchiveStderr: %v, ArchiveJobSpec: %v, ArchiveFailContext: %v, ArchiveProfile: %v",
        releaseFlags.ArchiveStderr,
        releaseFlags.ArchiveJobSpec,
        releaseFlags.ArchiveFailContext,
        releaseFlags.ArchiveProfile);
}

////////////////////////////////////////////////////////////////////////////////

TTableSchemaPtr RenameColumnsInSchema(
    TStringBuf tableDescription,
    const TTableSchemaPtr& originalSchema,
    bool isDynamic,
    const TColumnRenameDescriptors& renameDescriptors,
    bool changeStableName)
{
    auto schema = originalSchema;
    try {
        THashMap<TStringBuf, TStringBuf> columnMapping;
        for (const auto& descriptor : renameDescriptors) {
            EmplaceOrCrash(columnMapping, descriptor.OriginalName, descriptor.NewName);
        }
        auto newColumns = schema->Columns();
        for (auto& column : newColumns) {
            auto it = columnMapping.find(column.Name());
            if (it != columnMapping.end()) {
                column.SetName(TString(it->second));
                if (changeStableName) {
                    column.SetStableName(TColumnStableName(column.Name()));
                }
                ValidateColumnSchema(column, schema->IsSorted(), isDynamic);
                columnMapping.erase(it);
            }
        }
        if (!columnMapping.empty()) {
            THROW_ERROR_EXCEPTION("Rename is supported only for columns in schema")
                << TErrorAttribute("failed_rename_descriptors", columnMapping)
                << TErrorAttribute("schema", schema);
        }
        schema = New<TTableSchema>(newColumns, schema->GetStrict(), schema->GetUniqueKeys());
        ValidateColumnUniqueness(*schema);
        return schema;
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error renaming columns")
            << TErrorAttribute("table_description", tableDescription)
            << TErrorAttribute("column_rename_descriptors", renameDescriptors)
            << ex;
    }
}

////////////////////////////////////////////////////////////////////////////////

void ValidateJobShellAccess(
    const NApi::NNative::IClientPtr& client,
    const TString& user,
    const TString& jobShellName,
    const std::vector<TString>& jobShellOwners)
{
    if (user == RootUserName || user == SuperusersGroupName) {
        return;
    }

    auto proxy = CreateObjectServiceReadProxy(
        client,
        EMasterChannelKind::Cache);
    TMasterReadOptions readOptions;
    readOptions.ReadFrom = EMasterChannelKind::Cache;

    auto userClosure = GetSubjectClosure(
        user,
        proxy,
        client->GetNativeConnection(),
        readOptions);

    auto allowedSubjects = jobShellOwners;
    allowedSubjects.push_back(RootUserName);
    allowedSubjects.push_back(SuperusersGroupName);

    for (const auto& allowedSubject : allowedSubjects) {
        if (allowedSubject == user || userClosure.contains(allowedSubject)) {
            return;
        }
    }

    THROW_ERROR_EXCEPTION(
        NSecurityClient::EErrorCode::AuthorizationError,
        "User %Qv is not allowed to run job shell %Qv",
        user,
        jobShellName);
}

////////////////////////////////////////////////////////////////////////////////

void PackBaggageFromJobSpec(
    const NTracing::TTraceContextPtr& traceContext,
    const NControllerAgent::NProto::TJobSpec& jobSpec,
    TOperationId operationId,
    TJobId jobId)
{
    auto baggage = traceContext->UnpackOrCreateBaggage();
    const auto& jobSpecExt = jobSpec.GetExtension(NControllerAgent::NProto::TJobSpecExt::job_spec_ext);
    auto ioTags = NYTree::FromProto(jobSpecExt.io_tags());
    baggage->MergeFrom(*ioTags);
    AddTagToBaggage(baggage, ERawIOTag::OperationId, ToString(operationId));
    AddTagToBaggage(baggage, ERawIOTag::JobId, ToString(jobId));
    AddTagToBaggage(baggage, EAggregateIOTag::JobType, FormatEnum(static_cast<EJobType>(jobSpec.type())));
    traceContext->PackBaggage(baggage);
}

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(NProto::TReleaseJobFlags* protoReleaseJobFlags, const NControllerAgent::TReleaseJobFlags& releaseJobFlags)
{
    protoReleaseJobFlags->set_archive_job_spec(releaseJobFlags.ArchiveJobSpec);
    protoReleaseJobFlags->set_archive_stderr(releaseJobFlags.ArchiveStderr);
    protoReleaseJobFlags->set_archive_fail_context(releaseJobFlags.ArchiveFailContext);
    protoReleaseJobFlags->set_archive_profile(releaseJobFlags.ArchiveProfile);
}

void FromProto(NControllerAgent::TReleaseJobFlags* releaseJobFlags, const NProto::TReleaseJobFlags& protoReleaseJobFlags)
{
    releaseJobFlags->ArchiveJobSpec = protoReleaseJobFlags.archive_job_spec();
    releaseJobFlags->ArchiveStderr = protoReleaseJobFlags.archive_stderr();
    releaseJobFlags->ArchiveFailContext = protoReleaseJobFlags.archive_fail_context();
    releaseJobFlags->ArchiveProfile = protoReleaseJobFlags.archive_profile();
}

void ToProto(NProto::TJobToRemove* protoJobToRemove, const NControllerAgent::TJobToRelease& jobToRelease)
{
    ToProto(protoJobToRemove->mutable_job_id(), jobToRelease.JobId);
    ToProto(protoJobToRemove->mutable_release_job_flags(), jobToRelease.ReleaseFlags);
}

void FromProto(NControllerAgent::TJobToRelease* jobToRelease, const NProto::TJobToRemove& protoJobToRemove)
{
    FromProto(&jobToRelease->JobId, protoJobToRemove.job_id());
    FromProto(&jobToRelease->ReleaseFlags, protoJobToRemove.release_job_flags());
}

void ToProto(NProto::TJobToAbort* protoJobToAbort, const NControllerAgent::TJobToAbort& jobToAbort)
{
    ToProto(protoJobToAbort->mutable_job_id(), jobToAbort.JobId);
    protoJobToAbort->set_abort_reason(static_cast<i32>(jobToAbort.AbortReason));
    protoJobToAbort->set_graceful(jobToAbort.Graceful);
}

void FromProto(NControllerAgent::TJobToAbort* jobToAbort, const NProto::TJobToAbort& protoJobToAbort)
{
    FromProto(&jobToAbort->JobId, protoJobToAbort.job_id());
    jobToAbort->AbortReason = NYT::FromProto<EAbortReason>(protoJobToAbort.abort_reason());
    jobToAbort->Graceful = NYT::FromProto<bool>(protoJobToAbort.graceful());
}

void ToProto(
    NProto::TJobToStore* protoJobToStore,
    const NControllerAgent::TJobToStore& jobToStore)
{
    ToProto(protoJobToStore->mutable_job_id(), jobToStore.JobId);
}

void FromProto(
    NControllerAgent::TJobToStore* jobToStore,
    const NProto::TJobToStore& protoJobToStore)
{
    FromProto(&jobToStore->JobId, protoJobToStore.job_id());
}

void ToProto(
    NProto::TJobToConfirm* protoJobToConfirm,
    const NControllerAgent::TJobToConfirm& jobToConfirm)
{
    ToProto(protoJobToConfirm->mutable_job_id(), jobToConfirm.JobId);
}

void FromProto(
    NControllerAgent::TJobToConfirm* jobToConfirm,
    const NProto::TJobToConfirm& protoJobToConfirm)
{
    FromProto(&jobToConfirm->JobId, protoJobToConfirm.job_id());
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
