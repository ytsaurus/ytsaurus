#include "helpers.h"

#include <yt/yt/server/lib/controller_agent/serialize.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/column_rename_descriptor.h>

#include <yt/yt/ytlib/table_client/schema.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/format.h>
#include <yt/yt/core/misc/guid.h>
#include <yt/yt/core/misc/phoenix.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>

#include <util/generic/cast.h>

// TODO(max42): this whole file must be moved to server/lib/job_tracker_client.
namespace NYT::NJobTrackerClient {

using namespace NTableClient;

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
                    column.SetStableName(TStableName(column.Name()));
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

namespace NProto {

void ToProto(NProto::TReleaseJobFlags* protoReleaseJobFlags, const NJobTrackerClient::TReleaseJobFlags& releaseJobFlags)
{
    protoReleaseJobFlags->set_archive_job_spec(releaseJobFlags.ArchiveJobSpec);
    protoReleaseJobFlags->set_archive_stderr(releaseJobFlags.ArchiveStderr);
    protoReleaseJobFlags->set_archive_fail_context(releaseJobFlags.ArchiveFailContext);
    protoReleaseJobFlags->set_archive_profile(releaseJobFlags.ArchiveProfile);
}

void FromProto(NJobTrackerClient::TReleaseJobFlags* releaseJobFlags, const NProto::TReleaseJobFlags& protoReleaseJobFlags)
{
    releaseJobFlags->ArchiveJobSpec = protoReleaseJobFlags.archive_job_spec();
    releaseJobFlags->ArchiveStderr = protoReleaseJobFlags.archive_stderr();
    releaseJobFlags->ArchiveFailContext = protoReleaseJobFlags.archive_fail_context();
    releaseJobFlags->ArchiveProfile = protoReleaseJobFlags.archive_profile();
}

void ToProto(NProto::TJobToRemove* protoJobToRemove, const NJobTrackerClient::TJobToRelease& jobToRelease)
{
    ToProto(protoJobToRemove->mutable_job_id(), jobToRelease.JobId);
    ToProto(protoJobToRemove->mutable_release_job_flags(), jobToRelease.ReleaseFlags);
}

void FromProto(NJobTrackerClient::TJobToRelease* jobToRelease, const NProto::TJobToRemove& protoJobToRemove)
{
    FromProto(&jobToRelease->JobId, protoJobToRemove.job_id());
    FromProto(&jobToRelease->ReleaseFlags, protoJobToRemove.release_job_flags());
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobTrackerClient
