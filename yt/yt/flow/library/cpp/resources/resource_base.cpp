#include "resource_base.h"

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/resource_manager.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/file_storage/file_storage.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <algorithm>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TMaterializedFileSource::TMaterializedFileSource(
    TFileSourceRevisionPtr revision,
    NFileStorage::IFileStorageObjectPtr storageObject)
    : Revision_(std::move(revision))
    , StorageObject_(std::move(storageObject))
    , RootPath_(StorageObject_->GetPath())
{ }

const TFileSourceRevisionPtr& TMaterializedFileSource::GetRevision() const
{
    return Revision_;
}

const std::string& TMaterializedFileSource::GetRootPath() const
{
    return RootPath_;
}

TMaterializedFileSourceSnapshot::TMaterializedFileSourceSnapshot(
    TFileSnapshotPtr fileSnapshot,
    THashMap<TFileSourceId, TMaterializedFileSourcePtr> fileSources)
    : FileSnapshot_(std::move(fileSnapshot))
    , FileSources_(std::move(fileSources))
{ }

const TFileSnapshotPtr& TMaterializedFileSourceSnapshot::GetFileSnapshot() const
{
    return FileSnapshot_;
}

const THashMap<TFileSourceId, TMaterializedFileSourcePtr>& TMaterializedFileSourceSnapshot::GetFileSources() const
{
    return FileSources_;
}

const TMaterializedFileSourcePtr& TMaterializedFileSourceSnapshot::GetFileSource(const TFileSourceId& id) const
{
    auto it = FileSources_.find(id);
    THROW_ERROR_EXCEPTION_UNLESS(
        it != FileSources_.end(),
        "Unknown materialized file source %Qv",
        id);
    return it->second;
}

const TMaterializedFileSourcePtr& TMaterializedFileSourceSnapshot::GetOnlyFileSource() const
{
    THROW_ERROR_EXCEPTION_UNLESS(
        FileSources_.size() == 1,
        "Expected exactly one materialized file source, got %v",
        FileSources_.size());
    return FileSources_.begin()->second;
}

////////////////////////////////////////////////////////////////////////////////

TResourceBase::TResourceBase(TResourceContextPtr context, TDynamicResourceContextPtr dynamicContext)
    : Context_(context)
    , DynamicContext_(dynamicContext)
    , Parameters_(TRegistry::Get()->ParseResourceParameters(context->ResourceSpec))
    , DynamicParameters_(TRegistry::Get()->ParseResourceDynamicParameters(context->ResourceSpec, dynamicContext->DynamicResourceSpec))
    , Logger(Context_->Logger)
{
    for (const auto& [name, sourceSpec] : Context_->ResourceSpec->FileSources) {
        auto sourceContext = New<TFileSourceContext>();
        sourceContext->SourceSpec = sourceSpec;
        sourceContext->ClientsCache = Context_->ClientsCache;
        sourceContext->PipelinePath = Context_->PipelinePath;
        sourceContext->Invoker = Context_->Invoker;
        sourceContext->Logger = Context_->Logger
            .WithTag("Component", "FileSource")
            .WithTag("FileSource", name);

        auto dynamicSourceContext = New<TDynamicFileSourceContext>();
        dynamicSourceContext->DynamicFileSourceSpec = GetOrDefault(
            dynamicContext->DynamicResourceSpec->FileSources,
            name,
            New<TDynamicFileSourceSpec>());
        EmplaceOrCrash(
            FileSources_,
            name,
            TRegistry::Get()->CreateFileSource(sourceContext, dynamicSourceContext));
    }

    SubscribeReconfigured(BIND([this] (const TDynamicResourceContextPtr& dynamicContext) {
        DynamicContext_ = dynamicContext;
        DynamicParameters_ = TRegistry::Get()->ParseResourceDynamicParameters(Context_->ResourceSpec, dynamicContext->DynamicResourceSpec);
        for (const auto& [name, source] : FileSources_) {
            auto dynamicSourceContext = New<TDynamicFileSourceContext>();
            dynamicSourceContext->DynamicFileSourceSpec = GetOrDefault(
                dynamicContext->DynamicResourceSpec->FileSources,
                name,
                New<TDynamicFileSourceSpec>());
            source->Reconfigure(dynamicSourceContext);
        }
    }));
}

TFuture<TMaterializedFileSourcePtr> TResourceBase::MaterializeFileSource(
    const TFileSnapshotPtr& fileSnapshot,
    const TFileSourceId& id) const
{
    THROW_ERROR_EXCEPTION_UNLESS(fileSnapshot, "Cannot materialize a file source without a file snapshot");
    auto sourceIt = FileSources_.find(id);
    THROW_ERROR_EXCEPTION_UNLESS(
        sourceIt != FileSources_.end(),
        "File source %Qv is not configured for resource %Qv",
        id,
        Context_->ResourceId);
    auto revisionIt = fileSnapshot->FileSources.find(id);
    THROW_ERROR_EXCEPTION_UNLESS(
        revisionIt != fileSnapshot->FileSources.end(),
        "File snapshot %v has no file source %Qv",
        fileSnapshot->Id,
        id);

    const auto& sourceSpec = GetOrCrash(Context_->ResourceSpec->FileSources, id);
    const auto& revision = revisionIt->second;
    THROW_ERROR_EXCEPTION_UNLESS(
        revision,
        "File snapshot %v has null file source %Qv",
        fileSnapshot->Id,
        id);
    THROW_ERROR_EXCEPTION_UNLESS(
        revision->FileSourceClassName == sourceSpec->FileSourceClassName,
        "File snapshot source %Qv class %Qv differs from configured class %Qv",
        id,
        revision->FileSourceClassName,
        sourceSpec->FileSourceClassName);
    THROW_ERROR_EXCEPTION_UNLESS(
        Context_->FileStorage,
        "Resource cannot materialize file source %Qv because file storage is unavailable in this process",
        id);

    return Context_->FileStorage->GetOrCreate(
        revision->ObjectId,
        revision->Size,
        [source = sourceIt->second, revision] (const std::string& directory) {
            return source->Download(revision, directory);
        })
        .Apply(BIND([revision] (NFileStorage::IFileStorageObjectPtr storageObject) {
            return New<TMaterializedFileSource>(revision, std::move(storageObject));
        }))
        .ToUncancelable();
}

TFuture<TMaterializedFileSourceSnapshotPtr> TResourceBase::MaterializeFileSources(
    const TFileSnapshotPtr& fileSnapshot,
    const std::vector<TFileSourceId>& ids) const
{
    THROW_ERROR_EXCEPTION_UNLESS(fileSnapshot, "Cannot materialize file sources without a file snapshot");

    std::vector<TFileSourceId> requestedIds = ids;
    if (requestedIds.empty()) {
        THROW_ERROR_EXCEPTION_UNLESS(
            fileSnapshot->FileSources.size() == FileSources_.size(),
            "File snapshot %v has %v file sources while resource %Qv configures %v",
            fileSnapshot->Id,
            fileSnapshot->FileSources.size(),
            Context_->ResourceId,
            FileSources_.size());
        requestedIds.reserve(FileSources_.size());
        for (const auto& [id, _] : FileSources_) {
            requestedIds.push_back(id);
        }
        std::sort(requestedIds.begin(), requestedIds.end());
    }

    THashSet<TFileSourceId> uniqueIds;
    std::vector<TFuture<TMaterializedFileSourcePtr>> futures;
    futures.reserve(requestedIds.size());
    for (const auto& id : requestedIds) {
        THROW_ERROR_EXCEPTION_UNLESS(uniqueIds.insert(id).second, "File source %Qv was requested more than once", id);
        futures.push_back(MaterializeFileSource(fileSnapshot, id));
    }

    return AllSucceeded(std::move(futures))
        .Apply(BIND([
            fileSnapshot,
            requestedIds = std::move(requestedIds)
        ] (const std::vector<TMaterializedFileSourcePtr>& materialized) {
            THashMap<TFileSourceId, TMaterializedFileSourcePtr> fileSources;
            for (int index = 0; index < std::ssize(requestedIds); ++index) {
                EmplaceOrCrash(fileSources, requestedIds[index], materialized[index]);
            }
            return New<TMaterializedFileSourceSnapshot>(fileSnapshot, std::move(fileSources));
        }))
        .ToUncancelable();
}

TResourceContextPtr TResourceBase::GetContext() const
{
    return Context_;
}

TDynamicResourceContextPtr TResourceBase::GetDynamicContext() const
{
    return DynamicContext_.Acquire();
}

TResourceSpecPtr TResourceBase::GetSpec() const
{
    return Context_->ResourceSpec;
}

TDynamicResourceSpecPtr TResourceBase::GetDynamicSpec() const
{
    return GetDynamicContext()->DynamicResourceSpec;
}

NYTree::TYsonStructPtr TResourceBase::GetParametersBase() const
{
    return Parameters_;
}

TFuture<void> TResourceBase::Load(const THashMap<TResourceId, IResourcePtr>& /*dependencies*/)
{
    return OKFuture;
}

void TResourceBase::Reconfigure(const TDynamicResourceContextPtr& dynamicContext)
{
    TReconfigurable<TDynamicResourceContext>::Reconfigure(dynamicContext);
}

TResourceRevisionState TResourceBase::GetRevisionState() const
{
    auto dynamicContext = GetDynamicContext();
    if (!dynamicContext->TargetRevision) {
        return {};
    }
    return {
        .AppliedRevisionId = dynamicContext->TargetRevision->RevisionId,
        .TargetRevisionId = dynamicContext->TargetRevision->RevisionId,
        .ResourceInstanceId = Context_->ResourceInstanceId,
        .ResourceIncarnationGeneration = Context_->ResourceIncarnationGeneration,
    };
}

NYTree::TYsonStructPtr TResourceBase::GetDynamicParametersBase() const
{
    return DynamicParameters_.Acquire();
}

void TResourceBase::FeedStatus(i64 morePushedToQueue, i64 moreFetchedFromQueue)
{
    auto resourceManager = Context_->ResourceManager.Lock();
    if (!resourceManager) {
        YT_TLOG_WARNING("Resource manager is not available, skipping FeedStatus");
        return;
    }
    resourceManager->FeedStatus(Context_->ResourceId, morePushedToQueue, moreFetchedFromQueue);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
