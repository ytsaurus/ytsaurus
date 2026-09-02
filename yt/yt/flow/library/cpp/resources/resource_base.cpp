#include "resource_base.h"

#include "file_provider_postprocessor.h"

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/resource_manager.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/file_storage/file_storage.h>

#include <yt/yt/core/crypto/crypto.h>
#include <yt/yt/core/misc/collection_helpers.h>

#include <algorithm>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

std::string GetFileProviderIdentityDigest(
    const TResourceId& resourceId,
    const TFileProviderId& providerId,
    const TFileProviderRevisionPtr& revision)
{
    NCrypto::TSha256Hasher hasher;
    hasher.Append(Format("%Qv-%Qv-%Qv",
        resourceId.Underlying(),
        providerId.Underlying(),
        revision->ObjectId.Underlying()));
    return hasher.GetHexDigestLowerCase();
}

NFileStorage::TFileStorageObjectId GetFileProviderDownloadObjectId(
    const TResourceId& resourceId,
    const TFileProviderId& providerId,
    const TFileProviderRevisionPtr& revision)
{
    return NFileStorage::TFileStorageObjectId(Format("%v-%v-%v-original-%v",
        resourceId.Underlying(),
        providerId.Underlying(),
        revision->ObjectId.Underlying(),
        GetFileProviderIdentityDigest(resourceId, providerId, revision)));
}

NFileStorage::TFileStorageObjectId GetFileProviderPostprocessObjectId(
    const TResourceId& resourceId,
    const TFileProviderId& providerId,
    const TFileProviderRevisionPtr& revision,
    const TFileProviderSpecPtr& providerSpec)
{
    YT_VERIFY(providerSpec->PostprocessCommand);

    NCrypto::TSha256Hasher hasher;
    hasher.Append(*providerSpec->PostprocessCommand);
    return NFileStorage::TFileStorageObjectId(Format("%v-%v-%v-postprocess-%v-%v",
        resourceId.Underlying(),
        providerId.Underlying(),
        revision->ObjectId.Underlying(),
        hasher.GetHexDigestLowerCase(),
        GetFileProviderIdentityDigest(resourceId, providerId, revision)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TMaterializedFileProvider::TMaterializedFileProvider(
    TFileProviderRevisionPtr revision,
    NFileStorage::IFileStorageObjectPtr storageObject)
    : Revision_(std::move(revision))
    , StorageObject_(std::move(storageObject))
    , RootPath_(StorageObject_->GetPath())
{ }

const TFileProviderRevisionPtr& TMaterializedFileProvider::GetRevision() const
{
    return Revision_;
}

const std::string& TMaterializedFileProvider::GetRootPath() const
{
    return RootPath_;
}

TMaterializedFileProviderSnapshot::TMaterializedFileProviderSnapshot(
    TFileSnapshotPtr fileSnapshot,
    THashMap<TFileProviderId, TMaterializedFileProviderPtr> fileProviders)
    : FileSnapshot_(std::move(fileSnapshot))
    , FileProviders_(std::move(fileProviders))
{ }

const TFileSnapshotPtr& TMaterializedFileProviderSnapshot::GetFileSnapshot() const
{
    return FileSnapshot_;
}

const THashMap<TFileProviderId, TMaterializedFileProviderPtr>& TMaterializedFileProviderSnapshot::GetFileProviders() const
{
    return FileProviders_;
}

const TMaterializedFileProviderPtr& TMaterializedFileProviderSnapshot::GetFileProvider(const TFileProviderId& id) const
{
    auto it = FileProviders_.find(id);
    THROW_ERROR_EXCEPTION_UNLESS(
        it != FileProviders_.end(),
        "Unknown materialized file provider %Qv",
        id);
    return it->second;
}

const TMaterializedFileProviderPtr& TMaterializedFileProviderSnapshot::GetOnlyFileProvider() const
{
    THROW_ERROR_EXCEPTION_UNLESS(
        FileProviders_.size() == 1,
        "Expected exactly one materialized file provider, got %v",
        FileProviders_.size());
    return FileProviders_.begin()->second;
}

////////////////////////////////////////////////////////////////////////////////

TResourceBase::TResourceBase(TResourceContextPtr context, TDynamicResourceContextPtr dynamicContext)
    : Context_(context)
    , DynamicContext_(dynamicContext)
    , Parameters_(TRegistry::Get()->ParseResourceParameters(context->ResourceSpec))
    , DynamicParameters_(TRegistry::Get()->ParseResourceDynamicParameters(context->ResourceSpec, dynamicContext->DynamicResourceSpec))
    , Logger(Context_->Logger)
{
    for (const auto& [name, providerSpec] : Context_->ResourceSpec->FileProviders) {
        auto providerContext = New<TFileProviderContext>();
        providerContext->ProviderSpec = providerSpec;
        providerContext->ClientsCache = Context_->ClientsCache;
        providerContext->PipelinePath = Context_->PipelinePath;
        providerContext->Invoker = Context_->Invoker;
        providerContext->Logger = Context_->Logger
            .WithTag("Component", "FileProvider")
            .WithTag("FileProvider", name);

        auto dynamicProviderContext = New<TDynamicFileProviderContext>();
        dynamicProviderContext->DynamicFileProviderSpec = GetOrDefault(
            dynamicContext->DynamicResourceSpec->FileProviders,
            name,
            New<TDynamicFileProviderSpec>());
        EmplaceOrCrash(
            FileProviders_,
            name,
            TRegistry::Get()->CreateFileProvider(providerContext, dynamicProviderContext));
    }

    SubscribeReconfigured(BIND([this] (const TDynamicResourceContextPtr& dynamicContext) {
        DynamicContext_ = dynamicContext;
        DynamicParameters_ = TRegistry::Get()->ParseResourceDynamicParameters(Context_->ResourceSpec, dynamicContext->DynamicResourceSpec);
        for (const auto& [name, provider] : FileProviders_) {
            auto dynamicProviderContext = New<TDynamicFileProviderContext>();
            dynamicProviderContext->DynamicFileProviderSpec = GetOrDefault(
                dynamicContext->DynamicResourceSpec->FileProviders,
                name,
                New<TDynamicFileProviderSpec>());
            provider->Reconfigure(dynamicProviderContext);
        }
    }));
}

TFuture<TMaterializedFileProviderPtr> TResourceBase::MaterializeFileProvider(
    const TFileSnapshotPtr& fileSnapshot,
    const TFileProviderId& id) const
{
    THROW_ERROR_EXCEPTION_UNLESS(fileSnapshot, "Cannot materialize a file provider without a file snapshot");
    auto providerIt = FileProviders_.find(id);
    THROW_ERROR_EXCEPTION_UNLESS(
        providerIt != FileProviders_.end(),
        "File provider %Qv is not configured for resource %Qv",
        id,
        Context_->ResourceId);
    auto revisionIt = fileSnapshot->FileProviders.find(id);
    THROW_ERROR_EXCEPTION_UNLESS(
        revisionIt != fileSnapshot->FileProviders.end(),
        "File snapshot %v has no file provider %Qv",
        fileSnapshot->Id,
        id);

    const auto& providerSpec = GetOrCrash(Context_->ResourceSpec->FileProviders, id);
    const auto& revision = revisionIt->second;
    THROW_ERROR_EXCEPTION_UNLESS(
        revision,
        "File snapshot %v has null file provider %Qv",
        fileSnapshot->Id,
        id);
    THROW_ERROR_EXCEPTION_UNLESS(
        revision->FileProviderClassName == providerSpec->FileProviderClassName,
        "File snapshot provider %Qv class %Qv differs from configured class %Qv",
        id,
        revision->FileProviderClassName,
        providerSpec->FileProviderClassName);
    THROW_ERROR_EXCEPTION_UNLESS(
        Context_->FileStorage,
        "Resource cannot materialize file provider %Qv because file storage is unavailable in this process",
        id);

    auto rawObjectFuture = Context_->FileStorage->GetOrCreate(
        GetFileProviderDownloadObjectId(Context_->ResourceId, id, revision),
        revision->Size,
        [provider = providerIt->second, revision] (const std::string& directory) {
            return provider->Download(revision, directory);
        });
    TFuture<NFileStorage::IFileStorageObjectPtr> storageObjectFuture;
    if (!providerSpec->PostprocessCommand) {
        storageObjectFuture = std::move(rawObjectFuture);
    } else {
        storageObjectFuture = rawObjectFuture.Apply(BIND([
            providerId = id,
            revision,
            providerSpec,
            objectId = GetFileProviderPostprocessObjectId(Context_->ResourceId, id, revision, providerSpec),
            fileStorage = Context_->FileStorage,
            invoker = Context_->Invoker,
            logger = Logger
        ] (NFileStorage::IFileStorageObjectPtr rawObject) {
            return fileStorage->GetOrCreate(
                objectId,
                std::nullopt,
                [
                    providerId,
                    revision,
                    providerSpec,
                    rawObject = std::move(rawObject),
                    invoker,
                    logger
                ] (const std::string& directory) {
                    return BIND(
                        &PostprocessFileProvider,
                        providerId,
                        revision,
                        providerSpec,
                        rawObject,
                        directory,
                        logger)
                        .AsyncVia(invoker)
                        .Run();
                });
        }).AsyncVia(Context_->Invoker));
    }

    return storageObjectFuture
        .Apply(BIND([revision] (NFileStorage::IFileStorageObjectPtr storageObject) {
            return New<TMaterializedFileProvider>(revision, std::move(storageObject));
        }))
        .ToUncancelable();
}

TFuture<TMaterializedFileProviderSnapshotPtr> TResourceBase::MaterializeFileProviders(
    const TFileSnapshotPtr& fileSnapshot,
    const std::vector<TFileProviderId>& ids) const
{
    THROW_ERROR_EXCEPTION_UNLESS(fileSnapshot, "Cannot materialize file providers without a file snapshot");

    std::vector<TFileProviderId> requestedIds = ids;
    if (requestedIds.empty()) {
        THROW_ERROR_EXCEPTION_UNLESS(
            fileSnapshot->FileProviders.size() == FileProviders_.size(),
            "File snapshot %v has %v file providers while resource %Qv configures %v",
            fileSnapshot->Id,
            fileSnapshot->FileProviders.size(),
            Context_->ResourceId,
            FileProviders_.size());
        requestedIds.reserve(FileProviders_.size());
        for (const auto& [id, _] : FileProviders_) {
            requestedIds.push_back(id);
        }
        std::sort(requestedIds.begin(), requestedIds.end());
    }

    THashSet<TFileProviderId> uniqueIds;
    std::vector<TFuture<TMaterializedFileProviderPtr>> futures;
    futures.reserve(requestedIds.size());
    for (const auto& id : requestedIds) {
        THROW_ERROR_EXCEPTION_UNLESS(uniqueIds.insert(id).second, "File provider %Qv was requested more than once", id);
        futures.push_back(MaterializeFileProvider(fileSnapshot, id));
    }

    return AllSucceeded(std::move(futures))
        .Apply(BIND([
            fileSnapshot,
            requestedIds = std::move(requestedIds)
        ] (const std::vector<TMaterializedFileProviderPtr>& materialized) {
            THashMap<TFileProviderId, TMaterializedFileProviderPtr> fileProviders;
            for (int index = 0; index < std::ssize(requestedIds); ++index) {
                EmplaceOrCrash(fileProviders, requestedIds[index], materialized[index]);
            }
            return New<TMaterializedFileProviderSnapshot>(fileSnapshot, std::move(fileProviders));
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
