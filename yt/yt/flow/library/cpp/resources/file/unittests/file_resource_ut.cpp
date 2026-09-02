#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>
#include <yt/yt/flow/library/cpp/resources/file/file_resource.h>

#include <yt/yt/flow/library/cpp/resources/file_provider_postprocessor.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/init_context.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/resource_manager.h>
#include <yt/yt/flow/library/cpp/common/unittests/mock/state.h>
#include <yt/yt/flow/library/cpp/common/unittests/mock/time_provider.h>
#include <yt/yt/flow/library/cpp/file_providers/file_provider_base.h>
#include <yt/yt/flow/library/cpp/file_storage/file_storage.h>
#include <yt/yt/flow/library/cpp/misc/versioned_value.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/crypto/crypto.h>
#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/system/type_name.h>

#include <algorithm>
#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <deque>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

struct TFakeFileProviderParameters
    : public virtual TYsonStruct
{
    std::string Prefix;

    REGISTER_YSON_STRUCT(TFakeFileProviderParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("prefix", &TThis::Prefix)
            .NonEmpty();
    }
};

struct TFakeFileProviderDynamicParameters
    : public virtual TYsonStruct
{
    std::optional<std::string> PinnedContentId;

    REGISTER_YSON_STRUCT(TFakeFileProviderDynamicParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("pinned_content_id", &TThis::PinnedContentId)
            .Default();
        registrar.Postprocessor([] (TThis* parameters) {
            THROW_ERROR_EXCEPTION_IF(
                parameters->PinnedContentId && parameters->PinnedContentId->empty(),
                "Pinned content id must be nonempty");
        });
    }
};

class TFakeFileProvider
    : public TFileProviderBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TFakeFileProviderParameters, TFileProviderBase);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TFakeFileProviderDynamicParameters, TFileProviderBase);

    using TFileProviderBase::TFileProviderBase;

    TFuture<TFileProviderRevisionPtr> Discover() override
    {
        auto pinnedContentId = GetDynamicParameters()->PinnedContentId;
        TErrorOr<TFileProviderRevisionPtr> result;
        TFuture<void> gate = OKFuture;
        {
            auto guard = Guard(Lock_);
            const auto& prefix = GetParameters()->Prefix;
            ++DiscoverCounts_[prefix];
            if (pinnedContentId) {
                auto revision = New<TFileProviderRevision>();
                revision->FileProviderClassName = TypeName<TFakeFileProvider>();
                revision->ObjectId = NFileStorage::TFileStorageObjectId(*pinnedContentId);
                revision->DisplayVersion = *pinnedContentId;
                result = std::move(revision);
            } else {
                auto& results = DiscoverResults_[prefix];
                if (results.empty()) {
                    result = TFileProviderRevisionPtr{};
                } else {
                    result = results.front();
                    if (results.size() > 1) {
                        results.pop_front();
                    }
                }
            }

            if (result.IsOK() && result.Value()) {
                if (auto it = DiscoveryGates_.find(result.Value()->ObjectId.Underlying());
                    it != DiscoveryGates_.end())
                {
                    gate = it->second;
                }
            }
        }
        return gate.Apply(BIND([result = std::move(result)] {
            return result.ValueOrThrow();
        }));
    }

    TFuture<void> Download(
        const TFileProviderRevisionPtr& revision,
        const std::string& stagingDirectory) override
    {
        TFuture<void> gate = OKFuture;
        bool notifyStarted = false;
        int downloadCount = 0;
        {
            auto guard = Guard(Lock_);
            downloadCount = ++DownloadCounts_[revision->ObjectId.Underlying()];
            if (BlockedContentId_ == revision->ObjectId.Underlying()) {
                gate = DownloadGate_.ToFuture();
                notifyStarted = true;
            }
        }

        if (notifyStarted) {
            DownloadStarted_.Set();
        }

        if (revision->ObjectId.Underlying() == "download-failure" ||
            (revision->ObjectId.Underlying() == "download-failure-once" && downloadCount == 1))
        {
            return MakeFuture<void>(TError("Fake download failure"));
        }

        std::string relativePath = "artifact";
        auto contentId = revision->ObjectId.Underlying();
        auto payload = GetParameters()->Prefix + ":" + contentId;
        return gate.Apply(BIND([
            stagingDirectory,
            relativePath = std::move(relativePath),
            payload = std::move(payload),
            contentId = std::move(contentId)
        ] {
            auto path = TFsPath(stagingDirectory) / TFsPath(relativePath);
            TOFStream output(path.GetPath());
            output << payload;
            output.Finish();

            auto guard = Guard(Lock_);
            ++CompletedDownloadCounts_[contentId];

            return;
        }));
    }

    static void Reset()
    {
        auto guard = Guard(Lock_);
        DownloadCounts_.clear();
        CompletedDownloadCounts_.clear();
        BlockedContentId_.clear();
        DownloadGate_ = NewPromise<void>();
        DownloadStarted_ = NewPromise<void>();
        DiscoverCounts_.clear();
        DiscoverResults_.clear();
        DiscoveryGates_.clear();
        DiscoverResults_["payload"].push_back(TFileProviderRevisionPtr{});
    }

    static void Block(const std::string& contentId)
    {
        auto guard = Guard(Lock_);
        BlockedContentId_ = contentId;
        DownloadGate_ = NewPromise<void>();
        DownloadStarted_ = NewPromise<void>();
    }

    static TFuture<void> GetDownloadStartedFuture()
    {
        auto guard = Guard(Lock_);
        return DownloadStarted_.ToFuture();
    }

    static void Unblock()
    {
        TPromise<void> gate;
        {
            auto guard = Guard(Lock_);
            gate = DownloadGate_;
            BlockedContentId_.clear();
        }
        gate.Set();
    }

    static int GetDownloadCount(const std::string& contentId)
    {
        auto guard = Guard(Lock_);
        return GetOrDefault(DownloadCounts_, contentId);
    }

    static int GetCompletedDownloadCount(const std::string& contentId)
    {
        auto guard = Guard(Lock_);
        return GetOrDefault(CompletedDownloadCounts_, contentId);
    }

    static void SetDiscoveryError(const std::string& prefix = "payload")
    {
        auto guard = Guard(Lock_);
        auto& results = DiscoverResults_[prefix];
        results.clear();
        results.push_back(TError("Fake discovery failure"));
    }

    static void PushDiscoveryRevision(
        const std::string& contentId,
        const std::string& prefix = "payload")
    {
        auto revision = New<TFileProviderRevision>();
        revision->FileProviderClassName = TypeName<TFakeFileProvider>();
        revision->ObjectId = NFileStorage::TFileStorageObjectId(contentId);
        revision->DisplayVersion = contentId;

        auto guard = Guard(Lock_);
        DiscoverResults_[prefix].push_back(std::move(revision));
    }

    static void PushNullDiscovery(const std::string& prefix = "payload")
    {
        auto guard = Guard(Lock_);
        DiscoverResults_[prefix].push_back(TFileProviderRevisionPtr{});
    }

    static void SetDiscoveryGate(const std::string& contentId, TFuture<void> gate)
    {
        auto guard = Guard(Lock_);
        DiscoveryGates_[contentId] = std::move(gate);
    }

    static int GetDiscoverCount(const std::string& prefix = "payload")
    {
        auto guard = Guard(Lock_);
        return GetOrDefault(DiscoverCounts_, prefix);
    }

private:
    static NThreading::TSpinLock Lock_;
    static THashMap<std::string, int> DownloadCounts_;
    static THashMap<std::string, int> CompletedDownloadCounts_;
    static std::string BlockedContentId_;
    static TPromise<void> DownloadGate_;
    static TPromise<void> DownloadStarted_;
    static THashMap<std::string, int> DiscoverCounts_;
    static THashMap<std::string, std::deque<TErrorOr<TFileProviderRevisionPtr>>> DiscoverResults_;
    static THashMap<std::string, TFuture<void>> DiscoveryGates_;
};

NThreading::TSpinLock TFakeFileProvider::Lock_;
THashMap<std::string, int> TFakeFileProvider::DownloadCounts_;
THashMap<std::string, int> TFakeFileProvider::CompletedDownloadCounts_;
std::string TFakeFileProvider::BlockedContentId_;
TPromise<void> TFakeFileProvider::DownloadGate_ = NewPromise<void>();
TPromise<void> TFakeFileProvider::DownloadStarted_ = NewPromise<void>();
THashMap<std::string, int> TFakeFileProvider::DiscoverCounts_;
THashMap<std::string, std::deque<TErrorOr<TFileProviderRevisionPtr>>> TFakeFileProvider::DiscoverResults_;
THashMap<std::string, TFuture<void>> TFakeFileProvider::DiscoveryGates_;

YT_FLOW_DEFINE_FILE_PROVIDER(TFakeFileProvider);

////////////////////////////////////////////////////////////////////////////////

class TFakeStorageObject
    : public NFileStorage::IFileStorageObject
{
public:
    TFakeStorageObject(NFileStorage::TFileStorageObjectId id, std::string path)
        : Id_(std::move(id))
        , Path_(std::move(path))
    { }

    const NFileStorage::TFileStorageObjectId& GetId() const override
    {
        return Id_;
    }

    const std::string& GetPath() const override
    {
        return Path_;
    }

private:
    const NFileStorage::TFileStorageObjectId Id_;
    const std::string Path_;
};

void MakeTreeWritable(const TFsPath& path)
{
    if (!path.Exists()) {
        return;
    }

    TFileStat stat(path, /*nofollow*/ true);
    if (stat.IsSymlink()) {
        return;
    }

    YT_VERIFY(Chmod(path.GetPath().c_str(), stat.Mode | S_IRUSR | S_IWUSR | S_IXUSR) == 0);
    if (stat.IsDir()) {
        TVector<TFsPath> children;
        path.List(children);
        for (const auto& child : children) {
            MakeTreeWritable(child);
        }
    }
}

class TFakeFileStorage
    : public NFileStorage::IFileStorage
{
public:
    ~TFakeFileStorage() override
    {
        for (const auto& directory : Directories_) {
            MakeTreeWritable(TFsPath(directory->Name()));
        }
    }

    TFuture<NFileStorage::IFileStorageObjectPtr> GetOrCreate(
        NFileStorage::TFileStorageObjectId id,
        NFileStorage::TFileStorageFiller filler) override
    {
        auto rawId = id.Underlying();
        {
            auto guard = Guard(Lock_);
            if (auto it = Objects_.find(rawId); it != Objects_.end()) {
                return MakeFuture<NFileStorage::IFileStorageObjectPtr>(it->second);
            }
            if (auto it = Inflight_.find(rawId); it != Inflight_.end()) {
                return it->second;
            }
        }

        auto directory = std::make_unique<TTempDir>();
        auto path = (TFsPath(directory->Name()) / "payload").GetPath();
        TFsPath(path).MkDir();
        auto object = New<TFakeStorageObject>(id, path);
        auto promise = NewPromise<NFileStorage::IFileStorageObjectPtr>();
        auto future = promise.ToFuture().ToUncancelable();
        {
            auto guard = Guard(Lock_);
            Directories_.push_back(std::move(directory));
            Inflight_[rawId] = future;
        }

        filler(path).Subscribe(BIND([
            strongThis = MakeStrong(this),
            rawId = std::move(rawId),
            object = std::move(object),
            promise
        ] (const TError& error) {
            {
                auto guard = Guard(strongThis->Lock_);
                strongThis->Inflight_.erase(rawId);
                if (error.IsOK()) {
                    strongThis->Objects_[rawId] = object;
                }
            }
            if (error.IsOK()) {
                promise.Set(object);
            } else {
                promise.Set(error);
            }
        }));
        return future;
    }

private:
    NThreading::TSpinLock Lock_;
    std::vector<std::unique_ptr<TTempDir>> Directories_;
    THashMap<std::string, NFileStorage::IFileStorageObjectPtr> Objects_;
    THashMap<std::string, TFuture<NFileStorage::IFileStorageObjectPtr>> Inflight_;
};

struct TRealFileStorageFixture
{
    TTempDir Root;
    TActionQueuePtr Queue = New<TActionQueue>();
    IStatusProfilerPtr StatusProfiler = CreateSyncStatusProfiler();

    ~TRealFileStorageFixture()
    {
        MakeTreeWritable(TFsPath(Root.Name()));
    }

    NFileStorage::IFileStoragePtr MakeStorage() const
    {
        auto config = New<NFileStorage::TFileStorageConfig>();
        config->Path = Root.Name();
        config->SoftSizeLimit = 1_MB;
        config->HardSizeLimit = 2_MB;
        config->CleanupPeriod = TDuration::Hours(1);
        return NFileStorage::CreateFileStorage(
            std::move(config),
            Queue->GetInvoker(),
            NLogging::TLogger("FileResourceRealFileStorageTest"),
            {},
            StatusProfiler);
    }
};

class TThrowingFileStorage
    : public NFileStorage::IFileStorage
{
public:
    TFuture<NFileStorage::IFileStorageObjectPtr> GetOrCreate(
        NFileStorage::TFileStorageObjectId id,
        NFileStorage::TFileStorageFiller /*filler*/) override
    {
        return MakeFuture<NFileStorage::IFileStorageObjectPtr>(
            TError("File storage object %Qv requires worker.file_storage configuration",
                id.Underlying()));
    }
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTestState);

class TTestState
    : public TRefCounted
{
public:
    TTestState(std::string value, std::string filePath)
        : Value(std::move(value))
        , FilePath(std::move(filePath))
    { }

    const std::string Value;
    const std::string FilePath;
};

DEFINE_REFCOUNTED_TYPE(TTestState);

class TTestFileResource
    : public TFileResourceBase<TTestState>
{
public:
    using TFileResourceBase::TFileResourceBase;

    TFuture<TMaterializedFileProviderPtr> MaterializeOne(
        const TFileSnapshotPtr& fileSnapshot,
        const TFileProviderId& id) const
    {
        return MaterializeFileProvider(fileSnapshot, id);
    }

    TFuture<TMaterializedFileProviderSnapshotPtr> MaterializeMany(
        const TFileSnapshotPtr& fileSnapshot,
        const std::vector<TFileProviderId>& ids) const
    {
        return MaterializeFileProviders(fileSnapshot, ids);
    }

    static void Reset()
    {
        auto guard = Guard(Lock_);
        InitializeCounts_.clear();
        BlockedInitializationValue_.clear();
        InitializationGate_ = NewPromise<void>();
        InitializationStarted_ = NewPromise<void>();
        BlockedValidationValue_.clear();
        ValidationGate_ = NewPromise<void>();
        ValidationStarted_ = NewPromise<void>();
    }

    static int GetInitializeCount(const std::string& value)
    {
        auto guard = Guard(Lock_);
        return GetOrDefault(InitializeCounts_, value);
    }

    static void BlockInitialization(const std::string& value)
    {
        auto guard = Guard(Lock_);
        BlockedInitializationValue_ = value;
        InitializationGate_ = NewPromise<void>();
        InitializationStarted_ = NewPromise<void>();
    }

    static TFuture<void> GetInitializationStartedFuture()
    {
        auto guard = Guard(Lock_);
        return InitializationStarted_.ToFuture();
    }

    static void UnblockInitialization()
    {
        TPromise<void> gate;
        {
            auto guard = Guard(Lock_);
            BlockedInitializationValue_.clear();
            gate = InitializationGate_;
        }
        gate.Set();
    }

    static void BlockValidation(const std::string& value)
    {
        auto guard = Guard(Lock_);
        BlockedValidationValue_ = value;
        ValidationGate_ = NewPromise<void>();
        ValidationStarted_ = NewPromise<void>();
    }

    static TFuture<void> GetValidationStartedFuture()
    {
        auto guard = Guard(Lock_);
        return ValidationStarted_.ToFuture();
    }

    static void UnblockValidation()
    {
        TPromise<void> gate;
        {
            auto guard = Guard(Lock_);
            BlockedValidationValue_.clear();
            gate = ValidationGate_;
        }
        gate.Set();
    }

protected:
    TTestStatePtr Initialize(const TMaterializedFileProviderSnapshotPtr& fileProviders) override
    {
        if (fileProviders->GetFileProviders().size() == 1) {
            const auto& fileProvider = fileProviders->GetOnlyFileProvider();
            auto path = TFsPath(fileProvider->GetRootPath()).Child("artifact").GetPath();
            auto input = TFileInput(TString(path));
            auto contents = input.ReadAll();
            std::string value(contents.data(), contents.size());

            {
                auto guard = Guard(Lock_);
                ++InitializeCounts_[value];
            }

            WaitForInitializationIfBlocked(value);

            if (value.contains("initialize-failure")) {
                THROW_ERROR_EXCEPTION("Fake initialization failure");
            }
            return New<TTestState>(std::move(value), std::move(path));
        }

        std::vector<TFileProviderId> ids;
        ids.reserve(fileProviders->GetFileProviders().size());
        for (const auto& [id, _] : fileProviders->GetFileProviders()) {
            ids.push_back(id);
        }
        std::sort(ids.begin(), ids.end());

        std::string value;
        std::string firstPath;
        for (const auto& id : ids) {
            auto path = TFsPath(fileProviders->GetFileProvider(id)->GetRootPath()).Child("artifact").GetPath();
            auto input = TFileInput(TString(path));
            auto contents = input.ReadAll();
            if (!value.empty()) {
                value += ";";
            }
            value += Format("%v=%v", id, TStringBuf(contents));
            if (firstPath.empty()) {
                firstPath = path;
            }
        }

        {
            auto guard = Guard(Lock_);
            ++InitializeCounts_[value];
        }

        WaitForInitializationIfBlocked(value);

        if (value.contains("initialize-failure")) {
            THROW_ERROR_EXCEPTION("Fake initialization failure");
        }

        return New<TTestState>(std::move(value), std::move(firstPath));
    }

    void Validate(const TTestStatePtr& state) override
    {
        TFuture<void> gate = OKFuture;
        TPromise<void> started;
        bool notifyStarted = false;
        {
            auto guard = Guard(Lock_);
            if (BlockedValidationValue_ == state->Value) {
                gate = ValidationGate_.ToFuture();
                started = ValidationStarted_;
                notifyStarted = true;
            }
        }
        if (notifyStarted) {
            started.Set();
        }
        WaitFor(gate).ThrowOnError();

        if (state->Value.contains("validation-failure")) {
            THROW_ERROR_EXCEPTION("Fake validation failure");
        }
    }

private:
    static void WaitForInitializationIfBlocked(const std::string& value)
    {
        TFuture<void> gate = OKFuture;
        TPromise<void> started;
        bool notifyStarted = false;
        {
            auto guard = Guard(Lock_);
            if (BlockedInitializationValue_ == value) {
                gate = InitializationGate_.ToFuture();
                started = InitializationStarted_;
                notifyStarted = true;
            }
        }
        if (notifyStarted) {
            started.Set();
        }
        WaitFor(gate).ThrowOnError();
    }

    static NThreading::TSpinLock Lock_;
    static THashMap<std::string, int> InitializeCounts_;
    static std::string BlockedInitializationValue_;
    static TPromise<void> InitializationGate_;
    static TPromise<void> InitializationStarted_;
    static std::string BlockedValidationValue_;
    static TPromise<void> ValidationGate_;
    static TPromise<void> ValidationStarted_;
};

NThreading::TSpinLock TTestFileResource::Lock_;
THashMap<std::string, int> TTestFileResource::InitializeCounts_;
std::string TTestFileResource::BlockedInitializationValue_;
TPromise<void> TTestFileResource::InitializationGate_ = NewPromise<void>();
TPromise<void> TTestFileResource::InitializationStarted_ = NewPromise<void>();
std::string TTestFileResource::BlockedValidationValue_;
TPromise<void> TTestFileResource::ValidationGate_ = NewPromise<void>();
TPromise<void> TTestFileResource::ValidationStarted_ = NewPromise<void>();

using TTestFileResourcePtr = TIntrusivePtr<TTestFileResource>;

YT_FLOW_DEFINE_RESOURCE(TTestFileResource);

class TTestFileResourceWithDirectController
    : public TTestFileResource
{
public:
    using TController = TNullResourceController;
    using TTestFileResource::TTestFileResource;
};

YT_FLOW_DEFINE_RESOURCE(TTestFileResourceWithDirectController);

////////////////////////////////////////////////////////////////////////////////

TFileProviderSpecPtr MakeFileProviderSpec(
    std::string prefix,
    std::string fileProviderClassName = TypeName<TFakeFileProvider>())
{
    auto spec = New<TFileProviderSpec>();
    spec->FileProviderClassName = std::move(fileProviderClassName);
    spec->Parameters = ConvertTo<IMapNodePtr>(TYsonString(Format("{prefix=%Qv;}", prefix)));
    return spec;
}

TResourceSpecPtr MakeNamedResourceSpec(const THashMap<std::string, std::string>& fileProviders)
{
    auto spec = New<TResourceSpec>();
    spec->ResourceClassName = TypeName<TTestFileResource>();
    spec->Parameters = GetEphemeralNodeFactory()->CreateMap();
    for (const auto& [name, prefix] : fileProviders) {
        spec->FileProviders[TFileProviderId(name)] = MakeFileProviderSpec(prefix);
    }
    return spec;
}

TResourceSpecPtr MakeResourceSpec(
    std::string fileProviderClassName = TypeName<TFakeFileProvider>(),
    std::string prefix = "payload")
{
    auto spec = MakeNamedResourceSpec({{"file", std::move(prefix)}});
    spec->FileProviders.at(TFileProviderId("file"))->FileProviderClassName = std::move(fileProviderClassName);
    return spec;
}

TFileProviderRevisionPtr MakeProviderRevision(
    const std::string& contentId,
    std::string fileProviderClassName = TypeName<TFakeFileProvider>())
{
    auto revision = New<TFileProviderRevision>();
    revision->FileProviderClassName = std::move(fileProviderClassName);
    revision->ObjectId = NFileStorage::TFileStorageObjectId(contentId);
    revision->DisplayVersion = contentId;
    return revision;
}

TResourceRevisionPtr MakeNamedTarget(
    i64 deliveryRevisionId,
    const THashMap<std::string, std::string>& fileProviders)
{
    auto target = New<TResourceRevision>();
    target->RevisionId = deliveryRevisionId;
    target->ActiveFileSnapshot = New<TFileSnapshot>();
    target->ActiveFileSnapshot->Id = TFileSnapshotId(deliveryRevisionId);
    for (const auto& [name, contentId] : fileProviders) {
        target->ActiveFileSnapshot->FileProviders[TFileProviderId(name)] = MakeProviderRevision(contentId);
    }
    return target;
}

TFileSnapshotPtr MakeNamedFileSnapshot(
    i64 snapshotId,
    const THashMap<std::string, std::string>& fileProviders)
{
    auto fileSnapshot = New<TFileSnapshot>();
    fileSnapshot->Id = TFileSnapshotId(snapshotId);
    for (const auto& [name, contentId] : fileProviders) {
        fileSnapshot->FileProviders[TFileProviderId(name)] = MakeProviderRevision(contentId);
    }
    return fileSnapshot;
}

TFileSnapshotPtr MakeFileSnapshot(i64 snapshotId, const std::string& contentId)
{
    return MakeNamedFileSnapshot(snapshotId, {{"file", contentId}});
}

TResourceRevisionPtr MakeRolloutTarget(
    i64 deliveryRevisionId,
    TFileSnapshotPtr activeFileSnapshot,
    TFileSnapshotPtr preparingFileSnapshot = nullptr)
{
    auto target = New<TResourceRevision>();
    target->RevisionId = deliveryRevisionId;
    target->ActiveFileSnapshot = std::move(activeFileSnapshot);
    target->PreparingFileSnapshot = std::move(preparingFileSnapshot);
    return target;
}

TResourceRevisionPtr MakeTarget(i64 deliveryRevisionId, const std::string& contentId)
{
    return MakeNamedTarget(deliveryRevisionId, {{"file", contentId}});
}

TResourceRevisionPtr MakeMalformedTarget(i64 deliveryRevisionId)
{
    return MakeRolloutTarget(deliveryRevisionId, MakeNamedFileSnapshot(deliveryRevisionId, {}));
}

TResourceRevisionPtr MakeClassMismatchTarget(i64 deliveryRevisionId)
{
    auto target = MakeTarget(deliveryRevisionId, "mismatched");
    target->ActiveFileSnapshot->FileProviders.at(TFileProviderId("file"))->FileProviderClassName = "mismatched-provider";
    return target;
}

const TFileSnapshotPtr& GetLatestFileSnapshot(const TResourceRevisionPtr& target)
{
    return target->PreparingFileSnapshot
        ? target->PreparingFileSnapshot
        : target->ActiveFileSnapshot;
}

TDynamicFileProviderSpecPtr MakeDynamicFileProviderSpec(
    std::optional<std::string> pinnedContentId = std::nullopt)
{
    auto parameters = New<TFakeFileProviderDynamicParameters>();
    parameters->PinnedContentId = std::move(pinnedContentId);

    auto spec = New<TDynamicFileProviderSpec>();
    spec->Parameters = ConvertToNode(parameters)->AsMap();
    return spec;
}

TDynamicResourceContextPtr MakeNamedDynamicContext(
    TResourceRevisionPtr target = nullptr,
    TDuration discoverPeriod = TDuration::MilliSeconds(10),
    TDuration updateRetryPeriod = TDuration::MilliSeconds(100),
    const THashMap<std::string, std::string>& pinnedContentIds = {},
    TDuration fileSnapshotMinCreationPeriod = TDuration::MilliSeconds(1),
    i64 fileSnapshotCatalogMaxEntries = 1024,
    TDuration fileSnapshotRolloutWarningPeriod = TDuration::Minutes(15))
{
    auto context = New<TDynamicResourceContext>();
    context->DynamicResourceSpec = New<TDynamicResourceSpec>();
    context->DynamicResourceSpec->Parameters = GetEphemeralNodeFactory()->CreateMap();
    context->DynamicResourceSpec->FileProviderDiscoverPeriod = discoverPeriod;
    context->DynamicResourceSpec->FileProviderUpdateRetryPeriod = updateRetryPeriod;
    context->DynamicResourceSpec->FileSnapshotMinCreationPeriod = fileSnapshotMinCreationPeriod;
    context->DynamicResourceSpec->FileSnapshotCatalogMaxEntries = fileSnapshotCatalogMaxEntries;
    context->DynamicResourceSpec->FileSnapshotRolloutWarningPeriod = fileSnapshotRolloutWarningPeriod;
    for (const auto& [name, contentId] : pinnedContentIds) {
        context->DynamicResourceSpec->FileProviders[TFileProviderId(name)] =
            MakeDynamicFileProviderSpec(contentId);
    }
    context->TargetRevision = std::move(target);
    return context;
}

TDynamicResourceContextPtr MakeDynamicContext(
    TResourceRevisionPtr target = nullptr,
    i64 updateRetryPeriod = 100)
{
    return MakeNamedDynamicContext(
        std::move(target),
        TDuration::MilliSeconds(10),
        TDuration::MilliSeconds(updateRetryPeriod));
}

class TTestResourceController
    : public TResourceControllerBase
{
public:
    TTestResourceController(
        TResourceControllerContextPtr context,
        TDynamicResourceControllerContextPtr dynamicContext,
        INodePtr targetSpec)
        : TResourceControllerBase(std::move(context), std::move(dynamicContext))
        , TargetSpec_(std::move(targetSpec))
    { }

protected:
    INodePtr DoBuildTargetRevisionSpec() override
    {
        return TargetSpec_;
    }

private:
    const INodePtr TargetSpec_;
};

TIntrusivePtr<TResourceControllerBase> MakeNamedController(
    const IInvokerPtr& invoker,
    IStatusProfilerPtr statusProfiler,
    const THashMap<std::string, std::string>& fileProviders,
    TDuration discoverPeriod = TDuration::MilliSeconds(10),
    NProfiling::TProfiler profiler = {},
    const THashMap<std::string, std::string>& pinnedContentIds = {},
    TDuration fileSnapshotMinCreationPeriod = TDuration::MilliSeconds(1),
    i64 fileSnapshotCatalogMaxEntries = 1024,
    TDuration fileSnapshotRolloutWarningPeriod = TDuration::Minutes(15),
    INodePtr targetSpec = nullptr)
{
    auto context = New<TResourceControllerContext>();
    context->ResourceId = TResourceId("test");
    context->ResourceSpec = MakeNamedResourceSpec(fileProviders);
    context->Invoker = invoker;
    static const auto timeProvider = New<TFakeTimeProvider>();
    context->TimeProvider = timeProvider;
    context->Logger = NLogging::TLogger("ResourceControllerTest");
    context->StatusProfiler = std::move(statusProfiler);
    context->Profiler = std::move(profiler);

    auto dynamicContext = New<TDynamicResourceControllerContext>();
    dynamicContext->DynamicResourceSpec = MakeNamedDynamicContext(
        nullptr,
        discoverPeriod,
        TDuration::MilliSeconds(100),
        pinnedContentIds,
        fileSnapshotMinCreationPeriod,
        fileSnapshotCatalogMaxEntries,
        fileSnapshotRolloutWarningPeriod)
        ->DynamicResourceSpec;
    return New<TTestResourceController>(
        std::move(context),
        std::move(dynamicContext),
        std::move(targetSpec));
}

TTestFileResourcePtr MakeResource(
    const IInvokerPtr& invoker,
    TResourceRevisionPtr target = nullptr,
    NFileStorage::IFileStoragePtr fileStorage = New<TFakeFileStorage>(),
    IStatusProfilerPtr statusProfiler = CreateSyncStatusProfiler(),
    i64 updateRetryPeriod = 100)
{
    auto context = New<TResourceContext>();
    context->ResourceId = TResourceId("test");
    context->ResourceSpec = MakeResourceSpec();
    context->Invoker = invoker;
    context->Logger = NLogging::TLogger("FileResourceTest");
    context->StatusProfiler = std::move(statusProfiler);
    context->FileStorage = std::move(fileStorage);

    return TRegistry::Get()
        ->CreateResource(
            context,
            MakeDynamicContext(std::move(target), updateRetryPeriod))
        ->As<TTestFileResource>();
}

TTestFileResourcePtr MakePostprocessedResource(
    const IInvokerPtr& invoker,
    TResourceRevisionPtr target,
    std::string command,
    TDuration timeout = TDuration::Minutes(1),
    NFileStorage::IFileStoragePtr fileStorage = New<TFakeFileStorage>(),
    IStatusProfilerPtr statusProfiler = CreateSyncStatusProfiler(),
    TDuration updateRetryPeriod = TDuration::MilliSeconds(100))
{
    auto context = New<TResourceContext>();
    context->ResourceId = TResourceId("test");
    context->ResourceSpec = MakeResourceSpec();
    context->ResourceSpec->FileProviders.at(TFileProviderId("file"))->PostprocessCommand = std::move(command);
    context->ResourceSpec->FileProviders.at(TFileProviderId("file"))->PostprocessTimeout = timeout;
    context->Invoker = invoker;
    context->Logger = NLogging::TLogger("PostprocessedFileResourceTest");
    context->StatusProfiler = std::move(statusProfiler);
    context->FileStorage = std::move(fileStorage);

    return TRegistry::Get()
        ->CreateResource(
            context,
            MakeNamedDynamicContext(
                std::move(target),
                TDuration::MilliSeconds(10),
                updateRetryPeriod))
        ->As<TTestFileResource>();
}

const TIncarnationId DefaultWorkerIncarnationId(TGuid::Create());

TWorkerStatusPtr MakeWorkerStatus(
    const TWorkerResourceStatusPtr& resourceStatus,
    TIncarnationId workerIncarnationId = DefaultWorkerIncarnationId)
{
    auto workerStatus = New<TWorkerStatus>();
    workerStatus->WorkerIncarnationId = workerIncarnationId;
    workerStatus->ResourceStatuses[TResourceId("test")] = resourceStatus;
    return workerStatus;
}

TTestFileResourcePtr MakeNamedResource(
    const IInvokerPtr& invoker,
    const THashMap<std::string, std::string>& fileProviders,
    TResourceRevisionPtr target = nullptr,
    NFileStorage::IFileStoragePtr fileStorage = New<TFakeFileStorage>(),
    IStatusProfilerPtr statusProfiler = CreateSyncStatusProfiler(),
    TDuration updateRetryPeriod = TDuration::MilliSeconds(100))
{
    auto context = New<TResourceContext>();
    context->ResourceId = TResourceId("test");
    context->ResourceSpec = MakeNamedResourceSpec(fileProviders);
    context->Invoker = invoker;
    context->Logger = NLogging::TLogger("NamedFileResourceTest");
    context->StatusProfiler = std::move(statusProfiler);
    context->FileStorage = std::move(fileStorage);

    return TRegistry::Get()
        ->CreateResource(
            context,
            MakeNamedDynamicContext(std::move(target), TDuration::MilliSeconds(10), updateRetryPeriod))
        ->As<TTestFileResource>();
}

void WaitForAppliedRevision(const TTestFileResourcePtr& resource, i64 revisionId)
{
    WaitForPredicate(
        [&] {
            return resource->GetRevisionState().AppliedRevisionId == revisionId;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(10),
            .Message = Format("File resource did not apply revision %v", revisionId),
        });
}

void WaitForPreparingState(
    const TTestFileResourcePtr& resource,
    TFileSnapshotId snapshotId,
    EFileSnapshotState expectedState,
    std::optional<EFileSnapshotPreparationStage> expectedStage = std::nullopt)
{
    WaitForPredicate(
        [&] {
            auto state = resource->GetRevisionState();
            return state.PreparingFileSnapshot &&
                state.PreparingFileSnapshot->SnapshotId == snapshotId &&
                state.PreparingFileSnapshot->State == expectedState &&
                state.PreparingFileSnapshot->PreparationStage == expectedStage;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(10),
            .Message = Format("Snapshot %v did not reach %v/%v", snapshotId, expectedState, expectedStage),
        });
}

class TFileResourceTest
    : public ::testing::Test
{
protected:
    void SetUp() override
    {
        TFakeFileProvider::Reset();
        TTestFileResource::Reset();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TFileResourceTest, RegistryValidatesProvider)
{
    EXPECT_NO_THROW(TRegistry::Get()->ValidateResourceSpec(MakeResourceSpec()));

    EXPECT_THROW_WITH_SUBSTRING(
        TRegistry::Get()->ValidateResourceSpec(MakeResourceSpec("missing-provider")),
        "file provider");
    EXPECT_THROW_WITH_SUBSTRING(
        TRegistry::Get()->ValidateResourceSpec(MakeResourceSpec(TypeName<TFakeFileProvider>(), "")),
        "prefix");
}

TEST_F(TFileResourceTest, FileProviderPostprocessSpecValidation)
{
    auto parsed = ConvertTo<TFileProviderSpecPtr>(TYsonString(Format("{file_provider_class_name=%Qv;parameters={prefix=payload;};postprocess_command=\"/bin/true\";}",
        TypeName<TFakeFileProvider>())));
    EXPECT_EQ(parsed->PostprocessCommand, "/bin/true");
    EXPECT_EQ(parsed->PostprocessTimeout, TDuration::Minutes(1));

    auto withoutCommand = ConvertTo<TFileProviderSpecPtr>(TYsonString(Format("{file_provider_class_name=%Qv;parameters={prefix=payload;};postprocess_timeout=\"2m\";}",
        TypeName<TFakeFileProvider>())));
    EXPECT_FALSE(withoutCommand->PostprocessCommand);
    EXPECT_EQ(withoutCommand->PostprocessTimeout, TDuration::Minutes(2));

    EXPECT_THROW_WITH_SUBSTRING(
        ConvertTo<TFileProviderSpecPtr>(TYsonString(Format("{file_provider_class_name=%Qv;parameters={prefix=payload;};postprocess_command=\"\";}",
        TypeName<TFakeFileProvider>()))),
        "must be nonempty");
    EXPECT_THROW_WITH_SUBSTRING(
        ConvertTo<TFileProviderSpecPtr>(TYsonString(Format("{file_provider_class_name=%Qv;parameters={prefix=payload;};postprocess_timeout=0;}",
        TypeName<TFakeFileProvider>()))),
        "Expected >");
}

TEST_F(TFileResourceTest, RawAndProcessedCacheIdentitiesDoNotAlias)
{
    auto queue = New<TActionQueue>();
    auto storage = New<TFakeFileStorage>();
    const std::string command = R"(
/bin/cat "$YT_FLOW_RESOURCE_PATH/artifact" > "$YT_FLOW_POSTPROCESSING_PATH/artifact"
)";
    auto processed = MakePostprocessedResource(
        queue->GetInvoker(),
        MakeTarget(1, "raw"),
        command,
        TDuration::Seconds(5),
        storage);
    WaitFor(processed->Load({}).WithTimeout(TDuration::Seconds(5))).ThrowOnError();
    EXPECT_EQ(processed->Lock()->Value, "payload:raw");
    processed.Reset();

    NCrypto::TSha256Hasher commandHasher;
    commandHasher.Append(command);
    NCrypto::TSha256Hasher identityHasher;
    identityHasher.Append(Format("%Qv-%Qv-%Qv",
        TStringBuf("test"),
        TStringBuf("file"),
        TStringBuf("raw")));
    auto collidingObjectId = Format("test-file-raw-postprocess-%v-%v",
        commandHasher.GetHexDigestLowerCase(),
        identityHasher.GetHexDigestLowerCase());
    auto raw = MakeResource(
        queue->GetInvoker(),
        MakeTarget(2, collidingObjectId),
        storage);

    WaitFor(raw->Load({}).WithTimeout(TDuration::Seconds(5))).ThrowOnError();
    EXPECT_EQ(raw->Lock()->Value, Format("payload:%v", collidingObjectId));
    EXPECT_EQ(TFakeFileProvider::GetDownloadCount(collidingObjectId), 1);
}

TEST_F(TFileResourceTest, RegistryRejectsNamedProvidersForDirectResourceController)
{
    auto spec = MakeNamedResourceSpec({{"file", "payload"}});
    spec->ResourceClassName = TypeName<TTestFileResourceWithDirectController>();

    EXPECT_THROW_WITH_SUBSTRING(
        TRegistry::Get()->ValidateResourceSpec(spec),
        "does not support file provider discovery");
}

TEST_F(TFileResourceTest, RegistryValidatesNamedProviders)
{
    auto parsed = ConvertTo<TResourceSpecPtr>(TYsonString(Format("{resource_class_name=%Qv;parameters={};file_providers={"
        "left={file_provider_class_name=%Qv;parameters={prefix=left;};};"
        "right={file_provider_class_name=%Qv;parameters={prefix=right;};};};}",
        TypeName<TTestFileResource>(),
        TypeName<TFakeFileProvider>(),
        TypeName<TFakeFileProvider>())));
    EXPECT_EQ(parsed->FileProviders.size(), 2);
    EXPECT_NO_THROW(TRegistry::Get()->ValidateResourceSpec(parsed));

    EXPECT_NO_THROW(TRegistry::Get()->ValidateResourceSpec(
        MakeNamedResourceSpec({{"left", "left"}, {"right", "right"}})));

    auto missingClass = MakeNamedResourceSpec({{"left", "left"}});
    missingClass->FileProviders[TFileProviderId("left")]->FileProviderClassName = "missing-provider";
    EXPECT_THROW_WITH_SUBSTRING(
        TRegistry::Get()->ValidateResourceSpec(missingClass),
        "file provider");

    auto emptyParameters = MakeNamedResourceSpec({{"left", ""}});
    EXPECT_THROW_WITH_SUBSTRING(
        TRegistry::Get()->ValidateResourceSpec(emptyParameters),
        "prefix");

    auto invalidName = MakeNamedResourceSpec({{"../left", "left"}});
    auto invalidPipelineSpec = New<TPipelineSpec>();
    invalidPipelineSpec->Resources[TResourceId("resource")] = invalidName;
    EXPECT_THROW_WITH_SUBSTRING(
        ValidatePipelineSpec(invalidPipelineSpec),
        "single normal path component");

    EXPECT_THROW_WITH_SUBSTRING(
        TRegistry::Get()->ValidateResourceSpec(MakeNamedResourceSpec({})),
        "at least one file provider");
}

TEST_F(TFileResourceTest, RegistryValidatesDynamicNamedProviders)
{
    auto pipelineSpec = New<TPipelineSpec>();
    pipelineSpec->Resources[TResourceId("resource")] =
        MakeNamedResourceSpec({{"left", "left"}});

    auto validate = [&] (TStringBuf dynamicSpec) {
        return TRegistry::Get()->ValidateDynamicPipelineSpecParseability(
            pipelineSpec,
            ConvertTo<IMapNodePtr>(TYsonString(dynamicSpec)));
    };

    EXPECT_TRUE(validate(R"({resources={resource={file_providers={left={parameters={pinned_content_id=left-v2;};};};};};})").empty());

    auto unknownProviderErrors = validate(
        R"({resources={resource={file_providers={right={parameters={};};};};};})");
    ASSERT_FALSE(unknownProviderErrors.empty());
    EXPECT_THAT(ToString(unknownProviderErrors[0]), ::testing::HasSubstr("does not exist in static spec"));

    auto invalidParametersErrors = validate(
        R"({resources={resource={file_providers={left={parameters={pinned_content_id="";};};};};};})");
    ASSERT_FALSE(invalidParametersErrors.empty());
    EXPECT_THAT(ToString(invalidParametersErrors[0]), ::testing::HasSubstr("must be nonempty"));

    auto unrecognizedParametersErrors = validate(
        R"({resources={resource={file_providers={left={parameters={unknown=1;};};};};};})");
    ASSERT_FALSE(unrecognizedParametersErrors.empty());
    EXPECT_THAT(ToString(unrecognizedParametersErrors[0]), ::testing::HasSubstr("unknown"));

    auto invalidDynamicPipelineSpec = New<TDynamicPipelineSpec>();
    auto invalidDynamicResourceSpec = New<TDynamicResourceSpec>();
    invalidDynamicResourceSpec->FileProviders[TFileProviderId("../left")] = New<TDynamicFileProviderSpec>();
    invalidDynamicPipelineSpec->Resources[TResourceId("resource")] = invalidDynamicResourceSpec;
    EXPECT_THROW_WITH_SUBSTRING(
        ValidateDynamicPipelineSpec(invalidDynamicPipelineSpec),
        "single normal path component");
}

TEST_F(TFileResourceTest, FileSnapshotProtocolRoundTrips)
{
    auto revision = New<TResourceRevision>();
    revision->RevisionId = 17;
    revision->ActiveFileSnapshot = New<TFileSnapshot>();
    revision->ActiveFileSnapshot->Id = TFileSnapshotId(3);
    revision->ActiveFileSnapshot->FileProviders[TFileProviderId("left")] = MakeProviderRevision("left-v1");
    revision->PreparingFileSnapshot = New<TFileSnapshot>();
    revision->PreparingFileSnapshot->Id = TFileSnapshotId(4);
    revision->PreparingFileSnapshot->FileProviders[TFileProviderId("left")] = MakeProviderRevision("left-v2");

    auto roundTrippedRevision = ConvertTo<TResourceRevisionPtr>(ConvertToNode(revision));
    ASSERT_TRUE(roundTrippedRevision->ActiveFileSnapshot);
    ASSERT_TRUE(roundTrippedRevision->PreparingFileSnapshot);
    EXPECT_EQ(roundTrippedRevision->ActiveFileSnapshot->Id, TFileSnapshotId(3));
    EXPECT_EQ(roundTrippedRevision->PreparingFileSnapshot->Id, TFileSnapshotId(4));

    auto workerStatus = New<TWorkerStatus>();
    const auto workerIncarnationId = TIncarnationId(TGuid::Create());
    workerStatus->WorkerIncarnationId = workerIncarnationId;
    auto resourceStatus = New<TWorkerResourceStatus>();
    resourceStatus->ResourceInstanceId = TResourceInstanceId(TGuid::Create());
    resourceStatus->ResourceIncarnationGeneration = 5;
    resourceStatus->ActiveFileSnapshotId = TFileSnapshotId(3);
    resourceStatus->PreparingFileSnapshot = New<TFileSnapshotStatus>();
    resourceStatus->PreparingFileSnapshot->SnapshotId = TFileSnapshotId(4);
    resourceStatus->PreparingFileSnapshot->State = EFileSnapshotState::Preparing;
    resourceStatus->PreparingFileSnapshot->PreparationStage = EFileSnapshotPreparationStage::Validating;
    resourceStatus->LiveAccessorCounts[TFileSnapshotId(3)] = 2;
    resourceStatus->LiveAccessorCounts[TFileSnapshotId(4)] = 1;
    workerStatus->ResourceStatuses[TResourceId("resource")] = std::move(resourceStatus);

    auto roundTrippedStatus = ConvertTo<TWorkerStatusPtr>(ConvertToNode(workerStatus));
    ASSERT_TRUE(roundTrippedStatus->WorkerIncarnationId);
    const auto& roundTrippedResourceStatus = GetOrCrash(
        roundTrippedStatus->ResourceStatuses,
        TResourceId("resource"));
    EXPECT_EQ(roundTrippedResourceStatus->ResourceIncarnationGeneration, 5u);
    EXPECT_EQ(roundTrippedResourceStatus->ActiveFileSnapshotId, TFileSnapshotId(3));
    EXPECT_EQ(roundTrippedResourceStatus->PreparingFileSnapshot->SnapshotId, TFileSnapshotId(4));
    EXPECT_EQ(roundTrippedResourceStatus->PreparingFileSnapshot->State, EFileSnapshotState::Preparing);
    EXPECT_EQ(
        roundTrippedResourceStatus->PreparingFileSnapshot->PreparationStage,
        EFileSnapshotPreparationStage::Validating);
    EXPECT_EQ(roundTrippedResourceStatus->LiveAccessorCounts.at(TFileSnapshotId(3)), 2);
    EXPECT_EQ(roundTrippedResourceStatus->LiveAccessorCounts.at(TFileSnapshotId(4)), 1);
    auto dynamicSpec = New<TDynamicResourceSpec>();
    auto roundTrippedDynamicSpec = ConvertTo<TDynamicResourceSpecPtr>(ConvertToNode(dynamicSpec));
    EXPECT_EQ(roundTrippedDynamicSpec->FileSnapshotMinCreationPeriod, TDuration::Minutes(5));
    EXPECT_EQ(roundTrippedDynamicSpec->FileSnapshotCatalogMaxEntries, 1024);
    EXPECT_EQ(roundTrippedDynamicSpec->FileSnapshotRolloutWarningPeriod, TDuration::Minutes(15));
}

TEST_F(TFileResourceTest, NamedControllerPublishesOnlyCompleteSnapshots)
{
    TFakeFileProvider::PushDiscoveryRevision("left-v1", "left");

    auto queue = New<TActionQueue>();
    auto statusProfiler = CreateSyncStatusProfiler();
    auto controller = MakeNamedController(
        queue->GetInvoker(),
        statusProfiler,
        {{"left", "left"}, {"right", "right"}},
        TDuration::MilliSeconds(1));
    controller->Init(nullptr);

    WaitForPredicate(
        [] {
            return TFakeFileProvider::GetDiscoverCount("left") > 0 &&
                TFakeFileProvider::GetDiscoverCount("right") > 0;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    EXPECT_FALSE(controller->BuildTargetRevision());
    EXPECT_TRUE(statusProfiler->GetStatus().Errors.contains("/file_providers/right/discovery"));

    TFakeFileProvider::PushDiscoveryRevision("right-v1", "right");
    WaitForPredicate(
        [&] {
            return static_cast<bool>(controller->BuildTargetRevision());
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });

    const auto target = controller->BuildTargetRevision();
    EXPECT_FALSE(target->ActiveFileSnapshot);
    ASSERT_TRUE(target->PreparingFileSnapshot);
    EXPECT_EQ(target->PreparingFileSnapshot->FileProviders.at(TFileProviderId("left"))->ObjectId.Underlying(), "left-v1");
    EXPECT_EQ(target->PreparingFileSnapshot->FileProviders.at(TFileProviderId("right"))->ObjectId.Underlying(), "right-v1");
    EXPECT_FALSE(statusProfiler->GetStatus().Errors.contains("/file_providers/right/discovery"));

    TFakeFileProvider::PushDiscoveryRevision("left-v2", "left");
    WaitForPredicate(
        [&] {
            auto updated = controller->BuildTargetRevision();
            return updated &&
                GetLatestFileSnapshot(updated)->FileProviders.at(TFileProviderId("left"))->ObjectId.Underlying() == "left-v2";
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    EXPECT_EQ(
        GetLatestFileSnapshot(controller->BuildTargetRevision())->FileProviders.at(TFileProviderId("right"))->ObjectId.Underlying(),
        "right-v1");
}

TEST_F(TFileResourceTest, NamedControllerKeepsOwnSpecWhenDiscoveryFails)
{
    TFakeFileProvider::SetDiscoveryError("file");

    auto queue = New<TActionQueue>();
    auto controller = MakeNamedController(
        queue->GetInvoker(),
        CreateSyncStatusProfiler(),
        {{"file", "file"}},
        TDuration::MilliSeconds(1),
        {},
        {},
        TDuration::MilliSeconds(1),
        1024,
        TDuration::Minutes(15),
        ConvertToNode("controller-spec"));
    controller->Init(nullptr);

    WaitForPredicate([] {
        return TFakeFileProvider::GetDiscoverCount("file") > 0;
    });

    auto target = controller->BuildTargetRevision();
    ASSERT_TRUE(target);
    EXPECT_EQ(ConvertTo<std::string>(target->Spec), "controller-spec");
    EXPECT_FALSE(target->ActiveFileSnapshot);
    EXPECT_FALSE(target->PreparingFileSnapshot);
}

TEST_F(TFileResourceTest, NamedControllerPromotesOnlyAuthoritativeCurrentValidatedSnapshot)
{
    TFakeFileProvider::PushDiscoveryRevision("v1", "file");

    auto queue = New<TActionQueue>();
    auto stateManager = New<TStateManagerMock>();
    auto controller = MakeNamedController(
        queue->GetInvoker(),
        CreateSyncStatusProfiler(),
        {{"file", "file"}},
        TDuration::MilliSeconds(1));
    controller->Init(stateManager->CreateContext());

    TResourceRevisionPtr target;
    WaitForPredicate(
        [&] {
            target = controller->BuildTargetRevision();
            return target && target->PreparingFileSnapshot;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    const auto snapshotId = target->PreparingFileSnapshot->Id;

    const auto workerIncarnationId = TIncarnationId(TGuid::Create());
    const auto currentResourceInstanceId = TResourceInstanceId(TGuid::Create());
    auto currentStatus = New<TWorkerResourceStatus>();
    currentStatus->TargetRevisionId = 17;
    currentStatus->ResourceInstanceId = currentResourceInstanceId;
    currentStatus->ResourceIncarnationGeneration = 2;
    currentStatus->PreparingFileSnapshot = New<TFileSnapshotStatus>();
    currentStatus->PreparingFileSnapshot->SnapshotId = snapshotId;
    currentStatus->PreparingFileSnapshot->State = EFileSnapshotState::Preparing;
    currentStatus->PreparingFileSnapshot->PreparationStage = EFileSnapshotPreparationStage::Validating;
    controller->CollectStatuses({{"worker", MakeWorkerStatus(currentStatus, workerIncarnationId)}}, nullptr, 17);
    EXPECT_TRUE(controller->BuildTargetRevision()->PreparingFileSnapshot);

    auto staleIncarnationStatus = CloneYsonStruct(currentStatus);
    staleIncarnationStatus->ResourceInstanceId = TResourceInstanceId(TGuid::Create());
    staleIncarnationStatus->ResourceIncarnationGeneration = 1;
    staleIncarnationStatus->PreparingFileSnapshot->State = EFileSnapshotState::Validated;
    controller->CollectStatuses({{"worker", MakeWorkerStatus(staleIncarnationStatus, workerIncarnationId)}}, nullptr, 17);
    EXPECT_TRUE(controller->BuildTargetRevision()->PreparingFileSnapshot);

    auto staleRevisionStatus = CloneYsonStruct(currentStatus);
    staleRevisionStatus->TargetRevisionId = 16;
    staleRevisionStatus->PreparingFileSnapshot->State = EFileSnapshotState::Validated;
    controller->CollectStatuses({{"worker", MakeWorkerStatus(staleRevisionStatus, workerIncarnationId)}}, nullptr, 17);
    EXPECT_TRUE(controller->BuildTargetRevision()->PreparingFileSnapshot);

    currentStatus->PreparingFileSnapshot->State = EFileSnapshotState::Validated;
    controller->CollectStatuses({{"worker", MakeWorkerStatus(currentStatus, workerIncarnationId)}}, nullptr, 17);
    target = controller->BuildTargetRevision();
    ASSERT_TRUE(target->ActiveFileSnapshot);
    EXPECT_EQ(target->ActiveFileSnapshot->Id, snapshotId);
    EXPECT_FALSE(target->PreparingFileSnapshot);

    WaitFor(BIND([] {
    }).AsyncVia(queue->GetInvoker())
            .Run())
        .ThrowOnError();
    stateManager->Sync();
    auto restoredStateManager = New<TStateManagerMock>();
    restoredStateManager->SetStorage(stateManager->GetStorage());
    controller.Reset();
    TFakeFileProvider::SetDiscoveryError("file");
    auto restored = MakeNamedController(
        queue->GetInvoker(),
        CreateSyncStatusProfiler(),
        {{"file", "file"}},
        TDuration::Hours(1));
    restored->Init(restoredStateManager->CreateContext());
    target = restored->BuildTargetRevision();
    ASSERT_TRUE(target);
    ASSERT_TRUE(target->ActiveFileSnapshot);
    EXPECT_EQ(target->ActiveFileSnapshot->Id, snapshotId);
    EXPECT_FALSE(target->PreparingFileSnapshot);
}

TEST_F(TFileResourceTest, NamedControllerRateLimitsPreparingSnapshotReplacement)
{
    TFakeFileProvider::PushDiscoveryRevision("v1", "file");

    auto queue = New<TActionQueue>();
    auto controller = MakeNamedController(
        queue->GetInvoker(),
        CreateSyncStatusProfiler(),
        {{"file", "file"}},
        TDuration::MilliSeconds(1),
        {},
        {},
        TDuration::Hours(1));
    controller->Init(nullptr);

    TResourceRevisionPtr target;
    WaitForPredicate(
        [&] {
            target = controller->BuildTargetRevision();
            return target && target->PreparingFileSnapshot;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    const auto firstSnapshotId = target->PreparingFileSnapshot->Id;

    const auto discoverCount = TFakeFileProvider::GetDiscoverCount("file");
    TFakeFileProvider::PushDiscoveryRevision("v2", "file");
    WaitForPredicate(
        [&] {
            return TFakeFileProvider::GetDiscoverCount("file") >= discoverCount + 2;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });

    target = controller->BuildTargetRevision();
    ASSERT_TRUE(target->PreparingFileSnapshot);
    EXPECT_EQ(target->PreparingFileSnapshot->Id, firstSnapshotId);
    EXPECT_EQ(
        target->PreparingFileSnapshot->FileProviders.at(TFileProviderId("file"))->ObjectId.Underlying(),
        "v1");

    auto dynamicContext = New<TDynamicResourceControllerContext>();
    dynamicContext->DynamicResourceSpec = MakeNamedDynamicContext(
        nullptr,
        TDuration::MilliSeconds(1),
        TDuration::MilliSeconds(100),
        {},
        TDuration::MilliSeconds(1))
        ->DynamicResourceSpec;
    controller->Reconfigure(dynamicContext);

    WaitForPredicate(
        [&] {
            target = controller->BuildTargetRevision();
            return target &&
                target->PreparingFileSnapshot &&
                target->PreparingFileSnapshot->Id != firstSnapshotId;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    EXPECT_EQ(
        target->PreparingFileSnapshot->FileProviders.at(TFileProviderId("file"))->ObjectId.Underlying(),
        "v2");
}

TEST_F(TFileResourceTest, NamedControllerBoundsSnapshotCatalogAndKeepsCurrentSlots)
{
    TFakeFileProvider::PushDiscoveryRevision("v1", "file");

    auto queue = New<TActionQueue>();
    auto stateManager = New<TStateManagerMock>();
    auto controller = MakeNamedController(
        queue->GetInvoker(),
        CreateSyncStatusProfiler(),
        {{"file", "file"}},
        TDuration::MilliSeconds(1),
        {},
        {},
        TDuration::MilliSeconds(1),
        2);
    controller->Init(stateManager->CreateContext());

    TResourceRevisionPtr target;
    WaitForPredicate(
        [&] {
            target = controller->BuildTargetRevision();
            return target && target->PreparingFileSnapshot;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });

    auto status = New<TWorkerResourceStatus>();
    status->ResourceInstanceId = TResourceInstanceId(TGuid::Create());
    status->ResourceIncarnationGeneration = 1;
    status->TargetRevisionId = 17;
    status->PreparingFileSnapshot = New<TFileSnapshotStatus>();
    status->PreparingFileSnapshot->SnapshotId = target->PreparingFileSnapshot->Id;
    status->PreparingFileSnapshot->State = EFileSnapshotState::Validated;
    controller->CollectStatuses({{"worker", MakeWorkerStatus(status)}}, nullptr, 17);

    target = controller->BuildTargetRevision();
    ASSERT_TRUE(target->ActiveFileSnapshot);
    EXPECT_FALSE(target->PreparingFileSnapshot);
    const auto activeId = target->ActiveFileSnapshot->Id;

    TFakeFileProvider::PushDiscoveryRevision("v2", "file");
    WaitForPredicate(
        [&] {
            target = controller->BuildTargetRevision();
            return target &&
                target->PreparingFileSnapshot &&
                target->PreparingFileSnapshot->FileProviders.at(TFileProviderId("file"))->ObjectId.Underlying() == "v2";
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    const auto supersededPreparingId = target->PreparingFileSnapshot->Id;

    TFakeFileProvider::PushDiscoveryRevision("v3", "file");
    WaitForPredicate(
        [&] {
            target = controller->BuildTargetRevision();
            return target &&
                target->PreparingFileSnapshot &&
                target->PreparingFileSnapshot->Id != supersededPreparingId;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });

    EXPECT_EQ(target->ActiveFileSnapshot->Id, activeId);
    EXPECT_EQ(
        target->PreparingFileSnapshot->FileProviders.at(TFileProviderId("file"))->ObjectId.Underlying(),
        "v3");
    auto view = controller->GetView()->GetChildOrThrow("file_providers")->AsMap();
    EXPECT_EQ(view->GetChildValueOrThrow<i64>("known_file_snapshot_count"), 2);
    EXPECT_EQ(view->GetChildValueOrThrow<TFileSnapshotId>("active_file_snapshot_id"), activeId);
    EXPECT_EQ(
        view->GetChildValueOrThrow<TFileSnapshotId>("preparing_file_snapshot_id"),
        target->PreparingFileSnapshot->Id);

    WaitFor(BIND([] {
    }).AsyncVia(queue->GetInvoker())
            .Run())
        .ThrowOnError();
    stateManager->Sync();
    auto restoredStateManager = New<TStateManagerMock>();
    restoredStateManager->SetStorage(stateManager->GetStorage());
    controller.Reset();
    TFakeFileProvider::SetDiscoveryError("file");
    auto restored = MakeNamedController(
        queue->GetInvoker(),
        CreateSyncStatusProfiler(),
        {{"file", "file"}},
        TDuration::Hours(1),
        {},
        {},
        TDuration::MilliSeconds(1),
        2);
    restored->Init(restoredStateManager->CreateContext());
    target = restored->BuildTargetRevision();
    ASSERT_TRUE(target);
    ASSERT_TRUE(target->ActiveFileSnapshot);
    ASSERT_TRUE(target->PreparingFileSnapshot);
    EXPECT_EQ(target->ActiveFileSnapshot->Id, activeId);
    EXPECT_EQ(restored->GetView()
            ->GetChildOrThrow("file_providers")
            ->AsMap()
            ->GetChildValueOrThrow<i64>("known_file_snapshot_count"),
        2);
}

TEST_F(TFileResourceTest, NamedControllerDropsPreparingWhenDiscoveryReturnsToActive)
{
    TFakeFileProvider::PushDiscoveryRevision("v1", "file");

    auto queue = New<TActionQueue>();
    auto controller = MakeNamedController(
        queue->GetInvoker(),
        CreateSyncStatusProfiler(),
        {{"file", "file"}},
        TDuration::MilliSeconds(1));
    controller->Init(nullptr);

    TResourceRevisionPtr target;
    WaitForPredicate(
        [&] {
            target = controller->BuildTargetRevision();
            return target && target->PreparingFileSnapshot;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });

    auto status = New<TWorkerResourceStatus>();
    status->ResourceInstanceId = TResourceInstanceId(TGuid::Create());
    status->ResourceIncarnationGeneration = 1;
    status->TargetRevisionId = 17;
    status->PreparingFileSnapshot = New<TFileSnapshotStatus>();
    status->PreparingFileSnapshot->SnapshotId = target->PreparingFileSnapshot->Id;
    status->PreparingFileSnapshot->State = EFileSnapshotState::Validated;
    controller->CollectStatuses({{"worker", MakeWorkerStatus(status)}}, nullptr, 17);
    ASSERT_TRUE(controller->BuildTargetRevision()->ActiveFileSnapshot);

    TFakeFileProvider::PushDiscoveryRevision("v2", "file");
    WaitForPredicate(
        [&] {
            target = controller->BuildTargetRevision();
            return target && target->PreparingFileSnapshot;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });

    TFakeFileProvider::PushDiscoveryRevision("v1", "file");
    WaitForPredicate(
        [&] {
            target = controller->BuildTargetRevision();
            return target && target->ActiveFileSnapshot && !target->PreparingFileSnapshot;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    EXPECT_EQ(
        target->ActiveFileSnapshot->FileProviders.at(TFileProviderId("file"))->ObjectId.Underlying(),
        "v1");
}

TEST_F(TFileResourceTest, NamedControllerRetainsLastCompleteSnapshotAcrossFailures)
{
    TFakeFileProvider::PushDiscoveryRevision("left-v1", "left");
    TFakeFileProvider::PushDiscoveryRevision("right-v1", "right");

    auto queue = New<TActionQueue>();
    auto statusProfiler = CreateSyncStatusProfiler();
    auto controller = MakeNamedController(
        queue->GetInvoker(),
        statusProfiler,
        {{"left", "left"}, {"right", "right"}},
        TDuration::MilliSeconds(1));
    controller->Init(nullptr);
    WaitForPredicate(
        [&] {
            return static_cast<bool>(controller->BuildTargetRevision());
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });

    auto discoverCount = TFakeFileProvider::GetDiscoverCount("right");
    TFakeFileProvider::PushNullDiscovery("right");
    WaitForPredicate(
        [&] {
            return TFakeFileProvider::GetDiscoverCount("right") >= discoverCount + 2;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    EXPECT_EQ(
        GetLatestFileSnapshot(controller->BuildTargetRevision())->FileProviders.at(TFileProviderId("right"))->ObjectId.Underlying(),
        "right-v1");
    EXPECT_FALSE(statusProfiler->GetStatus().Errors.contains("/file_providers/right/discovery"));

    TFakeFileProvider::SetDiscoveryError("right");
    WaitForPredicate(
        [&] {
            return statusProfiler->GetStatus().Errors.contains("/file_providers/right/discovery");
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    EXPECT_EQ(
        GetLatestFileSnapshot(controller->BuildTargetRevision())->FileProviders.at(TFileProviderId("right"))->ObjectId.Underlying(),
        "right-v1");
}

TEST_F(TFileResourceTest, NamedControllerReportsAndClearsPersistedRolloutWarning)
{
    TFakeFileProvider::PushDiscoveryRevision("v1", "file");

    auto queue = New<TActionQueue>();
    auto stateManager = New<TStateManagerMock>();
    auto controller = MakeNamedController(
        queue->GetInvoker(),
        CreateSyncStatusProfiler(),
        {{"file", "file"}},
        TDuration::MilliSeconds(1),
        {},
        {},
        TDuration::MilliSeconds(1),
        1024,
        TDuration::MilliSeconds(1));
    controller->Init(stateManager->CreateContext());

    TResourceRevisionPtr target;
    WaitForPredicate(
        [&] {
            target = controller->BuildTargetRevision();
            return target && target->PreparingFileSnapshot;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });

    auto status = New<TWorkerResourceStatus>();
    status->ResourceInstanceId = TResourceInstanceId(TGuid::Create());
    status->ResourceIncarnationGeneration = 1;
    status->TargetRevisionId = 17;
    status->PreparingFileSnapshot = New<TFileSnapshotStatus>();
    status->PreparingFileSnapshot->SnapshotId = target->PreparingFileSnapshot->Id;
    status->PreparingFileSnapshot->State = EFileSnapshotState::Validated;
    controller->CollectStatuses({{"worker", MakeWorkerStatus(status)}}, nullptr, 17);

    target = controller->BuildTargetRevision();
    ASSERT_TRUE(target->ActiveFileSnapshot);
    const auto activeSnapshotId = target->ActiveFileSnapshot->Id;
    WaitFor(BIND([] {
    }).AsyncVia(queue->GetInvoker())
            .Run())
        .ThrowOnError();
    stateManager->Sync();
    controller.Reset();

    TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(5));
    auto restoredStateManager = New<TStateManagerMock>();
    restoredStateManager->SetStorage(stateManager->GetStorage());
    auto statusProfiler = CreateSyncStatusProfiler();
    auto restored = MakeNamedController(
        queue->GetInvoker(),
        statusProfiler,
        {{"file", "file"}},
        TDuration::Hours(1),
        {},
        {},
        TDuration::MilliSeconds(1),
        1024,
        TDuration::MilliSeconds(1));
    restored->Init(restoredStateManager->CreateContext());

    status->PreparingFileSnapshot = New<TFileSnapshotStatus>();
    status->PreparingFileSnapshot->SnapshotId = activeSnapshotId;
    status->PreparingFileSnapshot->State = EFileSnapshotState::Preparing;
    status->PreparingFileSnapshot->PreparationStage = EFileSnapshotPreparationStage::Waiting;
    status->PreparingFileSnapshot->Error = TError("download failed");
    restored->CollectStatuses({{"worker", MakeWorkerStatus(status)}}, nullptr, 17);
    ASSERT_TRUE(statusProfiler->GetStatus().Errors.contains("/file_snapshot_rollout"));

    auto view = restored->GetView()->GetChildOrThrow("file_providers")->AsMap();
    EXPECT_EQ(view->GetChildValueOrThrow<i64>("rollout_instance_count"), 1);
    EXPECT_EQ(view->GetChildValueOrThrow<i64>("rollout_converged_instance_count"), 0);
    EXPECT_EQ(view->GetChildValueOrThrow<i64>("rollout_uninitialized_instance_count"), 1);
    EXPECT_TRUE(view->GetChildOrThrow("active_file_snapshot_published_at")->GetType() != ENodeType::Entity);
    EXPECT_EQ(
        view->GetChildOrThrow("rollout_progress_state_counts")
            ->AsMap()
            ->GetChildValueOrThrow<i64>(FormatEnum(EFileSnapshotPreparationStage::Waiting)),
        1);
    EXPECT_TRUE(view->GetChildOrThrow("rollout_errors")->AsMap()->FindChild("worker"));

    status->PreparingFileSnapshot->State = EFileSnapshotState::Draining;
    status->PreparingFileSnapshot->PreparationStage.reset();
    status->PreparingFileSnapshot->Error = TError();
    status->LiveAccessorCounts[TFileSnapshotId(activeSnapshotId.Underlying() + 1)] = 2;
    restored->CollectStatuses({{"worker", MakeWorkerStatus(status)}}, nullptr, 17);
    view = restored->GetView()->GetChildOrThrow("file_providers")->AsMap();
    EXPECT_EQ(view->GetChildValueOrThrow<i64>("rollout_blocking_accessor_count"), 2);

    status->ActiveFileSnapshotId = activeSnapshotId;
    status->PreparingFileSnapshot.Reset();
    restored->CollectStatuses({{"worker", MakeWorkerStatus(status)}}, nullptr, 17);
    EXPECT_FALSE(statusProfiler->GetStatus().Errors.contains("/file_snapshot_rollout"));
    view = restored->GetView()->GetChildOrThrow("file_providers")->AsMap();
    EXPECT_EQ(view->GetChildValueOrThrow<i64>("rollout_converged_instance_count"), 1);

    auto oldTargetStatus = New<TWorkerResourceStatus>();
    oldTargetStatus->ResourceInstanceId = TResourceInstanceId(TGuid::Create());
    oldTargetStatus->ResourceIncarnationGeneration = 1;
    oldTargetStatus->TargetRevisionId = 16;
    oldTargetStatus->ActiveFileSnapshotId = TFileSnapshotId(activeSnapshotId.Underlying() + 1);
    restored->CollectStatuses(
        {
            {"worker", MakeWorkerStatus(status)},
            {"old-target-worker", MakeWorkerStatus(oldTargetStatus, TIncarnationId(TGuid::Create()))},
        },
        nullptr,
        17);
    ASSERT_TRUE(statusProfiler->GetStatus().Errors.contains("/file_snapshot_rollout"));
    view = restored->GetView()->GetChildOrThrow("file_providers")->AsMap();
    EXPECT_EQ(view->GetChildValueOrThrow<i64>("rollout_instance_count"), 2);
    EXPECT_EQ(view->GetChildValueOrThrow<i64>("rollout_converged_instance_count"), 1);
    EXPECT_EQ(view->GetChildValueOrThrow<i64>("rollout_lagging_instance_count"), 1);
    EXPECT_EQ(
        view->GetChildOrThrow("rollout_progress_state_counts")
            ->AsMap()
            ->GetChildValueOrThrow<i64>("target_revision_pending"),
        1);

    restored->CollectStatuses({}, nullptr, 17);
    EXPECT_FALSE(statusProfiler->GetStatus().Errors.contains("/file_snapshot_rollout"));
    EXPECT_EQ(restored->GetView()
            ->GetChildOrThrow("file_providers")
            ->AsMap()
            ->GetChildValueOrThrow<i64>("rollout_instance_count"),
        0);
}

TEST_F(TFileResourceTest, NamedControllerAggregatesAndDropsWorkerSnapshotState)
{
    auto queue = New<TActionQueue>();
    auto controller = MakeNamedController(
        queue->GetInvoker(),
        CreateSyncStatusProfiler(),
        {{"file", "file"}});

    auto status = New<TWorkerResourceStatus>();
    status->ResourceInstanceId = TResourceInstanceId(TGuid::Create());
    status->ResourceIncarnationGeneration = 5;
    status->TargetRevisionId = 17;
    status->ActiveFileSnapshotId = TFileSnapshotId(7);
    status->PreparingFileSnapshot = New<TFileSnapshotStatus>();
    status->PreparingFileSnapshot->SnapshotId = TFileSnapshotId(8);
    status->PreparingFileSnapshot->State = EFileSnapshotState::Preparing;
    status->PreparingFileSnapshot->PreparationStage = EFileSnapshotPreparationStage::Validating;
    status->LiveAccessorCounts[TFileSnapshotId(7)] = 2;
    status->LiveAccessorCounts[TFileSnapshotId(8)] = 1;
    controller->CollectStatuses({{"worker", MakeWorkerStatus(status)}}, nullptr, 17);

    auto view = controller->GetView()->GetChildOrThrow("file_providers")->AsMap();
    auto fileSnapshotStateCounts = view->GetChildOrThrow("file_snapshot_state_counts")->AsMap();
    EXPECT_EQ(
        fileSnapshotStateCounts->GetChildValueOrThrow<i64>(
            Format("%v/%v", TFileSnapshotId(7), FormatEnum(EFileSnapshotState::Active))),
        1);
    EXPECT_EQ(
        fileSnapshotStateCounts->GetChildValueOrThrow<i64>(
            Format("%v/%v", TFileSnapshotId(8), FormatEnum(EFileSnapshotState::Preparing))),
        1);
    auto liveAccessorCounts = view->GetChildOrThrow("live_accessor_counts")->AsMap();
    EXPECT_EQ(liveAccessorCounts->GetChildValueOrThrow<i64>(ToString(TFileSnapshotId(7))), 2);
    EXPECT_EQ(liveAccessorCounts->GetChildValueOrThrow<i64>(ToString(TFileSnapshotId(8))), 1);
    EXPECT_EQ(view->GetChildValueOrThrow<i64>("unknown_file_snapshot_count"), 2);
    EXPECT_EQ(view->GetChildValueOrThrow<i64>("rollout_instance_count"), 1);
    EXPECT_EQ(view->GetChildValueOrThrow<i64>("rollout_lagging_instance_count"), 1);
    EXPECT_EQ(
        view->GetChildOrThrow("rollout_progress_state_counts")
            ->AsMap()
            ->GetChildValueOrThrow<i64>(FormatEnum(EFileSnapshotPreparationStage::Validating)),
        1);

    controller->CollectStatuses({}, nullptr, 17);
    view = controller->GetView()->GetChildOrThrow("file_providers")->AsMap();
    EXPECT_TRUE(view->GetChildOrThrow("file_snapshot_state_counts")->AsMap()->GetChildren().empty());
    EXPECT_TRUE(view->GetChildOrThrow("live_accessor_counts")->AsMap()->GetChildren().empty());
    EXPECT_EQ(view->GetChildValueOrThrow<i64>("unknown_file_snapshot_count"), 0);
    EXPECT_EQ(view->GetChildValueOrThrow<i64>("rollout_instance_count"), 0);

    status->ResourceInstanceId = TResourceInstanceId(TGuid::Create());
    status->ResourceIncarnationGeneration = 0;
    controller->CollectStatuses({{"worker", MakeWorkerStatus(status)}}, nullptr, 17);
    view = controller->GetView()->GetChildOrThrow("file_providers")->AsMap();
    EXPECT_EQ(view->GetChildValueOrThrow<i64>("rollout_instance_count"), 1);
}

TEST_F(TFileResourceTest, NamedControllerCountsHistoricalAppliedFileProviderRevisions)
{
    TFakeFileProvider::PushDiscoveryRevision("left-v1", "left");
    TFakeFileProvider::PushDiscoveryRevision("right-v1", "right");

    auto queue = New<TActionQueue>();
    auto controller = MakeNamedController(
        queue->GetInvoker(),
        CreateSyncStatusProfiler(),
        {{"left", "left"}, {"right", "right"}},
        TDuration::MilliSeconds(1));
    controller->Init(nullptr);

    auto status = New<TWorkerResourceStatus>();
    status->ResourceInstanceId = TResourceInstanceId(TGuid::Create());
    status->ResourceIncarnationGeneration = 3;
    status->TargetRevisionId = 17;

    TResourceRevisionPtr target;
    WaitForPredicate(
        [&] {
            target = controller->BuildTargetRevision();
            return target && target->PreparingFileSnapshot;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    status->PreparingFileSnapshot = New<TFileSnapshotStatus>();
    status->PreparingFileSnapshot->SnapshotId = target->PreparingFileSnapshot->Id;
    status->PreparingFileSnapshot->State = EFileSnapshotState::Validated;
    controller->CollectStatuses({{"worker", MakeWorkerStatus(status)}}, nullptr, 17);
    target = controller->BuildTargetRevision();
    ASSERT_TRUE(target->ActiveFileSnapshot);
    const auto activeId = target->ActiveFileSnapshot->Id;

    TFakeFileProvider::PushDiscoveryRevision("left-v2", "left");
    WaitForPredicate(
        [&] {
            target = controller->BuildTargetRevision();
            return target &&
                target->PreparingFileSnapshot &&
                target->PreparingFileSnapshot->FileProviders.at(TFileProviderId("left"))->ObjectId.Underlying() == "left-v2";
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });

    status->ActiveFileSnapshotId = activeId;
    status->PreparingFileSnapshot = New<TFileSnapshotStatus>();
    status->PreparingFileSnapshot->SnapshotId = target->PreparingFileSnapshot->Id;
    status->PreparingFileSnapshot->State = EFileSnapshotState::Preparing;
    status->PreparingFileSnapshot->PreparationStage = EFileSnapshotPreparationStage::Materializing;
    controller->CollectStatuses({{"worker", MakeWorkerStatus(status)}}, nullptr, 17);

    auto counts = controller->GetView()
        ->GetChildOrThrow("file_providers")
        ->AsMap()
        ->GetChildOrThrow("file_provider_revision_state_counts")
        ->AsMap();
    EXPECT_EQ(counts->GetChildValueOrThrow<i64>(
        Format("%v/%v/%v", TFileProviderId("left"), NFileStorage::TFileStorageObjectId("left-v1"), FormatEnum(EFileSnapshotState::Active))),
        1);
    EXPECT_EQ(counts->GetChildValueOrThrow<i64>(
        Format("%v/%v/%v", TFileProviderId("left"), NFileStorage::TFileStorageObjectId("left-v2"), FormatEnum(EFileSnapshotState::Preparing))),
        1);
    EXPECT_EQ(counts->GetChildValueOrThrow<i64>(
        Format("%v/%v/%v", TFileProviderId("right"), NFileStorage::TFileStorageObjectId("right-v1"), FormatEnum(EFileSnapshotState::Active))),
        1);
    EXPECT_FALSE(counts->FindChild(
        Format("%v/%v/%v", TFileProviderId("right"), NFileStorage::TFileStorageObjectId("right-v1"), FormatEnum(EFileSnapshotState::Preparing))));
}

TEST_F(TFileResourceTest, NamedControllerRestoresSnapshotsAcrossCompatibleSpecChanges)
{
    TFakeFileProvider::PushDiscoveryRevision("left-v1", "left");
    TFakeFileProvider::PushDiscoveryRevision("right-v1", "right");

    auto queue = New<TActionQueue>();
    auto stateManager = New<TStateManagerMock>();
    auto controller = MakeNamedController(
        queue->GetInvoker(),
        CreateSyncStatusProfiler(),
        {{"left", "left"}, {"right", "right"}},
        TDuration::MilliSeconds(1));
    controller->Init(stateManager->CreateContext());
    WaitForPredicate(
        [&] {
            return static_cast<bool>(controller->BuildTargetRevision());
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    WaitFor(BIND([] {
    }).AsyncVia(queue->GetInvoker())
            .Run())
        .ThrowOnError();
    stateManager->Sync();
    controller.Reset();

    TFakeFileProvider::SetDiscoveryError("left");
    TFakeFileProvider::SetDiscoveryError("right");
    auto restoredStateManager = New<TStateManagerMock>();
    restoredStateManager->SetStorage(stateManager->GetStorage());
    auto restored = MakeNamedController(
        queue->GetInvoker(),
        CreateSyncStatusProfiler(),
        {{"left", "left"}, {"right", "right"}},
        TDuration::Hours(1));
    restored->Init(restoredStateManager->CreateContext());
    ASSERT_TRUE(restored->BuildTargetRevision());
    EXPECT_EQ(
        GetLatestFileSnapshot(restored->BuildTargetRevision())->FileProviders.at(TFileProviderId("left"))->ObjectId.Underlying(),
        "left-v1");
    EXPECT_EQ(
        GetLatestFileSnapshot(restored->BuildTargetRevision())->FileProviders.at(TFileProviderId("right"))->ObjectId.Underlying(),
        "right-v1");
    auto changedStateManager = New<TStateManagerMock>();
    changedStateManager->SetStorage(stateManager->GetStorage());
    auto changed = MakeNamedController(
        queue->GetInvoker(),
        CreateSyncStatusProfiler(),
        {{"left", "changed-left"}, {"right", "right"}},
        TDuration::Hours(1));
    changed->Init(changedStateManager->CreateContext());
    ASSERT_TRUE(changed->BuildTargetRevision());
    EXPECT_EQ(
        GetLatestFileSnapshot(changed->BuildTargetRevision())->FileProviders.at(TFileProviderId("left"))->ObjectId.Underlying(),
        "left-v1");
    auto incompatibleStateManager = New<TStateManagerMock>();
    incompatibleStateManager->SetStorage(stateManager->GetStorage());
    auto incompatible = MakeNamedController(
        queue->GetInvoker(),
        CreateSyncStatusProfiler(),
        {{"left", "changed-left"}, {"right", "right"}, {"extra", "extra"}},
        TDuration::Hours(1));
    incompatible->Init(incompatibleStateManager->CreateContext());
    EXPECT_FALSE(incompatible->BuildTargetRevision());
}

TEST_F(TFileResourceTest, NamedControllerReconfiguresDirectDiscoverPeriod)
{
    auto queue = New<TActionQueue>();
    auto controller = MakeNamedController(
        queue->GetInvoker(),
        CreateSyncStatusProfiler(),
        {{"left", "left"}},
        TDuration::Hours(1));
    controller->Init(nullptr);
    WaitForPredicate(
        [] {
            return TFakeFileProvider::GetDiscoverCount("left") > 0;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    auto discoverCount = TFakeFileProvider::GetDiscoverCount("left");

    auto dynamicContext = New<TDynamicResourceControllerContext>();
    dynamicContext->DynamicResourceSpec = MakeNamedDynamicContext(
        nullptr,
        TDuration::MilliSeconds(1))
        ->DynamicResourceSpec;
    controller->Reconfigure(dynamicContext);

    WaitForPredicate(
        [&] {
            return TFakeFileProvider::GetDiscoverCount("left") >= discoverCount + 2;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
}

TEST_F(TFileResourceTest, NamedControllerAppliesDynamicPinImmediately)
{
    TFakeFileProvider::PushDiscoveryRevision("latest-v1", "left");

    auto queue = New<TActionQueue>();
    auto controller = MakeNamedController(
        queue->GetInvoker(),
        CreateSyncStatusProfiler(),
        {{"left", "left"}},
        TDuration::Hours(1));
    controller->Init(nullptr);
    WaitForPredicate(
        [&] {
            auto target = controller->BuildTargetRevision();
            return target &&
                GetLatestFileSnapshot(target)->FileProviders.at(TFileProviderId("left"))->ObjectId.Underlying() == "latest-v1";
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });

    auto dynamicContext = New<TDynamicResourceControllerContext>();
    dynamicContext->DynamicResourceSpec = MakeNamedDynamicContext(
        nullptr,
        TDuration::Hours(1),
        TDuration::MilliSeconds(100),
        {{"left", "pinned-v1"}})
        ->DynamicResourceSpec;
    controller->Reconfigure(dynamicContext);

    WaitForPredicate(
        [&] {
            auto target = controller->BuildTargetRevision();
            return target &&
                GetLatestFileSnapshot(target)->FileProviders.at(TFileProviderId("left"))->ObjectId.Underlying() == "pinned-v1";
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
}

TEST_F(TFileResourceTest, NamedControllerPublishesMultipleDynamicPinsAtomically)
{
    TFakeFileProvider::PushDiscoveryRevision("left-v0", "left");
    TFakeFileProvider::PushDiscoveryRevision("right-v0", "right");
    auto rightGate = NewPromise<void>();
    TFakeFileProvider::SetDiscoveryGate("right-v1", rightGate.ToFuture());

    auto queue = New<TActionQueue>();
    auto stateManager = New<TStateManagerMock>();
    auto controller = MakeNamedController(
        queue->GetInvoker(),
        CreateSyncStatusProfiler(),
        {{"left", "left"}, {"right", "right"}},
        TDuration::Hours(1));
    controller->Init(stateManager->CreateContext());
    WaitForPredicate(
        [&] {
            auto target = controller->BuildTargetRevision();
            return target &&
                GetLatestFileSnapshot(target)->FileProviders.at(TFileProviderId("left"))->ObjectId.Underlying() == "left-v0" &&
                GetLatestFileSnapshot(target)->FileProviders.at(TFileProviderId("right"))->ObjectId.Underlying() == "right-v0";
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });

    auto leftDiscoverCount = TFakeFileProvider::GetDiscoverCount("left");
    auto rightDiscoverCount = TFakeFileProvider::GetDiscoverCount("right");
    auto dynamicContext = New<TDynamicResourceControllerContext>();
    dynamicContext->DynamicResourceSpec = MakeNamedDynamicContext(
        nullptr,
        TDuration::Hours(1),
        TDuration::MilliSeconds(100),
        {{"left", "left-v1"}, {"right", "right-v1"}})
        ->DynamicResourceSpec;
    controller->Reconfigure(dynamicContext);

    WaitForPredicate(
        [&] {
            return TFakeFileProvider::GetDiscoverCount("left") > leftDiscoverCount &&
                TFakeFileProvider::GetDiscoverCount("right") > rightDiscoverCount;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    WaitFor(BIND([] {
    }).AsyncVia(queue->GetInvoker())
            .Run())
        .ThrowOnError();

    auto pendingTarget = controller->BuildTargetRevision();
    ASSERT_TRUE(pendingTarget);
    EXPECT_EQ(
        GetLatestFileSnapshot(pendingTarget)->FileProviders.at(TFileProviderId("left"))->ObjectId.Underlying(),
        "left-v0");
    EXPECT_EQ(
        GetLatestFileSnapshot(pendingTarget)->FileProviders.at(TFileProviderId("right"))->ObjectId.Underlying(),
        "right-v0");

    stateManager->Sync();
    auto restoredStateManager = New<TStateManagerMock>();
    restoredStateManager->SetStorage(stateManager->GetStorage());
    auto restored = MakeNamedController(
        queue->GetInvoker(),
        CreateSyncStatusProfiler(),
        {{"left", "left"}, {"right", "right"}},
        TDuration::Hours(1),
        {},
        {{"left", "left-v1"}, {"right", "right-v1"}});
    restored->Init(restoredStateManager->CreateContext());
    auto restoredTarget = restored->BuildTargetRevision();
    ASSERT_TRUE(restoredTarget);
    EXPECT_EQ(
        GetLatestFileSnapshot(restoredTarget)->FileProviders.at(TFileProviderId("left"))->ObjectId.Underlying(),
        "left-v0");
    EXPECT_EQ(
        GetLatestFileSnapshot(restoredTarget)->FileProviders.at(TFileProviderId("right"))->ObjectId.Underlying(),
        "right-v0");

    rightGate.Set();
    WaitForPredicate(
        [&] {
            auto target = restored->BuildTargetRevision();
            return target &&
                GetLatestFileSnapshot(target)->FileProviders.at(TFileProviderId("left"))->ObjectId.Underlying() == "left-v1" &&
                GetLatestFileSnapshot(target)->FileProviders.at(TFileProviderId("right"))->ObjectId.Underlying() == "right-v1";
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
}

TEST_F(TFileResourceTest, NamedControllerDiscardsDiscoveryStartedBeforeDynamicPin)
{
    auto staleGate = NewPromise<void>();
    auto pinnedGate = NewPromise<void>();
    TFakeFileProvider::SetDiscoveryGate("stale", staleGate.ToFuture());
    TFakeFileProvider::SetDiscoveryGate("pinned", pinnedGate.ToFuture());
    TFakeFileProvider::PushDiscoveryRevision("initial", "left");
    TFakeFileProvider::PushDiscoveryRevision("stale", "left");

    auto queue = New<TActionQueue>();
    auto controller = MakeNamedController(
        queue->GetInvoker(),
        CreateSyncStatusProfiler(),
        {{"left", "left"}},
        TDuration::MilliSeconds(1));
    controller->Init(nullptr);
    WaitForPredicate(
        [&] {
            auto target = controller->BuildTargetRevision();
            return target &&
                GetLatestFileSnapshot(target)->FileProviders.at(TFileProviderId("left"))->ObjectId.Underlying() == "initial";
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    WaitForPredicate(
        [] {
            return TFakeFileProvider::GetDiscoverCount("left") >= 2;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });

    auto dynamicContext = New<TDynamicResourceControllerContext>();
    dynamicContext->DynamicResourceSpec = MakeNamedDynamicContext(
        nullptr,
        TDuration::Hours(1),
        TDuration::MilliSeconds(100),
        {{"left", "pinned"}})
        ->DynamicResourceSpec;
    controller->Reconfigure(dynamicContext);

    staleGate.Set();
    WaitForPredicate(
        [] {
            return TFakeFileProvider::GetDiscoverCount("left") >= 3;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    ASSERT_EQ(
        GetLatestFileSnapshot(controller->BuildTargetRevision())
            ->FileProviders.at(TFileProviderId("left"))
            ->ObjectId.Underlying(),
        "initial");

    pinnedGate.Set();
    WaitForPredicate(
        [&] {
            auto target = controller->BuildTargetRevision();
            return target &&
                GetLatestFileSnapshot(target)->FileProviders.at(TFileProviderId("left"))->ObjectId.Underlying() == "pinned";
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
}

TEST_F(TFileResourceTest, NamedControllerRestoresSnapshotAcrossDynamicPinChange)
{
    auto queue = New<TActionQueue>();
    auto stateManager = New<TStateManagerMock>();
    auto controller = MakeNamedController(
        queue->GetInvoker(),
        CreateSyncStatusProfiler(),
        {{"left", "left"}},
        TDuration::MilliSeconds(1),
        {},
        {{"left", "pinned-v1"}});
    controller->Init(stateManager->CreateContext());
    WaitForPredicate(
        [&] {
            return static_cast<bool>(controller->BuildTargetRevision());
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    WaitFor(BIND([] {
    }).AsyncVia(queue->GetInvoker())
            .Run())
        .ThrowOnError();
    stateManager->Sync();
    controller.Reset();

    TFakeFileProvider::SetDiscoveryError("left");
    auto restoredStateManager = New<TStateManagerMock>();
    restoredStateManager->SetStorage(stateManager->GetStorage());
    auto restored = MakeNamedController(
        queue->GetInvoker(),
        CreateSyncStatusProfiler(),
        {{"left", "left"}},
        TDuration::Hours(1));
    restored->Init(restoredStateManager->CreateContext());
    auto target = restored->BuildTargetRevision();
    ASSERT_TRUE(target);
    EXPECT_EQ(
        GetLatestFileSnapshot(target)->FileProviders.at(TFileProviderId("left"))->ObjectId.Underlying(),
        "pinned-v1");
}

TEST_F(TFileResourceTest, LockBeforeFirstStateThrows)
{
    auto queue = New<TActionQueue>();
    auto resource = MakeResource(queue->GetInvoker());

    EXPECT_THROW_WITH_SUBSTRING(resource->Lock(), "no initialized data");
}

TEST_F(TFileResourceTest, ThrowingStorageIsReportedAndRetried)
{
    auto queue = New<TActionQueue>();
    auto statusProfiler = CreateSyncStatusProfiler();
    auto resource = MakeResource(
        queue->GetInvoker(),
        MakeTarget(1, "v1"),
        New<TThrowingFileStorage>(),
        statusProfiler);

    EXPECT_TRUE(statusProfiler->GetStatus().Errors.empty());

    auto loadFuture = resource->Load({});
    WaitForPredicate(
        [&] {
            return statusProfiler->GetStatus().Errors.contains("/file_update");
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });

    EXPECT_FALSE(loadFuture.IsSet());
    const auto status = statusProfiler->GetStatus();
    const auto& error = status.Errors.at("/file_update");
    EXPECT_THAT(ToString(error), ::testing::HasSubstr("worker.file_storage"));
    auto revisionState = resource->GetRevisionState();
    ASSERT_TRUE(revisionState.PreparingFileSnapshot);
    EXPECT_FALSE(revisionState.PreparingFileSnapshot->Error.IsOK());
    EXPECT_TRUE(revisionState.PreparingFileSnapshot->NextRetryAt);
}

TEST_F(TFileResourceTest, MissingStorageIsReportedAndRetried)
{
    auto queue = New<TActionQueue>();
    auto statusProfiler = CreateSyncStatusProfiler();
    auto resource = MakeResource(
        queue->GetInvoker(),
        MakeTarget(1, "v1"),
        NFileStorage::IFileStoragePtr{},
        statusProfiler);

    auto loadFuture = resource->Load({});
    WaitForPredicate(
        [&] {
            return statusProfiler->GetStatus().Errors.contains("/file_update");
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });

    EXPECT_FALSE(loadFuture.IsSet());
    const auto status = statusProfiler->GetStatus();
    const auto& error = status.Errors.at("/file_update");
    EXPECT_THAT(ToString(error), ::testing::HasSubstr("file storage is unavailable"));
    auto revisionState = resource->GetRevisionState();
    ASSERT_TRUE(revisionState.PreparingFileSnapshot);
    EXPECT_FALSE(revisionState.PreparingFileSnapshot->Error.IsOK());
    EXPECT_TRUE(revisionState.PreparingFileSnapshot->NextRetryAt);
}

TEST_F(TFileResourceTest, MalformedTargetIsReportedAndCanRecover)
{
    for (const auto& target : {MakeMalformedTarget(1), MakeClassMismatchTarget(1)}) {
        auto queue = New<TActionQueue>();
        auto statusProfiler = CreateSyncStatusProfiler();
        auto resource = MakeResource(
            queue->GetInvoker(),
            target,
            New<TFakeFileStorage>(),
            statusProfiler);

        auto loadFuture = resource->Load({});
        WaitForPredicate(
            [&] {
                auto state = resource->GetRevisionState();
                return statusProfiler->GetStatus().Errors.contains("/file_update") &&
                    state.PreparingFileSnapshot &&
                    !state.PreparingFileSnapshot->Error.IsOK() &&
                    state.PreparingFileSnapshot->NextRetryAt;
            },
            TWaitForPredicateOptions{
                .IterationCount = 100,
                .Period = TDuration::MilliSeconds(5),
            });

        EXPECT_FALSE(loadFuture.IsSet());
        resource->Reconfigure(MakeDynamicContext(MakeTarget(2, "recovered")));
        WaitFor(loadFuture.WithTimeout(TDuration::Seconds(5))).ThrowOnError();
        EXPECT_EQ(resource->Lock()->Value, "payload:recovered");
        EXPECT_EQ(resource->GetRevisionState().AppliedRevisionId, 2);
        EXPECT_FALSE(statusProfiler->GetStatus().Errors.contains("/file_update"));
    }
}

TEST_F(TFileResourceTest, InitialLoadPublishesState)
{
    auto queue = New<TActionQueue>();
    auto resource = MakeResource(queue->GetInvoker(), MakeTarget(1, "v1"));

    WaitFor(resource->Load({})).ThrowOnError();

    auto accessor = resource->Lock();
    EXPECT_EQ(accessor->Value, "payload:v1");
    EXPECT_EQ(accessor.GetProviderRevision(TFileProviderId("file"))->ObjectId.Underlying(), "v1");
    EXPECT_EQ(accessor.GetDeliveryRevisionId(), 1);
    EXPECT_EQ(resource->GetRevisionState().AppliedRevisionId, 1);
    EXPECT_EQ(resource->GetRevisionState().TargetRevisionId, 1);
    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("v1"), 1);
    EXPECT_EQ(TTestFileResource::GetInitializeCount("payload:v1"), 1);
}

TEST_F(TFileResourceTest, PostprocessingUsesSanitizedEnvironmentAndDerivedCache)
{
    const std::string command = R"(
test "$PATH" = "/usr/bin:/bin"
test "$LANG" = "C"
test "$LC_ALL" = "C"
test "$TZ" = "UTC"
test "$PWD" = "$YT_FLOW_POSTPROCESSING_PATH"
test -z "${YT_FLOW_POSTPROCESS_TEST_SECRET+x}"
/bin/cat "$YT_FLOW_RESOURCE_PATH/artifact" > "$YT_FLOW_POSTPROCESSING_PATH/artifact"
/usr/bin/printf ':processed' >> "$YT_FLOW_POSTPROCESSING_PATH/artifact"
)";

    setenv("YT_FLOW_POSTPROCESS_TEST_SECRET", "must-not-leak", 1);
    auto envGuard = Finally([] {
        unsetenv("YT_FLOW_POSTPROCESS_TEST_SECRET");
    });

    auto queue = New<TActionQueue>();
    auto storage = New<TFakeFileStorage>();

    auto rawResource = MakeResource(queue->GetInvoker(), MakeTarget(1, "v1"), storage);
    WaitFor(rawResource->Load({}).WithTimeout(TDuration::Seconds(5))).ThrowOnError();
    EXPECT_EQ(rawResource->Lock()->Value, "payload:v1");
    rawResource.Reset();

    auto statusProfiler = CreateSyncStatusProfiler();
    auto first = MakePostprocessedResource(
        queue->GetInvoker(),
        MakeTarget(1, "v1"),
        command,
        TDuration::Seconds(5),
        storage,
        statusProfiler);
    auto firstLoadResult = WaitFor(first->Load({}).WithTimeout(TDuration::Seconds(5)));
    ASSERT_TRUE(firstLoadResult.IsOK())
        << ToString(statusProfiler->GetStatus().Errors.at("/file_update"));
    EXPECT_EQ(first->Lock()->Value, "payload:v1:processed");
    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("v1"), 1);
    first.Reset();

    auto cached = MakePostprocessedResource(
        queue->GetInvoker(),
        MakeTarget(1, "v1"),
        command,
        TDuration::Hours(1),
        storage);
    WaitFor(cached->Load({}).WithTimeout(TDuration::Seconds(5))).ThrowOnError();
    EXPECT_EQ(cached->Lock()->Value, "payload:v1:processed");
    cached.Reset();
    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("v1"), 1);

    auto changed = MakePostprocessedResource(
        queue->GetInvoker(),
        MakeTarget(1, "v1"),
        command + R"(/usr/bin/printf ':changed' >> "$YT_FLOW_POSTPROCESSING_PATH/artifact"
)",
        TDuration::Seconds(5),
        storage);
    WaitFor(changed->Load({}).WithTimeout(TDuration::Seconds(5))).ThrowOnError();
    EXPECT_EQ(changed->Lock()->Value, "payload:v1:processed:changed");
    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("v1"), 1);
}

TEST_F(TFileResourceTest, ChangedPostprocessCommandReusesRealFileStorageDownload)
{
    TRealFileStorageFixture storageFixture;
    auto storage = storageFixture.MakeStorage();
    auto queue = New<TActionQueue>();
    auto statusProfiler = CreateSyncStatusProfiler();
    const std::string command = R"(
/bin/cat "$YT_FLOW_RESOURCE_PATH/artifact" > "$YT_FLOW_POSTPROCESSING_PATH/artifact"
/usr/bin/printf ':first' >> "$YT_FLOW_POSTPROCESSING_PATH/artifact"
)";

    auto first = MakePostprocessedResource(
        queue->GetInvoker(),
        MakeTarget(1, "v1"),
        command,
        TDuration::Seconds(5),
        storage,
        statusProfiler);
    auto firstLoad = WaitFor(first->Load({}).WithTimeout(TDuration::Seconds(10)));
    auto status = statusProfiler->GetStatus();
    auto updateError = status.Errors.find("/file_update");
    ASSERT_TRUE(firstLoad.IsOK())
        << "Download count: " << TFakeFileProvider::GetDownloadCount("v1")
        << ", update error: " << (updateError == status.Errors.end() ? "missing" : ToString(updateError->second));
    EXPECT_EQ(first->Lock()->Value, "payload:v1:first");
    first.Reset();

    auto changed = MakePostprocessedResource(
        queue->GetInvoker(),
        MakeTarget(1, "v1"),
        command + R"(/usr/bin/printf ':changed' >> "$YT_FLOW_POSTPROCESSING_PATH/artifact"
)",
        TDuration::Seconds(5),
        storage,
        statusProfiler);
    WaitFor(changed->Load({}).WithTimeout(TDuration::Seconds(5))).ThrowOnError();
    EXPECT_EQ(changed->Lock()->Value, "payload:v1:first:changed");
    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("v1"), 1);

    changed.Reset();
    storage.Reset();
}

TEST_F(TFileResourceTest, PostprocessFailureRetainsPreviousSnapshotAndRecovers)
{
    const std::string command = R"(
if /bin/grep -q bad "$YT_FLOW_RESOURCE_PATH/artifact"; then
    /usr/bin/printf 'postprocess boom\n' >&2
    exit 42
fi
/bin/cp "$YT_FLOW_RESOURCE_PATH/artifact" "$YT_FLOW_POSTPROCESSING_PATH/artifact"
)";

    auto queue = New<TActionQueue>();
    auto statusProfiler = CreateSyncStatusProfiler();
    auto resource = MakePostprocessedResource(
        queue->GetInvoker(),
        MakeTarget(1, "v1"),
        command,
        TDuration::Seconds(5),
        New<TFakeFileStorage>(),
        statusProfiler,
        TDuration::Hours(1));
    WaitFor(resource->Load({}).WithTimeout(TDuration::Seconds(5))).ThrowOnError();

    resource->Reconfigure(MakeDynamicContext(
        MakeTarget(2, "bad"),
        TDuration::Hours(1).MilliSeconds()));
    WaitForPredicate([&] {
        return statusProfiler->GetStatus().Errors.contains("/file_update");
    });

    const auto error = ToString(statusProfiler->GetStatus().Errors.at("/file_update"));
    EXPECT_THAT(error, ::testing::HasSubstr("phase"));
    EXPECT_THAT(error, ::testing::HasSubstr("exit"));
    EXPECT_THAT(error, ::testing::HasSubstr("exit_code"));
    EXPECT_THAT(error, ::testing::HasSubstr("42"));
    EXPECT_THAT(error, ::testing::HasSubstr("postprocess boom"));
    EXPECT_THAT(error, ::testing::HasSubstr("command_digest"));
    EXPECT_EQ(resource->Lock()->Value, "payload:v1");
    EXPECT_EQ(resource->GetRevisionState().AppliedRevisionId, 1);

    resource->Reconfigure(MakeDynamicContext(MakeTarget(3, "v3")));
    WaitForAppliedRevision(resource, 3);
    EXPECT_EQ(resource->Lock()->Value, "payload:v3");
    EXPECT_FALSE(statusProfiler->GetStatus().Errors.contains("/file_update"));
}

TEST_F(TFileResourceTest, PostprocessHelperCrashDoesNotKillWorker)
{
    auto queue = New<TActionQueue>();
    auto statusProfiler = CreateSyncStatusProfiler();
    auto resource = MakePostprocessedResource(
        queue->GetInvoker(),
        MakeTarget(1, "v1"),
        "ulimit -c 0; /bin/kill -ABRT $$",
        TDuration::Seconds(5),
        New<TFakeFileStorage>(),
        statusProfiler,
        TDuration::Hours(1));

    auto loadFuture = resource->Load({});
    WaitForPredicate([&] {
        return statusProfiler->GetStatus().Errors.contains("/file_update");
    });

    EXPECT_FALSE(loadFuture.IsSet());
    const auto error = ToString(statusProfiler->GetStatus().Errors.at("/file_update"));
    EXPECT_THAT(error, ::testing::HasSubstr("signal"));
    EXPECT_THAT(error, ::testing::HasSubstr("command_digest"));
}

TEST_F(TFileResourceTest, PostprocessBoundsAndDrainsOutput)
{
    const std::string command = R"(
/bin/dd if=/dev/zero bs=1048576 count=8 2>/dev/null | /usr/bin/tr '\0' x
/usr/bin/printf 'STDOUT_MARKER\n'
/bin/dd if=/dev/zero bs=1048576 count=8 2>/dev/null | /usr/bin/tr '\0' y >&2
/usr/bin/printf 'STDERR_MARKER\n' >&2
exit 7
)";

    auto queue = New<TActionQueue>();
    auto statusProfiler = CreateSyncStatusProfiler();
    auto resource = MakePostprocessedResource(
        queue->GetInvoker(),
        MakeTarget(1, "v1"),
        command,
        TDuration::Seconds(5),
        New<TFakeFileStorage>(),
        statusProfiler,
        TDuration::Hours(1));

    auto loadFuture = resource->Load({});
    WaitForPredicate([&] {
        return statusProfiler->GetStatus().Errors.contains("/file_update");
    });

    EXPECT_FALSE(loadFuture.IsSet());
    const auto error = ToString(statusProfiler->GetStatus().Errors.at("/file_update"));
    EXPECT_THAT(error, ::testing::HasSubstr("STDOUT_MARKER"));
    EXPECT_THAT(error, ::testing::HasSubstr("STDERR_MARKER"));
    EXPECT_LT(error.size(), 40_KB);
}

TEST_F(TFileResourceTest, PostprocessInfiniteOutputHonorsTimeout)
{
    const std::string command = R"(
/usr/bin/yes stdout &
/usr/bin/yes stderr >&2 &
wait
)";

    auto queue = New<TActionQueue>();
    auto statusProfiler = CreateSyncStatusProfiler();
    auto resource = MakePostprocessedResource(
        queue->GetInvoker(),
        MakeTarget(1, "v1"),
        command,
        TDuration::MilliSeconds(50),
        New<TFakeFileStorage>(),
        statusProfiler,
        TDuration::Hours(1));

    auto loadFuture = resource->Load({});
    WaitForPredicate([&] {
        return statusProfiler->GetStatus().Errors.contains("/file_update");
    });

    EXPECT_FALSE(loadFuture.IsSet());
    const auto error = ToString(statusProfiler->GetStatus().Errors.at("/file_update"));
    EXPECT_THAT(error, ::testing::HasSubstr("timeout"));
    EXPECT_THAT(error, ::testing::HasSubstr("command_digest"));
    EXPECT_LT(error.size(), 40_KB);
}

TEST_F(TFileResourceTest, AccessorRejectsUnknownFileProvider)
{
    auto queue = New<TActionQueue>();
    auto resource = MakeResource(queue->GetInvoker(), MakeTarget(1, "v1"));
    WaitFor(resource->Load({})).ThrowOnError();

    auto accessor = resource->Lock();
    EXPECT_THROW_WITH_SUBSTRING(
        accessor.GetRootPath(TFileProviderId("missing")),
        "Unknown materialized file provider");
}

TEST_F(TFileResourceTest, AccessorAssignmentsPreserveLiveCount)
{
    auto queue = New<TActionQueue>();
    auto resource = MakeResource(queue->GetInvoker(), MakeTarget(1, "v1"));
    WaitFor(resource->Load({})).ThrowOnError();

    {
        auto first = resource->Lock();
        auto second = resource->Lock();
        EXPECT_EQ(resource->GetRevisionState().LiveAccessorCounts.at(TFileSnapshotId(1)), 2);

        second = first;
        EXPECT_EQ(resource->GetRevisionState().LiveAccessorCounts.at(TFileSnapshotId(1)), 2);

        auto third = std::move(second);
        first = std::move(third);
        EXPECT_EQ(resource->GetRevisionState().LiveAccessorCounts.at(TFileSnapshotId(1)), 1);
    }

    EXPECT_FALSE(resource->GetRevisionState().LiveAccessorCounts.contains(TFileSnapshotId(1)));
}

TEST_F(TFileResourceTest, RolloutPreparesActiveBeforePreparing)
{
    TFakeFileProvider::Block("active-v1");

    auto queue = New<TActionQueue>();
    auto resource = MakeResource(
        queue->GetInvoker(),
        MakeRolloutTarget(
            1,
            MakeFileSnapshot(10, "active-v1"),
            MakeFileSnapshot(11, "preparing-v1")));

    auto loadFuture = resource->Load({});
    WaitFor(TFakeFileProvider::GetDownloadStartedFuture().WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();
    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("preparing-v1"), 0);
    auto downloadingState = resource->GetRevisionState();
    ASSERT_TRUE(downloadingState.PreparingFileSnapshot);
    EXPECT_EQ(downloadingState.PreparingFileSnapshot->State, EFileSnapshotState::Preparing);
    EXPECT_EQ(
        downloadingState.PreparingFileSnapshot->PreparationStage,
        EFileSnapshotPreparationStage::Materializing);

    TFakeFileProvider::Unblock();
    WaitFor(loadFuture.WithTimeout(TDuration::Seconds(5))).ThrowOnError();
    WaitForPredicate(
        [&] {
            auto state = resource->GetRevisionState();
            return state.PreparingFileSnapshot &&
                state.PreparingFileSnapshot->State == EFileSnapshotState::Validated;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });

    auto state = resource->GetRevisionState();
    ASSERT_TRUE(state.PreparingFileSnapshot);
    EXPECT_EQ(state.ActiveFileSnapshotId, TFileSnapshotId(10));
    EXPECT_EQ(state.PreparingFileSnapshot->State, EFileSnapshotState::Validated);
    EXPECT_EQ(resource->Lock().GetFileSnapshotId(), TFileSnapshotId(10));
    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("active-v1"), 1);
    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("preparing-v1"), 1);
}

TEST_F(TFileResourceTest, UnavailableActiveSnapshotDoesNotBlockPreparingSnapshot)
{
    auto queue = New<TActionQueue>();
    auto resource = MakeResource(
        queue->GetInvoker(),
        MakeRolloutTarget(
            1,
            MakeFileSnapshot(10, "download-failure"),
            MakeFileSnapshot(11, "v2")),
        New<TFakeFileStorage>(),
        CreateSyncStatusProfiler(),
        TDuration::Hours(1).MilliSeconds());

    auto loadFuture = resource->Load({});
    WaitForPreparingState(resource, TFileSnapshotId(11), EFileSnapshotState::Validated);

    EXPECT_FALSE(loadFuture.IsSet());
    EXPECT_EQ(resource->GetRevisionState().ActiveFileSnapshotId, std::nullopt);
    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("download-failure"), 1);
    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("v2"), 1);

    resource->Reconfigure(MakeDynamicContext(
        MakeRolloutTarget(2, MakeFileSnapshot(11, "v2")),
        TDuration::Hours(1).MilliSeconds()));
    WaitFor(loadFuture.WithTimeout(TDuration::Seconds(5))).ThrowOnError();

    EXPECT_EQ(resource->Lock().GetFileSnapshotId(), TFileSnapshotId(11));
    EXPECT_EQ(resource->Lock()->Value, "payload:v2");
}

TEST_F(TFileResourceTest, RolloutReportsPreparationAndActivationStages)
{
    TFakeFileProvider::Block("v2");
    TTestFileResource::BlockInitialization("payload:v2");
    TTestFileResource::BlockValidation("payload:v2");

    auto queue = New<TActionQueue>();
    auto resource = MakeResource(
        queue->GetInvoker(),
        MakeRolloutTarget(
            1,
            MakeFileSnapshot(10, "v1"),
            MakeFileSnapshot(11, "v2")));

    WaitFor(resource->Load({}).WithTimeout(TDuration::Seconds(5))).ThrowOnError();
    std::optional<TTestFileResource::TAccessor> oldAccessor(resource->Lock());

    WaitFor(TFakeFileProvider::GetDownloadStartedFuture().WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();
    WaitForPreparingState(
        resource,
        TFileSnapshotId(11),
        EFileSnapshotState::Preparing,
        EFileSnapshotPreparationStage::Materializing);

    TFakeFileProvider::Unblock();
    WaitFor(TTestFileResource::GetInitializationStartedFuture().WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();
    WaitForPreparingState(
        resource,
        TFileSnapshotId(11),
        EFileSnapshotState::Preparing,
        EFileSnapshotPreparationStage::Initializing);

    TTestFileResource::UnblockInitialization();
    WaitFor(TTestFileResource::GetValidationStartedFuture().WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();
    WaitForPreparingState(
        resource,
        TFileSnapshotId(11),
        EFileSnapshotState::Preparing,
        EFileSnapshotPreparationStage::Validating);

    TTestFileResource::UnblockValidation();
    WaitForPreparingState(resource, TFileSnapshotId(11), EFileSnapshotState::Validated);

    resource->Reconfigure(MakeDynamicContext(MakeRolloutTarget(
        2,
        MakeFileSnapshot(11, "v2"))));
    WaitForPreparingState(resource, TFileSnapshotId(11), EFileSnapshotState::Draining);
    EXPECT_EQ(resource->GetRevisionState().ActiveFileSnapshotId, TFileSnapshotId(10));

    oldAccessor.reset();
    WaitForAppliedRevision(resource, 2);

    auto state = resource->GetRevisionState();
    EXPECT_EQ(state.ActiveFileSnapshotId, TFileSnapshotId(11));
    EXPECT_FALSE(state.PreparingFileSnapshot);
}

TEST_F(TFileResourceTest, RolloutPromotesValidatedPreparingWithoutRepeatingPreparation)
{
    auto queue = New<TActionQueue>();
    auto preparing = MakeFileSnapshot(10, "v1");
    auto resource = MakeResource(
        queue->GetInvoker(),
        MakeRolloutTarget(1, nullptr, preparing));

    auto loadFuture = resource->Load({});
    WaitForPredicate(
        [&] {
            auto state = resource->GetRevisionState();
            return state.PreparingFileSnapshot &&
                state.PreparingFileSnapshot->State == EFileSnapshotState::Validated;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });

    EXPECT_FALSE(loadFuture.IsSet());
    EXPECT_THROW_WITH_SUBSTRING(resource->Lock(), "no initialized data");
    EXPECT_EQ(TTestFileResource::GetInitializeCount("payload:v1"), 1);

    resource->Reconfigure(MakeDynamicContext(MakeRolloutTarget(
        2,
        MakeFileSnapshot(10, "v1"))));
    WaitFor(loadFuture.WithTimeout(TDuration::Seconds(5))).ThrowOnError();

    auto state = resource->GetRevisionState();
    EXPECT_FALSE(state.PreparingFileSnapshot);
    EXPECT_EQ(state.ActiveFileSnapshotId, TFileSnapshotId(10));
    EXPECT_EQ(resource->Lock().GetFileSnapshotId(), TFileSnapshotId(10));
    EXPECT_EQ(resource->Lock().GetDeliveryRevisionId(), 2);
    EXPECT_EQ(TTestFileResource::GetInitializeCount("payload:v1"), 1);
    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("v1"), 1);
}

TEST_F(TFileResourceTest, RolloutKeepsAppliedSnapshotWhileTargetActiveFails)
{
    auto queue = New<TActionQueue>();
    auto resource = MakeResource(
        queue->GetInvoker(),
        MakeRolloutTarget(1, MakeFileSnapshot(10, "v1")));
    WaitFor(resource->Load({})).ThrowOnError();

    resource->Reconfigure(MakeDynamicContext(MakeRolloutTarget(
        2,
        MakeFileSnapshot(11, "validation-failure"))));
    WaitForPredicate(
        [&] {
            auto state = resource->GetRevisionState();
            return state.PreparingFileSnapshot && !state.PreparingFileSnapshot->Error.IsOK();
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });

    auto state = resource->GetRevisionState();
    ASSERT_TRUE(state.PreparingFileSnapshot);
    EXPECT_EQ(state.AppliedRevisionId, 1);
    EXPECT_EQ(state.TargetRevisionId, 2);
    EXPECT_EQ(state.ActiveFileSnapshotId, TFileSnapshotId(10));
    EXPECT_EQ(state.PreparingFileSnapshot->SnapshotId, TFileSnapshotId(11));
    EXPECT_EQ(state.PreparingFileSnapshot->State, EFileSnapshotState::Preparing);
    EXPECT_EQ(
        state.PreparingFileSnapshot->PreparationStage,
        EFileSnapshotPreparationStage::Waiting);
    EXPECT_TRUE(state.PreparingFileSnapshot->NextRetryAt);
    EXPECT_EQ(resource->Lock()->Value, "payload:v1");
    EXPECT_EQ(resource->Lock().GetFileSnapshotId(), TFileSnapshotId(10));
}

TEST_F(TFileResourceTest, RolloutKeepsAppliedSnapshotWhenTargetHasNoActiveSnapshot)
{
    auto queue = New<TActionQueue>();
    auto resource = MakeResource(
        queue->GetInvoker(),
        MakeRolloutTarget(1, MakeFileSnapshot(10, "v1")));
    WaitFor(resource->Load({})).ThrowOnError();

    resource->Reconfigure(MakeDynamicContext(MakeRolloutTarget(
        2,
        nullptr,
        MakeFileSnapshot(11, "v2"))));
    WaitForPredicate([&] {
        auto state = resource->GetRevisionState();
        return state.PreparingFileSnapshot &&
            state.PreparingFileSnapshot->State == EFileSnapshotState::Validated;
    });

    auto state = resource->GetRevisionState();
    EXPECT_EQ(state.AppliedRevisionId, 1);
    EXPECT_EQ(state.TargetRevisionId, 2);
    EXPECT_EQ(state.ActiveFileSnapshotId, TFileSnapshotId(10));
    EXPECT_EQ(resource->Lock()->Value, "payload:v1");
    EXPECT_EQ(resource->Lock().GetFileSnapshotId(), TFileSnapshotId(10));
}

TEST_F(TFileResourceTest, NamedProvidersInitializeAsOneSnapshot)
{
    auto queue = New<TActionQueue>();
    auto resource = MakeNamedResource(
        queue->GetInvoker(),
        {{"left", "left"}, {"right", "right"}},
        MakeNamedTarget(1, {{"left", "left-v1"}, {"right", "right-v1"}}));

    WaitFor(resource->Load({}).WithTimeout(TDuration::Seconds(5))).ThrowOnError();

    auto accessor = resource->Lock();
    EXPECT_EQ(accessor->Value, "left=left:left-v1;right=right:right-v1");
    EXPECT_EQ(accessor.GetDeliveryRevisionId(), 1);
    EXPECT_EQ(accessor.GetProviderRevision(TFileProviderId("left"))->ObjectId.Underlying(), "left-v1");
    EXPECT_EQ(accessor.GetProviderRevision(TFileProviderId("right"))->ObjectId.Underlying(), "right-v1");
    EXPECT_TRUE(TFsPath(accessor.GetRootPath(TFileProviderId("left"))).Child("artifact").Exists());
    EXPECT_TRUE(TFsPath(accessor.GetRootPath(TFileProviderId("right"))).Child("artifact").Exists());
    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("left-v1"), 1);
    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("right-v1"), 1);
    EXPECT_EQ(TFakeFileProvider::GetDiscoverCount("left"), 0);
    EXPECT_EQ(TFakeFileProvider::GetDiscoverCount("right"), 0);
}

TEST_F(TFileResourceTest, NamedProviderCacheIdentityPreservesFieldBoundaries)
{
    auto queue = New<TActionQueue>();
    auto resource = MakeNamedResource(
        queue->GetInvoker(),
        {{"a", "left"}, {"a-b", "right"}},
        MakeNamedTarget(1, {{"a", "b-c"}, {"a-b", "c"}}));

    WaitFor(resource->Load({}).WithTimeout(TDuration::Seconds(5))).ThrowOnError();

    EXPECT_EQ(resource->Lock()->Value, "a=left:b-c;a-b=right:c");
    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("b-c"), 1);
    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("c"), 1);
}

TEST_F(TFileResourceTest, MaterializesOneAndExplicitSubset)
{
    auto queue = New<TActionQueue>();
    auto fileSnapshot = MakeNamedFileSnapshot(
        1,
        {{"left", "left-v1"}, {"right", "right-v1"}, {"unused", "unused-v1"}});
    auto resource = MakeNamedResource(
        queue->GetInvoker(),
        {{"left", "left"}, {"right", "right"}, {"unused", "unused"}},
        MakeNamedTarget(
            1,
            {{"left", "left-v1"}, {"right", "right-v1"}, {"unused", "unused-v1"}}));

    auto left = WaitFor(resource->MaterializeOne(fileSnapshot, TFileProviderId("left"))).ValueOrThrow();
    EXPECT_EQ(left->GetRevision()->ObjectId.Underlying(), "left-v1");

    auto subset = WaitFor(resource->MaterializeMany(
        fileSnapshot,
        {TFileProviderId("left"), TFileProviderId("right")}))
        .ValueOrThrow();
    EXPECT_EQ(subset->GetFileProviders().size(), 2);
    EXPECT_TRUE(subset->GetFileProviders().contains(TFileProviderId("left")));
    EXPECT_TRUE(subset->GetFileProviders().contains(TFileProviderId("right")));
    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("left-v1"), 1);
    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("right-v1"), 1);
    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("unused-v1"), 0);

    EXPECT_THROW_WITH_SUBSTRING(
        static_cast<void>(resource->MaterializeOne(fileSnapshot, TFileProviderId("missing"))),
        "is not configured");
    EXPECT_THROW_WITH_SUBSTRING(
        static_cast<void>(resource->MaterializeMany(
            fileSnapshot,
            {TFileProviderId("left"), TFileProviderId("left")})),
        "requested more than once");

    auto nullSnapshot = MakeNamedFileSnapshot(2, {{"left", "left-v1"}});
    nullSnapshot->FileProviders[TFileProviderId("left")] = nullptr;
    EXPECT_THROW_WITH_SUBSTRING(
        static_cast<void>(resource->MaterializeOne(nullSnapshot, TFileProviderId("left"))),
        "null file provider");

    auto mismatchedSnapshot = MakeNamedFileSnapshot(3, {{"left", "left-v1"}});
    mismatchedSnapshot->FileProviders[TFileProviderId("left")]->FileProviderClassName = "mismatched-provider";
    EXPECT_THROW_WITH_SUBSTRING(
        static_cast<void>(resource->MaterializeOne(mismatchedSnapshot, TFileProviderId("left"))),
        "differs from configured class");
}

TEST_F(TFileResourceTest, NamedProvidersNeverPublishPartialInitialization)
{
    auto queue = New<TActionQueue>();
    auto resource = MakeNamedResource(
        queue->GetInvoker(),
        {{"left", "left"}, {"right", "right"}},
        MakeNamedTarget(
            1,
            {{"left", "left-v1"}, {"right", "right-validation-failure"}}));

    auto loadFuture = resource->Load({});
    WaitForPredicate(
        [&] {
            auto state = resource->GetRevisionState();
            return state.PreparingFileSnapshot &&
                !state.PreparingFileSnapshot->Error.IsOK() &&
                state.PreparingFileSnapshot->NextRetryAt;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    EXPECT_FALSE(loadFuture.IsSet());
    EXPECT_THROW_WITH_SUBSTRING(resource->Lock(), "no initialized data");

    resource->Reconfigure(MakeNamedDynamicContext(
        MakeNamedTarget(2, {{"left", "left-v2"}, {"right", "right-v2"}})));
    WaitFor(loadFuture.WithTimeout(TDuration::Seconds(5))).ThrowOnError();
    EXPECT_EQ(resource->Lock()->Value, "left=left:left-v2;right=right:right-v2");
    EXPECT_EQ(resource->GetRevisionState().AppliedRevisionId, 2);
}

TEST_F(TFileResourceTest, NamedProvidersReuseEqualTupleAndReinitializeChangedTuple)
{
    auto queue = New<TActionQueue>();
    auto resource = MakeNamedResource(
        queue->GetInvoker(),
        {{"left", "left"}, {"right", "right"}},
        MakeNamedTarget(1, {{"left", "left-v1"}, {"right", "right-v1"}}));
    WaitFor(resource->Load({})).ThrowOnError();

    const std::string initialValue = "left=left:left-v1;right=right:right-v1";
    EXPECT_EQ(TTestFileResource::GetInitializeCount(initialValue), 1);

    resource->Reconfigure(MakeNamedDynamicContext(
        MakeRolloutTarget(
            2,
            MakeNamedFileSnapshot(1, {{"left", "left-v1"}, {"right", "right-v1"}}))));
    WaitForAppliedRevision(resource, 2);
    EXPECT_EQ(TTestFileResource::GetInitializeCount(initialValue), 1);
    EXPECT_EQ(resource->Lock().GetDeliveryRevisionId(), 2);

    resource->Reconfigure(MakeNamedDynamicContext(
        MakeNamedTarget(3, {{"left", "left-v1"}, {"right", "right-v2"}})));
    WaitForAppliedRevision(resource, 3);
    EXPECT_EQ(resource->Lock()->Value, "left=left:left-v1;right=right:right-v2");
    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("left-v1"), 1);
    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("right-v1"), 1);
    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("right-v2"), 1);
}

TEST_F(TFileResourceTest, ChangedTargetSpecKeepsActiveFileSnapshot)
{
    auto queue = New<TActionQueue>();
    auto firstTarget = MakeTarget(1, "same");
    firstTarget->Spec = ConvertToNode("first");
    auto resource = MakeResource(queue->GetInvoker(), firstTarget);
    WaitFor(resource->Load({})).ThrowOnError();
    auto oldAccessor = resource->Lock();
    EXPECT_EQ(TTestFileResource::GetInitializeCount("payload:same"), 1);

    auto secondTarget = MakeRolloutTarget(2, MakeFileSnapshot(1, "same"));
    secondTarget->Spec = ConvertToNode("second");
    resource->Reconfigure(MakeDynamicContext(secondTarget));
    WaitForAppliedRevision(resource, 2);

    EXPECT_EQ(TTestFileResource::GetInitializeCount("payload:same"), 1);
    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("same"), 1);
    EXPECT_EQ(oldAccessor.GetDeliveryRevisionId(), 1);
    EXPECT_EQ(resource->Lock().GetDeliveryRevisionId(), 2);
}

TEST_F(TFileResourceTest, ReconfigureKeepsValidatedPreparingSnapshot)
{
    auto queue = New<TActionQueue>();
    auto resource = MakeResource(
        queue->GetInvoker(),
        MakeRolloutTarget(1, MakeFileSnapshot(1, "v1")));
    WaitFor(resource->Load({})).ThrowOnError();

    resource->Reconfigure(MakeDynamicContext(MakeRolloutTarget(
        2,
        MakeFileSnapshot(1, "v1"),
        MakeFileSnapshot(2, "v2"))));
    WaitForPredicate([&] {
        auto state = resource->GetRevisionState();
        return state.PreparingFileSnapshot &&
            state.PreparingFileSnapshot->State == EFileSnapshotState::Validated;
    });
    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("v2"), 1);
    EXPECT_EQ(TTestFileResource::GetInitializeCount("payload:v2"), 1);

    resource->Reconfigure(MakeDynamicContext(MakeRolloutTarget(
        3,
        MakeFileSnapshot(1, "v1"),
        MakeFileSnapshot(2, "v2"))));
    WaitForAppliedRevision(resource, 3);

    auto state = resource->GetRevisionState();
    ASSERT_TRUE(state.PreparingFileSnapshot);
    EXPECT_EQ(state.PreparingFileSnapshot->SnapshotId, TFileSnapshotId(2));
    EXPECT_EQ(state.PreparingFileSnapshot->State, EFileSnapshotState::Validated);
    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("v2"), 1);
    EXPECT_EQ(TTestFileResource::GetInitializeCount("payload:v2"), 1);
}

TEST_F(TFileResourceTest, ReconfigureKeepsInFlightPreparingSnapshot)
{
    auto queue = New<TActionQueue>();
    auto resource = MakeResource(
        queue->GetInvoker(),
        MakeRolloutTarget(1, MakeFileSnapshot(1, "v1")));
    WaitFor(resource->Load({})).ThrowOnError();

    TFakeFileProvider::Block("v2");
    resource->Reconfigure(MakeDynamicContext(MakeRolloutTarget(
        2,
        MakeFileSnapshot(1, "v1"),
        MakeFileSnapshot(2, "v2"))));
    WaitFor(TFakeFileProvider::GetDownloadStartedFuture().WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();

    resource->Reconfigure(MakeDynamicContext(MakeRolloutTarget(
        3,
        MakeFileSnapshot(1, "v1"),
        MakeFileSnapshot(2, "v2"))));
    TFakeFileProvider::Unblock();
    WaitForPredicate([&] {
        auto state = resource->GetRevisionState();
        return state.PreparingFileSnapshot &&
            state.PreparingFileSnapshot->State == EFileSnapshotState::Validated;
    });

    EXPECT_EQ(resource->GetRevisionState().AppliedRevisionId, 3);
    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("v2"), 1);
    EXPECT_EQ(TTestFileResource::GetInitializeCount("payload:v2"), 1);
}

TEST_F(TFileResourceTest, NamedProviderTargetMustMatchConfiguredSnapshotExactly)
{
    auto queue = New<TActionQueue>();
    auto statusProfiler = CreateSyncStatusProfiler();
    auto resource = MakeNamedResource(
        queue->GetInvoker(),
        {{"left", "left"}, {"right", "right"}},
        MakeNamedTarget(1, {{"left", "left-v1"}}),
        New<TFakeFileStorage>(),
        statusProfiler);

    auto loadFuture = resource->Load({});
    WaitForPredicate(
        [&] {
            return statusProfiler->GetStatus().Errors.contains("/file_update");
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    EXPECT_FALSE(loadFuture.IsSet());
    EXPECT_THAT(
        ToString(statusProfiler->GetStatus().Errors.at("/file_update")),
        ::testing::HasSubstr("while the spec configures"));

    resource->Reconfigure(MakeNamedDynamicContext(
        MakeNamedTarget(2, {{"left", "left-v2"}, {"right", "right-v2"}})));
    WaitFor(loadFuture.WithTimeout(TDuration::Seconds(5))).ThrowOnError();
}

TEST_F(TFileResourceTest, NamedProviderTargetRejectsClassMismatch)
{
    auto target = MakeNamedTarget(1, {{"file", "v1"}});
    target->ActiveFileSnapshot->FileProviders[TFileProviderId("file")]->FileProviderClassName = "mismatched-provider";

    auto queue = New<TActionQueue>();
    auto statusProfiler = CreateSyncStatusProfiler();
    auto resource = MakeNamedResource(
        queue->GetInvoker(),
        {{"file", "named"}},
        target,
        New<TFakeFileStorage>(),
        statusProfiler);

    auto loadFuture = resource->Load({});
    WaitForPredicate(
        [&] {
            return statusProfiler->GetStatus().Errors.contains("/file_update");
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    EXPECT_FALSE(loadFuture.IsSet());
    EXPECT_THAT(
        ToString(statusProfiler->GetStatus().Errors.at("/file_update")),
        ::testing::HasSubstr("differs from configured class"));
}

TEST_F(TFileResourceTest, NamedProviderDirectRetryPeriodReconfigurationTriggersRetry)
{
    auto queue = New<TActionQueue>();
    auto statusProfiler = CreateSyncStatusProfiler();
    auto target = MakeNamedTarget(1, {{"file", "download-failure-once"}});
    auto resource = MakeNamedResource(
        queue->GetInvoker(),
        {{"file", "named"}},
        target,
        New<TFakeFileStorage>(),
        statusProfiler,
        TDuration::Hours(1));

    auto loadFuture = resource->Load({});
    WaitForPredicate(
        [&] {
            return statusProfiler->GetStatus().Errors.contains("/file_update");
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("download-failure-once"), 1);

    resource->Reconfigure(MakeNamedDynamicContext(
        target,
        TDuration::MilliSeconds(10),
        TDuration::MilliSeconds(1)));
    WaitFor(loadFuture.WithTimeout(TDuration::Seconds(5))).ThrowOnError();

    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("download-failure-once"), 2);
    EXPECT_FALSE(statusProfiler->GetStatus().Errors.contains("/file_update"));
}

TEST_F(TFileResourceTest, UnchangedTargetRetriesAndClearsUpdateError)
{
    auto queue = New<TActionQueue>();
    auto statusProfiler = CreateSyncStatusProfiler();
    auto resource = MakeResource(
        queue->GetInvoker(),
        MakeTarget(1, "download-failure-once"),
        New<TFakeFileStorage>(),
        statusProfiler);

    auto loadFuture = resource->Load({});
    WaitForPredicate(
        [&] {
            return statusProfiler->GetStatus().Errors.contains("/file_update");
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });

    WaitFor(loadFuture.WithTimeout(TDuration::Seconds(5))).ThrowOnError();
    EXPECT_EQ(resource->Lock()->Value, "payload:download-failure-once");
    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("download-failure-once"), 2);
    EXPECT_FALSE(statusProfiler->GetStatus().Errors.contains("/file_update"));
}

TEST_F(TFileResourceTest, DynamicRetryPeriodReconfigurationTriggersRetry)
{
    auto queue = New<TActionQueue>();
    auto statusProfiler = CreateSyncStatusProfiler();
    auto target = MakeTarget(1, "download-failure-once");
    auto resource = MakeResource(
        queue->GetInvoker(),
        target,
        New<TFakeFileStorage>(),
        statusProfiler,
        TDuration::Hours(1).MilliSeconds());

    auto loadFuture = resource->Load({});
    WaitForPredicate(
        [&] {
            return statusProfiler->GetStatus().Errors.contains("/file_update");
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("download-failure-once"), 1);

    resource->Reconfigure(MakeDynamicContext(target, 1));
    WaitFor(loadFuture.WithTimeout(TDuration::Seconds(5))).ThrowOnError();

    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("download-failure-once"), 2);
    EXPECT_FALSE(statusProfiler->GetStatus().Errors.contains("/file_update"));
}

TEST_F(TFileResourceTest, PendingSnapshotStateIsCollectedBeforeInitialLoadCompletes)
{
    TFakeFileProvider::Block("v1");

    auto queue = New<TActionQueue>();
    auto managerContext = New<TResourceManagerContext>();
    managerContext->Invoker = queue->GetInvoker();
    managerContext->Logger = NLogging::TLogger("FileResourceManagerTest");
    managerContext->StatusProfiler = CreateSyncStatusProfiler();
    managerContext->FileStorage = New<TFakeFileStorage>();

    auto manager = CreateResourceManager(
        managerContext,
        {{TResourceId("test"), MakeResourceSpec()}},
        {},
        {{TResourceId("test"), MakeTarget(1, "v1")}});
    auto loadFuture = manager->Load(TResourceId("test"));

    WaitFor(TFakeFileProvider::GetDownloadStartedFuture().WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();
    auto statuses = manager->CollectResourceStatuses();
    ASSERT_TRUE(statuses.contains(TResourceId("test")));
    EXPECT_EQ(statuses[TResourceId("test")]->AppliedRevisionId, std::nullopt);
    EXPECT_EQ(statuses[TResourceId("test")]->TargetRevisionId, std::optional<i64>(1));
    ASSERT_TRUE(statuses[TResourceId("test")]->PreparingFileSnapshot);
    EXPECT_EQ(
        statuses[TResourceId("test")]->PreparingFileSnapshot->State,
        EFileSnapshotState::Preparing);
    EXPECT_EQ(
        statuses[TResourceId("test")]->PreparingFileSnapshot->PreparationStage,
        EFileSnapshotPreparationStage::Materializing);

    TFakeFileProvider::Unblock();
    WaitFor(loadFuture.WithTimeout(TDuration::Seconds(5))).ThrowOnError();
}

TEST_F(TFileResourceTest, ActivationWaitsForPreviousAccessorLease)
{
    auto queue = New<TActionQueue>();
    auto resource = MakeResource(queue->GetInvoker(), MakeTarget(1, "v1"));
    WaitFor(resource->Load({})).ThrowOnError();

    std::optional<TTestFileResource::TAccessor> oldAccessor(resource->Lock());
    std::optional<TTestFileResource::TAccessor> oldAccessorCopy(*oldAccessor);
    std::optional<TTestFileResource::TAccessor> movedOldAccessor(std::move(*oldAccessorCopy));
    oldAccessorCopy.reset();
    auto oldFilePath = (*oldAccessor)->FilePath;

    resource->Reconfigure(MakeDynamicContext(MakeTarget(2, "v2")));
    WaitForPredicate([&] {
        auto state = resource->GetRevisionState();
        return state.PreparingFileSnapshot &&
            state.PreparingFileSnapshot->SnapshotId == TFileSnapshotId(2) &&
            state.PreparingFileSnapshot->State == EFileSnapshotState::Draining;
    });

    auto newAccessor = resource->Lock();
    EXPECT_EQ((*oldAccessor)->Value, "payload:v1");
    EXPECT_EQ(newAccessor->Value, "payload:v2");

    auto state = resource->GetRevisionState();
    EXPECT_EQ(state.AppliedRevisionId, 1);
    EXPECT_EQ(state.TargetRevisionId, 2);
    EXPECT_EQ(state.ActiveFileSnapshotId, TFileSnapshotId(1));
    ASSERT_TRUE(state.PreparingFileSnapshot);
    EXPECT_EQ(state.PreparingFileSnapshot->SnapshotId, TFileSnapshotId(2));
    EXPECT_EQ(state.PreparingFileSnapshot->State, EFileSnapshotState::Draining);
    EXPECT_EQ(state.LiveAccessorCounts.at(TFileSnapshotId(1)), 2);
    EXPECT_EQ(state.LiveAccessorCounts.at(TFileSnapshotId(2)), 1);
    EXPECT_TRUE(TFsPath(oldFilePath).Exists());

    oldAccessor.reset();
    EXPECT_EQ(resource->GetRevisionState().AppliedRevisionId, 1);
    movedOldAccessor.reset();
    WaitForAppliedRevision(resource, 2);

    state = resource->GetRevisionState();
    EXPECT_EQ(state.ActiveFileSnapshotId, TFileSnapshotId(2));
    EXPECT_FALSE(state.PreparingFileSnapshot);
    EXPECT_FALSE(state.LiveAccessorCounts.contains(TFileSnapshotId(1)));
    EXPECT_EQ(state.LiveAccessorCounts.at(TFileSnapshotId(2)), 1);
}

TEST_F(TFileResourceTest, LongLivedAccessorAcrossRewrapReportsActivationStall)
{
    auto queue = New<TActionQueue>();
    auto statusProfiler = CreateSyncStatusProfiler();
    auto resource = MakeResource(
        queue->GetInvoker(),
        MakeTarget(1, "v1"),
        New<TFakeFileStorage>(),
        statusProfiler);
    WaitFor(resource->Load({})).ThrowOnError();

    std::optional<TTestFileResource::TAccessor> oldAccessor(resource->Lock());
    resource->Reconfigure(MakeDynamicContext(MakeRolloutTarget(
        2,
        MakeFileSnapshot(1, "v1"))));
    WaitForAppliedRevision(resource, 2);
    EXPECT_EQ(oldAccessor->GetDeliveryRevisionId(), 1);

    resource->Reconfigure(MakeNamedDynamicContext(
        MakeRolloutTarget(
            3,
            MakeFileSnapshot(2, "v2")),
        TDuration::MilliSeconds(10),
        TDuration::MilliSeconds(100),
        {},
        TDuration::MilliSeconds(1),
        1024,
        TDuration::MilliSeconds(10)));

    WaitForPredicate([&] {
        return statusProfiler->GetStatus().Errors.contains("/file_snapshot_activation");
    });

    auto state = resource->GetRevisionState();
    EXPECT_EQ(state.AppliedRevisionId, 2);
    EXPECT_EQ(state.TargetRevisionId, 3);
    EXPECT_EQ(state.ActiveFileSnapshotId, TFileSnapshotId(1));
    ASSERT_TRUE(state.PreparingFileSnapshot);
    EXPECT_EQ(state.PreparingFileSnapshot->SnapshotId, TFileSnapshotId(2));
    EXPECT_EQ(state.PreparingFileSnapshot->State, EFileSnapshotState::Draining);
    const auto status = statusProfiler->GetStatus();
    const auto& error = status.Errors.at("/file_snapshot_activation");
    EXPECT_THAT(ToString(error), ::testing::HasSubstr("blocked by live accessors"));
    EXPECT_THAT(ToString(error), ::testing::HasSubstr("live_accessor_count"));

    oldAccessor.reset();
    WaitForAppliedRevision(resource, 3);
    EXPECT_EQ(resource->GetRevisionState().ActiveFileSnapshotId, TFileSnapshotId(2));
    EXPECT_FALSE(statusProfiler->GetStatus().Errors.contains("/file_snapshot_activation"));
}

TEST_F(TFileResourceTest, ReconfigureDuringActivationUsesLatestMatchingTarget)
{
    auto queue = New<TActionQueue>();
    auto resource = MakeResource(queue->GetInvoker(), MakeTarget(1, "v1"));
    WaitFor(resource->Load({})).ThrowOnError();
    std::optional<TTestFileResource::TAccessor> oldAccessor(resource->Lock());

    resource->Reconfigure(MakeDynamicContext(MakeRolloutTarget(
        2,
        MakeFileSnapshot(2, "v2"))));
    WaitForPredicate([&] {
        auto state = resource->GetRevisionState();
        return state.PreparingFileSnapshot &&
            state.PreparingFileSnapshot->SnapshotId == TFileSnapshotId(2) &&
            state.PreparingFileSnapshot->State == EFileSnapshotState::Draining;
    });

    resource->Reconfigure(MakeDynamicContext(MakeRolloutTarget(
        3,
        MakeFileSnapshot(2, "v2"),
        MakeFileSnapshot(3, "v3"))));
    EXPECT_EQ(resource->GetRevisionState().AppliedRevisionId, 1);
    EXPECT_EQ(resource->GetRevisionState().TargetRevisionId, 3);
    EXPECT_EQ(TTestFileResource::GetInitializeCount("payload:v3"), 0);

    oldAccessor.reset();
    WaitForAppliedRevision(resource, 3);
    WaitForPredicate([&] {
        auto state = resource->GetRevisionState();
        return state.PreparingFileSnapshot &&
            state.PreparingFileSnapshot->SnapshotId == TFileSnapshotId(3) &&
            state.PreparingFileSnapshot->State == EFileSnapshotState::Validated;
    });

    EXPECT_EQ(resource->GetRevisionState().ActiveFileSnapshotId, TFileSnapshotId(2));
    auto accessor = resource->Lock();
    EXPECT_EQ(accessor->Value, "payload:v2");
    EXPECT_EQ(accessor.GetDeliveryRevisionId(), 3);
    EXPECT_EQ(TTestFileResource::GetInitializeCount("payload:v2"), 1);
}

TEST_F(TFileResourceTest, ReconfigureDuringActivationPreparesTargetWithoutActiveSnapshot)
{
    auto queue = New<TActionQueue>();
    auto resource = MakeResource(queue->GetInvoker(), MakeTarget(1, "v1"));
    WaitFor(resource->Load({})).ThrowOnError();
    std::optional<TTestFileResource::TAccessor> oldAccessor(resource->Lock());

    resource->Reconfigure(MakeDynamicContext(MakeTarget(2, "v2")));
    WaitForPredicate([&] {
        auto state = resource->GetRevisionState();
        return state.PreparingFileSnapshot &&
            state.PreparingFileSnapshot->SnapshotId == TFileSnapshotId(2) &&
            state.PreparingFileSnapshot->State == EFileSnapshotState::Draining;
    });

    resource->Reconfigure(MakeDynamicContext(MakeRolloutTarget(
        3,
        nullptr,
        MakeFileSnapshot(3, "v3"))));

    auto state = resource->GetRevisionState();
    EXPECT_EQ(state.TargetRevisionId, 3);
    EXPECT_EQ(state.ActiveFileSnapshotId, TFileSnapshotId(1));
    EXPECT_EQ(resource->Lock()->Value, "payload:v2");
    EXPECT_EQ((*oldAccessor)->Value, "payload:v1");

    oldAccessor.reset();
    WaitForAppliedRevision(resource, 2);
    WaitForPredicate([&] {
        auto state = resource->GetRevisionState();
        return state.PreparingFileSnapshot &&
            state.PreparingFileSnapshot->SnapshotId == TFileSnapshotId(3) &&
            state.PreparingFileSnapshot->State == EFileSnapshotState::Validated;
    });

    state = resource->GetRevisionState();
    EXPECT_EQ(state.TargetRevisionId, 3);
    EXPECT_EQ(state.AppliedRevisionId, 2);
    EXPECT_EQ(state.ActiveFileSnapshotId, TFileSnapshotId(2));
    EXPECT_EQ(resource->Lock()->Value, "payload:v2");
    EXPECT_EQ(TTestFileResource::GetInitializeCount("payload:v3"), 1);
}

TEST_F(TFileResourceTest, EqualContentSkipsReinitialization)
{
    auto queue = New<TActionQueue>();
    auto resource = MakeResource(queue->GetInvoker(), MakeTarget(1, "same"));
    WaitFor(resource->Load({})).ThrowOnError();

    resource->Reconfigure(MakeDynamicContext(MakeRolloutTarget(
        2,
        MakeFileSnapshot(1, "same"))));
    WaitForAppliedRevision(resource, 2);

    auto accessor = resource->Lock();
    EXPECT_EQ(accessor.GetDeliveryRevisionId(), 2);
    EXPECT_EQ(resource->GetRevisionState().AppliedRevisionId, 2);
    EXPECT_EQ(TFakeFileProvider::GetDownloadCount("same"), 1);
    EXPECT_EQ(TTestFileResource::GetInitializeCount("payload:same"), 1);
}

TEST_F(TFileResourceTest, FailedUpdateRetainsPreviousState)
{
    auto queue = New<TActionQueue>();
    auto resource = MakeResource(queue->GetInvoker(), MakeTarget(1, "v1"));
    WaitFor(resource->Load({})).ThrowOnError();

    resource->Reconfigure(MakeDynamicContext(MakeTarget(2, "validation-failure")));
    WaitForPredicate([] {
        return TTestFileResource::GetInitializeCount("payload:validation-failure") >= 1;
    });

    EXPECT_EQ(resource->Lock()->Value, "payload:v1");
    EXPECT_EQ(resource->GetRevisionState().AppliedRevisionId, 1);
    EXPECT_EQ(resource->GetRevisionState().TargetRevisionId, 2);

    resource->Reconfigure(MakeDynamicContext(MakeTarget(3, "v3")));
    WaitForAppliedRevision(resource, 3);

    EXPECT_EQ(resource->Lock()->Value, "payload:v3");
    EXPECT_EQ(resource->GetRevisionState().AppliedRevisionId, 3);
}

TEST_F(TFileResourceTest, InitialFailureCanRecoverOnNewTarget)
{
    auto queue = New<TActionQueue>();
    auto resource = MakeResource(queue->GetInvoker(), MakeTarget(1, "download-failure"));
    auto loadFuture = resource->Load({});

    WaitForPredicate([] {
        return TFakeFileProvider::GetDownloadCount("download-failure") >= 1;
    });
    EXPECT_FALSE(loadFuture.IsSet());

    resource->Reconfigure(MakeDynamicContext(MakeTarget(2, "v2")));
    WaitFor(loadFuture.WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();

    EXPECT_EQ(resource->Lock()->Value, "payload:v2");
    EXPECT_EQ(resource->GetRevisionState().AppliedRevisionId, 2);
}

TEST_F(TFileResourceTest, SupersededCandidateIsNotPublished)
{
    TFakeFileProvider::Block("v1");

    auto queue = New<TActionQueue>();
    auto resource = MakeResource(queue->GetInvoker(), MakeTarget(1, "v1"));
    auto loadFuture = resource->Load({});

    WaitFor(TFakeFileProvider::GetDownloadStartedFuture().WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();
    EXPECT_FALSE(loadFuture.IsSet());

    resource->Reconfigure(MakeDynamicContext(MakeTarget(2, "v2")));
    WaitFor(loadFuture.WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();

    auto accessor = resource->Lock();
    EXPECT_EQ(accessor->Value, "payload:v2");
    EXPECT_EQ(accessor.GetDeliveryRevisionId(), 2);
    EXPECT_EQ(resource->GetRevisionState().AppliedRevisionId, 2);

    TFakeFileProvider::Unblock();
    WaitForPredicate(
        [] {
            return TFakeFileProvider::GetCompletedDownloadCount("v1") == 1;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    EXPECT_EQ(resource->Lock()->Value, "payload:v2");
    EXPECT_EQ(resource->GetRevisionState().AppliedRevisionId, 2);
}

TEST_F(TFileResourceTest, ResourceCanBeDestroyedWhileDownloadIsPending)
{
    TFakeFileProvider::Block("v1");

    auto queue = New<TActionQueue>();
    auto resource = MakeResource(queue->GetInvoker(), MakeTarget(1, "v1"));
    auto loadFuture = resource->Load({});

    WaitFor(TFakeFileProvider::GetDownloadStartedFuture().WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();

    auto weakResource = MakeWeak(resource);
    resource.Reset();
    WaitForPredicate(
        [&] {
            return !weakResource.Lock();
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    WaitForPredicate(
        [&] {
            return loadFuture.IsSet();
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(50),
            .Message = "Initial load future remained pending after resource destruction",
        });
    EXPECT_FALSE(WaitFor(loadFuture).IsOK());

    TFakeFileProvider::Unblock();
    WaitForPredicate(
        [] {
            return TFakeFileProvider::GetCompletedDownloadCount("v1") == 1;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    EXPECT_FALSE(weakResource.Lock());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
