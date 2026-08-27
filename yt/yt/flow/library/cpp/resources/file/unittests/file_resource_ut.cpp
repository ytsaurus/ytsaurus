#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>
#include <yt/yt/flow/library/cpp/resources/file/file_resource.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/init_context.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/resource_manager.h>
#include <yt/yt/flow/library/cpp/common/unittests/mock/state.h>
#include <yt/yt/flow/library/cpp/common/unittests/mock/time_provider.h>
#include <yt/yt/flow/library/cpp/file_sources/file_source_base.h>
#include <yt/yt/flow/library/cpp/file_storage/file_storage.h>
#include <yt/yt/flow/library/cpp/misc/versioned_value.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/system/type_name.h>

#include <algorithm>
#include <deque>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

struct TFakeFileSourceParameters
    : public virtual TYsonStruct
{
    std::string Prefix;

    REGISTER_YSON_STRUCT(TFakeFileSourceParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("prefix", &TThis::Prefix)
            .NonEmpty();
    }
};

struct TFakeFileSourceDynamicParameters
    : public virtual TYsonStruct
{
    std::optional<std::string> PinnedContentId;

    REGISTER_YSON_STRUCT(TFakeFileSourceDynamicParameters);

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

class TFakeFileSource
    : public TFileSourceBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TFakeFileSourceParameters, TFileSourceBase);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TFakeFileSourceDynamicParameters, TFileSourceBase);

    using TFileSourceBase::TFileSourceBase;

    TFuture<TFileSourceRevisionPtr> Discover() override
    {
        auto pinnedContentId = GetDynamicParameters()->PinnedContentId;
        TErrorOr<TFileSourceRevisionPtr> result;
        TFuture<void> gate = OKFuture;
        {
            auto guard = Guard(Lock_);
            const auto& prefix = GetParameters()->Prefix;
            ++DiscoverCounts_[prefix];
            if (pinnedContentId) {
                auto revision = New<TFileSourceRevision>();
                revision->FileSourceClassName = TypeName<TFakeFileSource>();
                revision->ObjectId = NFileStorage::TFileStorageObjectId(*pinnedContentId);
                revision->DisplayVersion = *pinnedContentId;
                result = std::move(revision);
            } else {
                auto& results = DiscoverResults_[prefix];
                if (results.empty()) {
                    result = TFileSourceRevisionPtr{};
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
        const TFileSourceRevisionPtr& revision,
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
        DiscoverResults_["payload"].push_back(TFileSourceRevisionPtr{});
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
        auto revision = New<TFileSourceRevision>();
        revision->FileSourceClassName = TypeName<TFakeFileSource>();
        revision->ObjectId = NFileStorage::TFileStorageObjectId(contentId);
        revision->DisplayVersion = contentId;

        auto guard = Guard(Lock_);
        DiscoverResults_[prefix].push_back(std::move(revision));
    }

    static void PushNullDiscovery(const std::string& prefix = "payload")
    {
        auto guard = Guard(Lock_);
        DiscoverResults_[prefix].push_back(TFileSourceRevisionPtr{});
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
    static THashMap<std::string, std::deque<TErrorOr<TFileSourceRevisionPtr>>> DiscoverResults_;
    static THashMap<std::string, TFuture<void>> DiscoveryGates_;
};

NThreading::TSpinLock TFakeFileSource::Lock_;
THashMap<std::string, int> TFakeFileSource::DownloadCounts_;
THashMap<std::string, int> TFakeFileSource::CompletedDownloadCounts_;
std::string TFakeFileSource::BlockedContentId_;
TPromise<void> TFakeFileSource::DownloadGate_ = NewPromise<void>();
TPromise<void> TFakeFileSource::DownloadStarted_ = NewPromise<void>();
THashMap<std::string, int> TFakeFileSource::DiscoverCounts_;
THashMap<std::string, std::deque<TErrorOr<TFileSourceRevisionPtr>>> TFakeFileSource::DiscoverResults_;
THashMap<std::string, TFuture<void>> TFakeFileSource::DiscoveryGates_;

YT_FLOW_DEFINE_FILE_SOURCE(TFakeFileSource);

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

class TFakeFileStorage
    : public NFileStorage::IFileStorage
{
public:
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
        auto object = New<TFakeStorageObject>(id, directory->Name());
        auto path = directory->Name();
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

    TFuture<TMaterializedFileSourcePtr> MaterializeOne(
        const TFileSnapshotPtr& fileSnapshot,
        const TFileSourceId& id) const
    {
        return MaterializeFileSource(fileSnapshot, id);
    }

    TFuture<TMaterializedFileSourceSnapshotPtr> MaterializeMany(
        const TFileSnapshotPtr& fileSnapshot,
        const std::vector<TFileSourceId>& ids) const
    {
        return MaterializeFileSources(fileSnapshot, ids);
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
    TTestStatePtr Initialize(const TMaterializedFileSourceSnapshotPtr& fileSources) override
    {
        if (fileSources->GetFileSources().size() == 1) {
            const auto& fileSource = fileSources->GetOnlyFileSource();
            auto path = TFsPath(fileSource->GetRootPath()).Child("artifact").GetPath();
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

        std::vector<TFileSourceId> ids;
        ids.reserve(fileSources->GetFileSources().size());
        for (const auto& [id, _] : fileSources->GetFileSources()) {
            ids.push_back(id);
        }
        std::sort(ids.begin(), ids.end());

        std::string value;
        std::string firstPath;
        for (const auto& id : ids) {
            auto path = TFsPath(fileSources->GetFileSource(id)->GetRootPath()).Child("artifact").GetPath();
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

TFileSourceSpecPtr MakeFileSourceSpec(
    std::string prefix,
    std::string fileSourceClassName = TypeName<TFakeFileSource>())
{
    auto spec = New<TFileSourceSpec>();
    spec->FileSourceClassName = std::move(fileSourceClassName);
    spec->Parameters = ConvertTo<IMapNodePtr>(TYsonString(Format("{prefix=%Qv;}", prefix)));
    return spec;
}

TResourceSpecPtr MakeNamedResourceSpec(const THashMap<std::string, std::string>& fileSources)
{
    auto spec = New<TResourceSpec>();
    spec->ResourceClassName = TypeName<TTestFileResource>();
    spec->Parameters = GetEphemeralNodeFactory()->CreateMap();
    for (const auto& [name, prefix] : fileSources) {
        spec->FileSources[TFileSourceId(name)] = MakeFileSourceSpec(prefix);
    }
    return spec;
}

TResourceSpecPtr MakeResourceSpec(
    std::string fileSourceClassName = TypeName<TFakeFileSource>(),
    std::string prefix = "payload")
{
    auto spec = MakeNamedResourceSpec({{"file", std::move(prefix)}});
    spec->FileSources.at(TFileSourceId("file"))->FileSourceClassName = std::move(fileSourceClassName);
    return spec;
}

TFileSourceRevisionPtr MakeSourceRevision(
    const std::string& contentId,
    std::string fileSourceClassName = TypeName<TFakeFileSource>())
{
    auto revision = New<TFileSourceRevision>();
    revision->FileSourceClassName = std::move(fileSourceClassName);
    revision->ObjectId = NFileStorage::TFileStorageObjectId(contentId);
    revision->DisplayVersion = contentId;
    return revision;
}

TResourceRevisionPtr MakeNamedTarget(
    i64 deliveryRevisionId,
    const THashMap<std::string, std::string>& fileSources)
{
    auto target = New<TResourceRevision>();
    target->RevisionId = deliveryRevisionId;
    target->ActiveFileSnapshot = New<TFileSnapshot>();
    target->ActiveFileSnapshot->Id = TFileSnapshotId(deliveryRevisionId);
    for (const auto& [name, contentId] : fileSources) {
        target->ActiveFileSnapshot->FileSources[TFileSourceId(name)] = MakeSourceRevision(contentId);
    }
    return target;
}

TFileSnapshotPtr MakeNamedFileSnapshot(
    i64 snapshotId,
    const THashMap<std::string, std::string>& fileSources)
{
    auto fileSnapshot = New<TFileSnapshot>();
    fileSnapshot->Id = TFileSnapshotId(snapshotId);
    for (const auto& [name, contentId] : fileSources) {
        fileSnapshot->FileSources[TFileSourceId(name)] = MakeSourceRevision(contentId);
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
    target->ActiveFileSnapshot->FileSources.at(TFileSourceId("file"))->FileSourceClassName = "mismatched-source";
    return target;
}

const TFileSnapshotPtr& GetLatestFileSnapshot(const TResourceRevisionPtr& target)
{
    return target->PreparingFileSnapshot
        ? target->PreparingFileSnapshot
        : target->ActiveFileSnapshot;
}

TDynamicFileSourceSpecPtr MakeDynamicFileSourceSpec(
    std::optional<std::string> pinnedContentId = std::nullopt)
{
    auto parameters = New<TFakeFileSourceDynamicParameters>();
    parameters->PinnedContentId = std::move(pinnedContentId);

    auto spec = New<TDynamicFileSourceSpec>();
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
    context->DynamicResourceSpec->FileSourceDiscoverPeriod = discoverPeriod;
    context->DynamicResourceSpec->FileSourceUpdateRetryPeriod = updateRetryPeriod;
    context->DynamicResourceSpec->FileSnapshotMinCreationPeriod = fileSnapshotMinCreationPeriod;
    context->DynamicResourceSpec->FileSnapshotCatalogMaxEntries = fileSnapshotCatalogMaxEntries;
    context->DynamicResourceSpec->FileSnapshotRolloutWarningPeriod = fileSnapshotRolloutWarningPeriod;
    for (const auto& [name, contentId] : pinnedContentIds) {
        context->DynamicResourceSpec->FileSources[TFileSourceId(name)] =
            MakeDynamicFileSourceSpec(contentId);
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
    const THashMap<std::string, std::string>& fileSources,
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
    context->ResourceSpec = MakeNamedResourceSpec(fileSources);
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
    const THashMap<std::string, std::string>& fileSources,
    TResourceRevisionPtr target = nullptr,
    NFileStorage::IFileStoragePtr fileStorage = New<TFakeFileStorage>(),
    IStatusProfilerPtr statusProfiler = CreateSyncStatusProfiler(),
    TDuration updateRetryPeriod = TDuration::MilliSeconds(100))
{
    auto context = New<TResourceContext>();
    context->ResourceId = TResourceId("test");
    context->ResourceSpec = MakeNamedResourceSpec(fileSources);
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
        TFakeFileSource::Reset();
        TTestFileResource::Reset();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TFileResourceTest, RegistryValidatesSource)
{
    EXPECT_NO_THROW(TRegistry::Get()->ValidateResourceSpec(MakeResourceSpec()));

    EXPECT_THROW_WITH_SUBSTRING(
        TRegistry::Get()->ValidateResourceSpec(MakeResourceSpec("missing-source")),
        "file source");
    EXPECT_THROW_WITH_SUBSTRING(
        TRegistry::Get()->ValidateResourceSpec(MakeResourceSpec(TypeName<TFakeFileSource>(), "")),
        "prefix");
}

TEST_F(TFileResourceTest, RegistryRejectsNamedSourcesForDirectResourceController)
{
    auto spec = MakeNamedResourceSpec({{"file", "payload"}});
    spec->ResourceClassName = TypeName<TTestFileResourceWithDirectController>();

    EXPECT_THROW_WITH_SUBSTRING(
        TRegistry::Get()->ValidateResourceSpec(spec),
        "does not support file source discovery");
}

TEST_F(TFileResourceTest, RegistryValidatesNamedSources)
{
    auto parsed = ConvertTo<TResourceSpecPtr>(TYsonString(Format("{resource_class_name=%Qv;parameters={};file_sources={"
        "left={file_source_class_name=%Qv;parameters={prefix=left;};};"
        "right={file_source_class_name=%Qv;parameters={prefix=right;};};};}",
        TypeName<TTestFileResource>(),
        TypeName<TFakeFileSource>(),
        TypeName<TFakeFileSource>())));
    EXPECT_EQ(parsed->FileSources.size(), 2);
    EXPECT_NO_THROW(TRegistry::Get()->ValidateResourceSpec(parsed));

    EXPECT_NO_THROW(TRegistry::Get()->ValidateResourceSpec(
        MakeNamedResourceSpec({{"left", "left"}, {"right", "right"}})));

    auto missingClass = MakeNamedResourceSpec({{"left", "left"}});
    missingClass->FileSources[TFileSourceId("left")]->FileSourceClassName = "missing-source";
    EXPECT_THROW_WITH_SUBSTRING(
        TRegistry::Get()->ValidateResourceSpec(missingClass),
        "file source");

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
        "at least one file source");
}

TEST_F(TFileResourceTest, RegistryValidatesDynamicNamedSources)
{
    auto pipelineSpec = New<TPipelineSpec>();
    pipelineSpec->Resources[TResourceId("resource")] =
        MakeNamedResourceSpec({{"left", "left"}});

    auto validate = [&] (TStringBuf dynamicSpec) {
        return TRegistry::Get()->ValidateDynamicPipelineSpecParseability(
            pipelineSpec,
            ConvertTo<IMapNodePtr>(TYsonString(dynamicSpec)));
    };

    EXPECT_TRUE(validate(R"({resources={resource={file_sources={left={parameters={pinned_content_id=left-v2;};};};};};})").empty());

    auto unknownSourceErrors = validate(
        R"({resources={resource={file_sources={right={parameters={};};};};};})");
    ASSERT_FALSE(unknownSourceErrors.empty());
    EXPECT_THAT(ToString(unknownSourceErrors[0]), ::testing::HasSubstr("does not exist in static spec"));

    auto invalidParametersErrors = validate(
        R"({resources={resource={file_sources={left={parameters={pinned_content_id="";};};};};};})");
    ASSERT_FALSE(invalidParametersErrors.empty());
    EXPECT_THAT(ToString(invalidParametersErrors[0]), ::testing::HasSubstr("must be nonempty"));

    auto unrecognizedParametersErrors = validate(
        R"({resources={resource={file_sources={left={parameters={unknown=1;};};};};};})");
    ASSERT_FALSE(unrecognizedParametersErrors.empty());
    EXPECT_THAT(ToString(unrecognizedParametersErrors[0]), ::testing::HasSubstr("unknown"));

    auto invalidDynamicPipelineSpec = New<TDynamicPipelineSpec>();
    auto invalidDynamicResourceSpec = New<TDynamicResourceSpec>();
    invalidDynamicResourceSpec->FileSources[TFileSourceId("../left")] = New<TDynamicFileSourceSpec>();
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
    revision->ActiveFileSnapshot->FileSources[TFileSourceId("left")] = MakeSourceRevision("left-v1");
    revision->PreparingFileSnapshot = New<TFileSnapshot>();
    revision->PreparingFileSnapshot->Id = TFileSnapshotId(4);
    revision->PreparingFileSnapshot->FileSources[TFileSourceId("left")] = MakeSourceRevision("left-v2");

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
    TFakeFileSource::PushDiscoveryRevision("left-v1", "left");

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
            return TFakeFileSource::GetDiscoverCount("left") > 0 &&
                TFakeFileSource::GetDiscoverCount("right") > 0;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    EXPECT_FALSE(controller->BuildTargetRevision());
    EXPECT_TRUE(statusProfiler->GetStatus().Errors.contains("/file_sources/right/discovery"));

    TFakeFileSource::PushDiscoveryRevision("right-v1", "right");
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
    EXPECT_EQ(target->PreparingFileSnapshot->FileSources.at(TFileSourceId("left"))->ObjectId.Underlying(), "left-v1");
    EXPECT_EQ(target->PreparingFileSnapshot->FileSources.at(TFileSourceId("right"))->ObjectId.Underlying(), "right-v1");
    EXPECT_FALSE(statusProfiler->GetStatus().Errors.contains("/file_sources/right/discovery"));

    TFakeFileSource::PushDiscoveryRevision("left-v2", "left");
    WaitForPredicate(
        [&] {
            auto updated = controller->BuildTargetRevision();
            return updated &&
                GetLatestFileSnapshot(updated)->FileSources.at(TFileSourceId("left"))->ObjectId.Underlying() == "left-v2";
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    EXPECT_EQ(
        GetLatestFileSnapshot(controller->BuildTargetRevision())->FileSources.at(TFileSourceId("right"))->ObjectId.Underlying(),
        "right-v1");
}

TEST_F(TFileResourceTest, NamedControllerKeepsOwnSpecWhenDiscoveryFails)
{
    TFakeFileSource::SetDiscoveryError("file");

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
        return TFakeFileSource::GetDiscoverCount("file") > 0;
    });

    auto target = controller->BuildTargetRevision();
    ASSERT_TRUE(target);
    EXPECT_EQ(ConvertTo<std::string>(target->Spec), "controller-spec");
    EXPECT_FALSE(target->ActiveFileSnapshot);
    EXPECT_FALSE(target->PreparingFileSnapshot);
}

TEST_F(TFileResourceTest, NamedControllerPromotesOnlyAuthoritativeCurrentValidatedSnapshot)
{
    TFakeFileSource::PushDiscoveryRevision("v1", "file");

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
    TFakeFileSource::SetDiscoveryError("file");
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
    TFakeFileSource::PushDiscoveryRevision("v1", "file");

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

    const auto discoverCount = TFakeFileSource::GetDiscoverCount("file");
    TFakeFileSource::PushDiscoveryRevision("v2", "file");
    WaitForPredicate(
        [&] {
            return TFakeFileSource::GetDiscoverCount("file") >= discoverCount + 2;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });

    target = controller->BuildTargetRevision();
    ASSERT_TRUE(target->PreparingFileSnapshot);
    EXPECT_EQ(target->PreparingFileSnapshot->Id, firstSnapshotId);
    EXPECT_EQ(
        target->PreparingFileSnapshot->FileSources.at(TFileSourceId("file"))->ObjectId.Underlying(),
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
        target->PreparingFileSnapshot->FileSources.at(TFileSourceId("file"))->ObjectId.Underlying(),
        "v2");
}

TEST_F(TFileResourceTest, NamedControllerBoundsSnapshotCatalogAndKeepsCurrentSlots)
{
    TFakeFileSource::PushDiscoveryRevision("v1", "file");

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

    TFakeFileSource::PushDiscoveryRevision("v2", "file");
    WaitForPredicate(
        [&] {
            target = controller->BuildTargetRevision();
            return target &&
                target->PreparingFileSnapshot &&
                target->PreparingFileSnapshot->FileSources.at(TFileSourceId("file"))->ObjectId.Underlying() == "v2";
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    const auto supersededPreparingId = target->PreparingFileSnapshot->Id;

    TFakeFileSource::PushDiscoveryRevision("v3", "file");
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
        target->PreparingFileSnapshot->FileSources.at(TFileSourceId("file"))->ObjectId.Underlying(),
        "v3");
    auto view = controller->GetView()->GetChildOrThrow("file_sources")->AsMap();
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
    TFakeFileSource::SetDiscoveryError("file");
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
            ->GetChildOrThrow("file_sources")
            ->AsMap()
            ->GetChildValueOrThrow<i64>("known_file_snapshot_count"),
        2);
}

TEST_F(TFileResourceTest, NamedControllerDropsPreparingWhenDiscoveryReturnsToActive)
{
    TFakeFileSource::PushDiscoveryRevision("v1", "file");

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

    TFakeFileSource::PushDiscoveryRevision("v2", "file");
    WaitForPredicate(
        [&] {
            target = controller->BuildTargetRevision();
            return target && target->PreparingFileSnapshot;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });

    TFakeFileSource::PushDiscoveryRevision("v1", "file");
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
        target->ActiveFileSnapshot->FileSources.at(TFileSourceId("file"))->ObjectId.Underlying(),
        "v1");
}

TEST_F(TFileResourceTest, NamedControllerRetainsLastCompleteSnapshotAcrossFailures)
{
    TFakeFileSource::PushDiscoveryRevision("left-v1", "left");
    TFakeFileSource::PushDiscoveryRevision("right-v1", "right");

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

    auto discoverCount = TFakeFileSource::GetDiscoverCount("right");
    TFakeFileSource::PushNullDiscovery("right");
    WaitForPredicate(
        [&] {
            return TFakeFileSource::GetDiscoverCount("right") >= discoverCount + 2;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    EXPECT_EQ(
        GetLatestFileSnapshot(controller->BuildTargetRevision())->FileSources.at(TFileSourceId("right"))->ObjectId.Underlying(),
        "right-v1");
    EXPECT_FALSE(statusProfiler->GetStatus().Errors.contains("/file_sources/right/discovery"));

    TFakeFileSource::SetDiscoveryError("right");
    WaitForPredicate(
        [&] {
            return statusProfiler->GetStatus().Errors.contains("/file_sources/right/discovery");
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    EXPECT_EQ(
        GetLatestFileSnapshot(controller->BuildTargetRevision())->FileSources.at(TFileSourceId("right"))->ObjectId.Underlying(),
        "right-v1");
}

TEST_F(TFileResourceTest, NamedControllerReportsAndClearsPersistedRolloutWarning)
{
    TFakeFileSource::PushDiscoveryRevision("v1", "file");

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

    auto view = restored->GetView()->GetChildOrThrow("file_sources")->AsMap();
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
    view = restored->GetView()->GetChildOrThrow("file_sources")->AsMap();
    EXPECT_EQ(view->GetChildValueOrThrow<i64>("rollout_blocking_accessor_count"), 2);

    status->ActiveFileSnapshotId = activeSnapshotId;
    status->PreparingFileSnapshot.Reset();
    restored->CollectStatuses({{"worker", MakeWorkerStatus(status)}}, nullptr, 17);
    EXPECT_FALSE(statusProfiler->GetStatus().Errors.contains("/file_snapshot_rollout"));
    view = restored->GetView()->GetChildOrThrow("file_sources")->AsMap();
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
    view = restored->GetView()->GetChildOrThrow("file_sources")->AsMap();
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
            ->GetChildOrThrow("file_sources")
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

    auto view = controller->GetView()->GetChildOrThrow("file_sources")->AsMap();
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
    view = controller->GetView()->GetChildOrThrow("file_sources")->AsMap();
    EXPECT_TRUE(view->GetChildOrThrow("file_snapshot_state_counts")->AsMap()->GetChildren().empty());
    EXPECT_TRUE(view->GetChildOrThrow("live_accessor_counts")->AsMap()->GetChildren().empty());
    EXPECT_EQ(view->GetChildValueOrThrow<i64>("unknown_file_snapshot_count"), 0);
    EXPECT_EQ(view->GetChildValueOrThrow<i64>("rollout_instance_count"), 0);

    status->ResourceInstanceId = TResourceInstanceId(TGuid::Create());
    status->ResourceIncarnationGeneration = 0;
    controller->CollectStatuses({{"worker", MakeWorkerStatus(status)}}, nullptr, 17);
    view = controller->GetView()->GetChildOrThrow("file_sources")->AsMap();
    EXPECT_EQ(view->GetChildValueOrThrow<i64>("rollout_instance_count"), 1);
}

TEST_F(TFileResourceTest, NamedControllerCountsHistoricalAppliedFileSourceRevisions)
{
    TFakeFileSource::PushDiscoveryRevision("left-v1", "left");
    TFakeFileSource::PushDiscoveryRevision("right-v1", "right");

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

    TFakeFileSource::PushDiscoveryRevision("left-v2", "left");
    WaitForPredicate(
        [&] {
            target = controller->BuildTargetRevision();
            return target &&
                target->PreparingFileSnapshot &&
                target->PreparingFileSnapshot->FileSources.at(TFileSourceId("left"))->ObjectId.Underlying() == "left-v2";
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
        ->GetChildOrThrow("file_sources")
        ->AsMap()
        ->GetChildOrThrow("file_source_revision_state_counts")
        ->AsMap();
    EXPECT_EQ(counts->GetChildValueOrThrow<i64>(
        Format("%v/%v/%v", TFileSourceId("left"), NFileStorage::TFileStorageObjectId("left-v1"), FormatEnum(EFileSnapshotState::Active))),
        1);
    EXPECT_EQ(counts->GetChildValueOrThrow<i64>(
        Format("%v/%v/%v", TFileSourceId("left"), NFileStorage::TFileStorageObjectId("left-v2"), FormatEnum(EFileSnapshotState::Preparing))),
        1);
    EXPECT_EQ(counts->GetChildValueOrThrow<i64>(
        Format("%v/%v/%v", TFileSourceId("right"), NFileStorage::TFileStorageObjectId("right-v1"), FormatEnum(EFileSnapshotState::Active))),
        1);
    EXPECT_FALSE(counts->FindChild(
        Format("%v/%v/%v", TFileSourceId("right"), NFileStorage::TFileStorageObjectId("right-v1"), FormatEnum(EFileSnapshotState::Preparing))));
}

TEST_F(TFileResourceTest, NamedControllerRestoresSnapshotsAcrossCompatibleSpecChanges)
{
    TFakeFileSource::PushDiscoveryRevision("left-v1", "left");
    TFakeFileSource::PushDiscoveryRevision("right-v1", "right");

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

    TFakeFileSource::SetDiscoveryError("left");
    TFakeFileSource::SetDiscoveryError("right");
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
        GetLatestFileSnapshot(restored->BuildTargetRevision())->FileSources.at(TFileSourceId("left"))->ObjectId.Underlying(),
        "left-v1");
    EXPECT_EQ(
        GetLatestFileSnapshot(restored->BuildTargetRevision())->FileSources.at(TFileSourceId("right"))->ObjectId.Underlying(),
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
        GetLatestFileSnapshot(changed->BuildTargetRevision())->FileSources.at(TFileSourceId("left"))->ObjectId.Underlying(),
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
            return TFakeFileSource::GetDiscoverCount("left") > 0;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    auto discoverCount = TFakeFileSource::GetDiscoverCount("left");

    auto dynamicContext = New<TDynamicResourceControllerContext>();
    dynamicContext->DynamicResourceSpec = MakeNamedDynamicContext(
        nullptr,
        TDuration::MilliSeconds(1))
        ->DynamicResourceSpec;
    controller->Reconfigure(dynamicContext);

    WaitForPredicate(
        [&] {
            return TFakeFileSource::GetDiscoverCount("left") >= discoverCount + 2;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
}

TEST_F(TFileResourceTest, NamedControllerAppliesDynamicPinImmediately)
{
    TFakeFileSource::PushDiscoveryRevision("latest-v1", "left");

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
                GetLatestFileSnapshot(target)->FileSources.at(TFileSourceId("left"))->ObjectId.Underlying() == "latest-v1";
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
                GetLatestFileSnapshot(target)->FileSources.at(TFileSourceId("left"))->ObjectId.Underlying() == "pinned-v1";
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
}

TEST_F(TFileResourceTest, NamedControllerPublishesMultipleDynamicPinsAtomically)
{
    TFakeFileSource::PushDiscoveryRevision("left-v0", "left");
    TFakeFileSource::PushDiscoveryRevision("right-v0", "right");
    auto rightGate = NewPromise<void>();
    TFakeFileSource::SetDiscoveryGate("right-v1", rightGate.ToFuture());

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
                GetLatestFileSnapshot(target)->FileSources.at(TFileSourceId("left"))->ObjectId.Underlying() == "left-v0" &&
                GetLatestFileSnapshot(target)->FileSources.at(TFileSourceId("right"))->ObjectId.Underlying() == "right-v0";
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });

    auto leftDiscoverCount = TFakeFileSource::GetDiscoverCount("left");
    auto rightDiscoverCount = TFakeFileSource::GetDiscoverCount("right");
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
            return TFakeFileSource::GetDiscoverCount("left") > leftDiscoverCount &&
                TFakeFileSource::GetDiscoverCount("right") > rightDiscoverCount;
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
        GetLatestFileSnapshot(pendingTarget)->FileSources.at(TFileSourceId("left"))->ObjectId.Underlying(),
        "left-v0");
    EXPECT_EQ(
        GetLatestFileSnapshot(pendingTarget)->FileSources.at(TFileSourceId("right"))->ObjectId.Underlying(),
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
        GetLatestFileSnapshot(restoredTarget)->FileSources.at(TFileSourceId("left"))->ObjectId.Underlying(),
        "left-v0");
    EXPECT_EQ(
        GetLatestFileSnapshot(restoredTarget)->FileSources.at(TFileSourceId("right"))->ObjectId.Underlying(),
        "right-v0");

    rightGate.Set();
    WaitForPredicate(
        [&] {
            auto target = restored->BuildTargetRevision();
            return target &&
                GetLatestFileSnapshot(target)->FileSources.at(TFileSourceId("left"))->ObjectId.Underlying() == "left-v1" &&
                GetLatestFileSnapshot(target)->FileSources.at(TFileSourceId("right"))->ObjectId.Underlying() == "right-v1";
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
    TFakeFileSource::SetDiscoveryGate("stale", staleGate.ToFuture());
    TFakeFileSource::SetDiscoveryGate("pinned", pinnedGate.ToFuture());
    TFakeFileSource::PushDiscoveryRevision("initial", "left");
    TFakeFileSource::PushDiscoveryRevision("stale", "left");

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
                GetLatestFileSnapshot(target)->FileSources.at(TFileSourceId("left"))->ObjectId.Underlying() == "initial";
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    WaitForPredicate(
        [] {
            return TFakeFileSource::GetDiscoverCount("left") >= 2;
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
            return TFakeFileSource::GetDiscoverCount("left") >= 3;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    ASSERT_EQ(
        GetLatestFileSnapshot(controller->BuildTargetRevision())
            ->FileSources.at(TFileSourceId("left"))
            ->ObjectId.Underlying(),
        "initial");

    pinnedGate.Set();
    WaitForPredicate(
        [&] {
            auto target = controller->BuildTargetRevision();
            return target &&
                GetLatestFileSnapshot(target)->FileSources.at(TFileSourceId("left"))->ObjectId.Underlying() == "pinned";
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

    TFakeFileSource::SetDiscoveryError("left");
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
        GetLatestFileSnapshot(target)->FileSources.at(TFileSourceId("left"))->ObjectId.Underlying(),
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
    EXPECT_EQ(accessor.GetSourceRevision(TFileSourceId("file"))->ObjectId.Underlying(), "v1");
    EXPECT_EQ(accessor.GetDeliveryRevisionId(), 1);
    EXPECT_EQ(resource->GetRevisionState().AppliedRevisionId, 1);
    EXPECT_EQ(resource->GetRevisionState().TargetRevisionId, 1);
    EXPECT_EQ(TFakeFileSource::GetDownloadCount("v1"), 1);
    EXPECT_EQ(TTestFileResource::GetInitializeCount("payload:v1"), 1);
}

TEST_F(TFileResourceTest, AccessorRejectsUnknownFileSource)
{
    auto queue = New<TActionQueue>();
    auto resource = MakeResource(queue->GetInvoker(), MakeTarget(1, "v1"));
    WaitFor(resource->Load({})).ThrowOnError();

    auto accessor = resource->Lock();
    EXPECT_THROW_WITH_SUBSTRING(
        accessor.GetRootPath(TFileSourceId("missing")),
        "Unknown materialized file source");
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
    TFakeFileSource::Block("active-v1");

    auto queue = New<TActionQueue>();
    auto resource = MakeResource(
        queue->GetInvoker(),
        MakeRolloutTarget(
            1,
            MakeFileSnapshot(10, "active-v1"),
            MakeFileSnapshot(11, "preparing-v1")));

    auto loadFuture = resource->Load({});
    WaitFor(TFakeFileSource::GetDownloadStartedFuture().WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();
    EXPECT_EQ(TFakeFileSource::GetDownloadCount("preparing-v1"), 0);
    auto downloadingState = resource->GetRevisionState();
    ASSERT_TRUE(downloadingState.PreparingFileSnapshot);
    EXPECT_EQ(downloadingState.PreparingFileSnapshot->State, EFileSnapshotState::Preparing);
    EXPECT_EQ(
        downloadingState.PreparingFileSnapshot->PreparationStage,
        EFileSnapshotPreparationStage::Materializing);

    TFakeFileSource::Unblock();
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
    EXPECT_EQ(TFakeFileSource::GetDownloadCount("active-v1"), 1);
    EXPECT_EQ(TFakeFileSource::GetDownloadCount("preparing-v1"), 1);
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
    EXPECT_EQ(TFakeFileSource::GetDownloadCount("download-failure"), 1);
    EXPECT_EQ(TFakeFileSource::GetDownloadCount("v2"), 1);

    resource->Reconfigure(MakeDynamicContext(
        MakeRolloutTarget(2, MakeFileSnapshot(11, "v2")),
        TDuration::Hours(1).MilliSeconds()));
    WaitFor(loadFuture.WithTimeout(TDuration::Seconds(5))).ThrowOnError();

    EXPECT_EQ(resource->Lock().GetFileSnapshotId(), TFileSnapshotId(11));
    EXPECT_EQ(resource->Lock()->Value, "payload:v2");
}

TEST_F(TFileResourceTest, RolloutReportsPreparationAndActivationStages)
{
    TFakeFileSource::Block("v2");
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

    WaitFor(TFakeFileSource::GetDownloadStartedFuture().WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();
    WaitForPreparingState(
        resource,
        TFileSnapshotId(11),
        EFileSnapshotState::Preparing,
        EFileSnapshotPreparationStage::Materializing);

    TFakeFileSource::Unblock();
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
    EXPECT_EQ(TFakeFileSource::GetDownloadCount("v1"), 1);
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

TEST_F(TFileResourceTest, NamedSourcesInitializeAsOneSnapshot)
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
    EXPECT_EQ(accessor.GetSourceRevision(TFileSourceId("left"))->ObjectId.Underlying(), "left-v1");
    EXPECT_EQ(accessor.GetSourceRevision(TFileSourceId("right"))->ObjectId.Underlying(), "right-v1");
    EXPECT_TRUE(TFsPath(accessor.GetRootPath(TFileSourceId("left"))).Child("artifact").Exists());
    EXPECT_TRUE(TFsPath(accessor.GetRootPath(TFileSourceId("right"))).Child("artifact").Exists());
    EXPECT_EQ(TFakeFileSource::GetDownloadCount("left-v1"), 1);
    EXPECT_EQ(TFakeFileSource::GetDownloadCount("right-v1"), 1);
    EXPECT_EQ(TFakeFileSource::GetDiscoverCount("left"), 0);
    EXPECT_EQ(TFakeFileSource::GetDiscoverCount("right"), 0);
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

    auto left = WaitFor(resource->MaterializeOne(fileSnapshot, TFileSourceId("left"))).ValueOrThrow();
    EXPECT_EQ(left->GetRevision()->ObjectId.Underlying(), "left-v1");

    auto subset = WaitFor(resource->MaterializeMany(
        fileSnapshot,
        {TFileSourceId("left"), TFileSourceId("right")}))
        .ValueOrThrow();
    EXPECT_EQ(subset->GetFileSources().size(), 2);
    EXPECT_TRUE(subset->GetFileSources().contains(TFileSourceId("left")));
    EXPECT_TRUE(subset->GetFileSources().contains(TFileSourceId("right")));
    EXPECT_EQ(TFakeFileSource::GetDownloadCount("left-v1"), 1);
    EXPECT_EQ(TFakeFileSource::GetDownloadCount("right-v1"), 1);
    EXPECT_EQ(TFakeFileSource::GetDownloadCount("unused-v1"), 0);

    EXPECT_THROW_WITH_SUBSTRING(
        static_cast<void>(resource->MaterializeOne(fileSnapshot, TFileSourceId("missing"))),
        "is not configured");
    EXPECT_THROW_WITH_SUBSTRING(
        static_cast<void>(resource->MaterializeMany(
            fileSnapshot,
            {TFileSourceId("left"), TFileSourceId("left")})),
        "requested more than once");

    auto nullSnapshot = MakeNamedFileSnapshot(2, {{"left", "left-v1"}});
    nullSnapshot->FileSources[TFileSourceId("left")] = nullptr;
    EXPECT_THROW_WITH_SUBSTRING(
        static_cast<void>(resource->MaterializeOne(nullSnapshot, TFileSourceId("left"))),
        "null file source");

    auto mismatchedSnapshot = MakeNamedFileSnapshot(3, {{"left", "left-v1"}});
    mismatchedSnapshot->FileSources[TFileSourceId("left")]->FileSourceClassName = "mismatched-source";
    EXPECT_THROW_WITH_SUBSTRING(
        static_cast<void>(resource->MaterializeOne(mismatchedSnapshot, TFileSourceId("left"))),
        "differs from configured class");
}

TEST_F(TFileResourceTest, NamedSourcesNeverPublishPartialInitialization)
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

TEST_F(TFileResourceTest, NamedSourcesReuseEqualTupleAndReinitializeChangedTuple)
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
    EXPECT_EQ(TFakeFileSource::GetDownloadCount("left-v1"), 1);
    EXPECT_EQ(TFakeFileSource::GetDownloadCount("right-v1"), 1);
    EXPECT_EQ(TFakeFileSource::GetDownloadCount("right-v2"), 1);
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
    EXPECT_EQ(TFakeFileSource::GetDownloadCount("same"), 1);
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
    EXPECT_EQ(TFakeFileSource::GetDownloadCount("v2"), 1);
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
    EXPECT_EQ(TFakeFileSource::GetDownloadCount("v2"), 1);
    EXPECT_EQ(TTestFileResource::GetInitializeCount("payload:v2"), 1);
}

TEST_F(TFileResourceTest, ReconfigureKeepsInFlightPreparingSnapshot)
{
    auto queue = New<TActionQueue>();
    auto resource = MakeResource(
        queue->GetInvoker(),
        MakeRolloutTarget(1, MakeFileSnapshot(1, "v1")));
    WaitFor(resource->Load({})).ThrowOnError();

    TFakeFileSource::Block("v2");
    resource->Reconfigure(MakeDynamicContext(MakeRolloutTarget(
        2,
        MakeFileSnapshot(1, "v1"),
        MakeFileSnapshot(2, "v2"))));
    WaitFor(TFakeFileSource::GetDownloadStartedFuture().WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();

    resource->Reconfigure(MakeDynamicContext(MakeRolloutTarget(
        3,
        MakeFileSnapshot(1, "v1"),
        MakeFileSnapshot(2, "v2"))));
    TFakeFileSource::Unblock();
    WaitForPredicate([&] {
        auto state = resource->GetRevisionState();
        return state.PreparingFileSnapshot &&
            state.PreparingFileSnapshot->State == EFileSnapshotState::Validated;
    });

    EXPECT_EQ(resource->GetRevisionState().AppliedRevisionId, 3);
    EXPECT_EQ(TFakeFileSource::GetDownloadCount("v2"), 1);
    EXPECT_EQ(TTestFileResource::GetInitializeCount("payload:v2"), 1);
}

TEST_F(TFileResourceTest, NamedSourceTargetMustMatchConfiguredSnapshotExactly)
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

TEST_F(TFileResourceTest, NamedSourceTargetRejectsClassMismatch)
{
    auto target = MakeNamedTarget(1, {{"file", "v1"}});
    target->ActiveFileSnapshot->FileSources[TFileSourceId("file")]->FileSourceClassName = "mismatched-source";

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

TEST_F(TFileResourceTest, NamedSourceDirectRetryPeriodReconfigurationTriggersRetry)
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
    EXPECT_EQ(TFakeFileSource::GetDownloadCount("download-failure-once"), 1);

    resource->Reconfigure(MakeNamedDynamicContext(
        target,
        TDuration::MilliSeconds(10),
        TDuration::MilliSeconds(1)));
    WaitFor(loadFuture.WithTimeout(TDuration::Seconds(5))).ThrowOnError();

    EXPECT_EQ(TFakeFileSource::GetDownloadCount("download-failure-once"), 2);
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
    EXPECT_EQ(TFakeFileSource::GetDownloadCount("download-failure-once"), 2);
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
    EXPECT_EQ(TFakeFileSource::GetDownloadCount("download-failure-once"), 1);

    resource->Reconfigure(MakeDynamicContext(target, 1));
    WaitFor(loadFuture.WithTimeout(TDuration::Seconds(5))).ThrowOnError();

    EXPECT_EQ(TFakeFileSource::GetDownloadCount("download-failure-once"), 2);
    EXPECT_FALSE(statusProfiler->GetStatus().Errors.contains("/file_update"));
}

TEST_F(TFileResourceTest, PendingSnapshotStateIsCollectedBeforeInitialLoadCompletes)
{
    TFakeFileSource::Block("v1");

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

    WaitFor(TFakeFileSource::GetDownloadStartedFuture().WithTimeout(TDuration::Seconds(5)))
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

    TFakeFileSource::Unblock();
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
    EXPECT_EQ(TFakeFileSource::GetDownloadCount("same"), 1);
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
        return TFakeFileSource::GetDownloadCount("download-failure") >= 1;
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
    TFakeFileSource::Block("v1");

    auto queue = New<TActionQueue>();
    auto resource = MakeResource(queue->GetInvoker(), MakeTarget(1, "v1"));
    auto loadFuture = resource->Load({});

    WaitFor(TFakeFileSource::GetDownloadStartedFuture().WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();
    EXPECT_FALSE(loadFuture.IsSet());

    resource->Reconfigure(MakeDynamicContext(MakeTarget(2, "v2")));
    WaitFor(loadFuture.WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();

    auto accessor = resource->Lock();
    EXPECT_EQ(accessor->Value, "payload:v2");
    EXPECT_EQ(accessor.GetDeliveryRevisionId(), 2);
    EXPECT_EQ(resource->GetRevisionState().AppliedRevisionId, 2);

    TFakeFileSource::Unblock();
    WaitForPredicate(
        [] {
            return TFakeFileSource::GetCompletedDownloadCount("v1") == 1;
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
    TFakeFileSource::Block("v1");

    auto queue = New<TActionQueue>();
    auto resource = MakeResource(queue->GetInvoker(), MakeTarget(1, "v1"));
    auto loadFuture = resource->Load({});

    WaitFor(TFakeFileSource::GetDownloadStartedFuture().WithTimeout(TDuration::Seconds(5)))
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

    TFakeFileSource::Unblock();
    WaitForPredicate(
        [] {
            return TFakeFileSource::GetCompletedDownloadCount("v1") == 1;
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
