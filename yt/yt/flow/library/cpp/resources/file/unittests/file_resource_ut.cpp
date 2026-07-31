#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>
#include <yt/yt/flow/library/cpp/resources/file/file_resource.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/init_context.h>
#include <yt/yt/flow/library/cpp/common/resource_manager.h>
#include <yt/yt/flow/library/cpp/common/unittests/mock/state.h>
#include <yt/yt/flow/library/cpp/common/unittests/mock/time_provider.h>
#include <yt/yt/flow/library/cpp/misc/versioned_value.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/system/type_name.h>

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

class TFakeFileSource
    : public TFileSourceBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TFakeFileSourceParameters, TFileSourceBase);

    using TFileSourceBase::TFileSourceBase;

    TFuture<TFileSourceRevisionPtr> Discover() override
    {
        TErrorOr<TFileSourceRevisionPtr> result;
        {
            auto guard = Guard(Lock_);
            ++DiscoverCount_;
            if (DiscoverResults_.empty()) {
                return MakeFuture<TFileSourceRevisionPtr>(nullptr);
            }
            result = DiscoverResults_.front();
            if (DiscoverResults_.size() > 1) {
                DiscoverResults_.pop_front();
            }
        }
        return MakeFuture(std::move(result));
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
        auto payload = GetParameters()->Prefix + ":" + revision->ObjectId.Underlying();
        return gate.Apply(BIND([
            stagingDirectory,
            relativePath = std::move(relativePath),
            payload = std::move(payload)
        ] {
            auto path = TFsPath(stagingDirectory) / TFsPath(relativePath);
            TOFStream output(path.GetPath());
            output << payload;
            output.Finish();

            return;
        }));
    }

    static void Reset()
    {
        auto guard = Guard(Lock_);
        DownloadCounts_.clear();
        BlockedContentId_.clear();
        DownloadGate_ = NewPromise<void>();
        DownloadStarted_ = NewPromise<void>();
        DiscoverCount_ = 0;
        DiscoverResults_.clear();
        DiscoverResults_.push_back(TFileSourceRevisionPtr{});
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

    static void SetDiscoveryError()
    {
        auto guard = Guard(Lock_);
        DiscoverResults_.clear();
        DiscoverResults_.push_back(TError("Fake discovery failure"));
    }

    static void PushDiscoveryRevision(const std::string& contentId)
    {
        auto revision = New<TFileSourceRevision>();
        revision->FileSourceClassName = TypeName<TFakeFileSource>();
        revision->ObjectId = NFileStorage::TFileStorageObjectId(contentId);
        revision->DisplayVersion = contentId;

        auto guard = Guard(Lock_);
        DiscoverResults_.push_back(std::move(revision));
    }

    static void PushNullDiscovery()
    {
        auto guard = Guard(Lock_);
        DiscoverResults_.push_back(TFileSourceRevisionPtr{});
    }

    static int GetDiscoverCount()
    {
        auto guard = Guard(Lock_);
        return DiscoverCount_;
    }

private:
    static NThreading::TSpinLock Lock_;
    static THashMap<std::string, int> DownloadCounts_;
    static std::string BlockedContentId_;
    static TPromise<void> DownloadGate_;
    static TPromise<void> DownloadStarted_;
    static int DiscoverCount_;
    static std::deque<TErrorOr<TFileSourceRevisionPtr>> DiscoverResults_;
};

NThreading::TSpinLock TFakeFileSource::Lock_;
THashMap<std::string, int> TFakeFileSource::DownloadCounts_;
std::string TFakeFileSource::BlockedContentId_;
TPromise<void> TFakeFileSource::DownloadGate_ = NewPromise<void>();
TPromise<void> TFakeFileSource::DownloadStarted_ = NewPromise<void>();
int TFakeFileSource::DiscoverCount_ = 0;
std::deque<TErrorOr<TFileSourceRevisionPtr>> TFakeFileSource::DiscoverResults_;

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
        if (auto it = Objects_.find(id.Underlying()); it != Objects_.end()) {
            return MakeFuture<NFileStorage::IFileStorageObjectPtr>(it->second);
        }

        auto directory = std::make_unique<TTempDir>();
        auto object = New<TFakeStorageObject>(id, directory->Name());
        WaitFor(filler(directory->Name())).ThrowOnError();
        Directories_.push_back(std::move(directory));
        Objects_[id.Underlying()] = object;
        return MakeFuture<NFileStorage::IFileStorageObjectPtr>(std::move(object));
    }

private:
    std::vector<std::unique_ptr<TTempDir>> Directories_;
    THashMap<std::string, NFileStorage::IFileStorageObjectPtr> Objects_;
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

    static void Reset()
    {
        auto guard = Guard(Lock_);
        InitializeCounts_.clear();
    }

    static int GetInitializeCount(const std::string& value)
    {
        auto guard = Guard(Lock_);
        return GetOrDefault(InitializeCounts_, value);
    }

protected:
    TTestStatePtr Initialize(const TMaterializedDirectoryPtr& directory) override
    {
        auto input = TFileInput(TString(TFsPath(directory->GetRootPath()).Child("artifact").GetPath()));
        auto valueString = input.ReadAll();
        std::string value(valueString.data(), valueString.size());

        {
            auto guard = Guard(Lock_);
            ++InitializeCounts_[value];
        }

        if (value.ends_with("initialize-failure")) {
            THROW_ERROR_EXCEPTION("Fake initialization failure");
        }

        return New<TTestState>(std::move(value), TFsPath(directory->GetRootPath()).Child("artifact").GetPath());
    }

    void Validate(const TTestStatePtr& state) override
    {
        if (state->Value.ends_with("validation-failure")) {
            THROW_ERROR_EXCEPTION("Fake validation failure");
        }
    }

private:
    static NThreading::TSpinLock Lock_;
    static THashMap<std::string, int> InitializeCounts_;
};

NThreading::TSpinLock TTestFileResource::Lock_;
THashMap<std::string, int> TTestFileResource::InitializeCounts_;

using TTestFileResourcePtr = TIntrusivePtr<TTestFileResource>;

YT_FLOW_DEFINE_RESOURCE(TTestFileResource);

////////////////////////////////////////////////////////////////////////////////

TResourceSpecPtr MakeResourceSpec(
    std::string fileSourceClassName = TypeName<TFakeFileSource>(),
    std::string prefix = "payload")
{
    auto spec = New<TResourceSpec>();
    spec->ResourceClassName = TypeName<TTestFileResource>();
    spec->Parameters = ConvertTo<IMapNodePtr>(TYsonString(Format("{file_source={file_source_class_name=%Qv;parameters={prefix=%Qv;};};}",
        fileSourceClassName,
        prefix)));
    return spec;
}

TResourceRevisionPtr MakeTarget(i64 deliveryRevisionId, const std::string& contentId)
{
    auto sourceRevision = New<TFileSourceRevision>();
    sourceRevision->FileSourceClassName = TypeName<TFakeFileSource>();
    sourceRevision->ObjectId = NFileStorage::TFileStorageObjectId(contentId);
    sourceRevision->DisplayVersion = contentId;

    auto target = New<TResourceRevision>();
    target->RevisionId = deliveryRevisionId;
    target->Spec = ConvertToNode(sourceRevision);
    return target;
}

TResourceRevisionPtr MakeMalformedTarget(i64 deliveryRevisionId)
{
    auto target = New<TResourceRevision>();
    target->RevisionId = deliveryRevisionId;
    target->Spec = ConvertToNode(std::string("malformed"));
    return target;
}

TResourceRevisionPtr MakeClassMismatchTarget(i64 deliveryRevisionId)
{
    auto sourceRevision = New<TFileSourceRevision>();
    sourceRevision->FileSourceClassName = "mismatched-source";
    sourceRevision->ObjectId = NFileStorage::TFileStorageObjectId("mismatched");
    sourceRevision->DisplayVersion = "mismatched";

    auto target = New<TResourceRevision>();
    target->RevisionId = deliveryRevisionId;
    target->Spec = ConvertToNode(sourceRevision);
    return target;
}

TDynamicResourceContextPtr MakeDynamicContext(
    TResourceRevisionPtr target = nullptr,
    i64 updateRetryPeriod = 100)
{
    auto context = New<TDynamicResourceContext>();
    context->DynamicResourceSpec = New<TDynamicResourceSpec>();
    context->DynamicResourceSpec->Parameters =
        ConvertTo<IMapNodePtr>(TYsonString(Format("{discover_period=10;update_retry_period=%v;}",
        updateRetryPeriod)));
    context->TargetRevision = std::move(target);
    return context;
}

TIntrusivePtr<TFileResourceController> MakeController(
    const IInvokerPtr& invoker,
    IStatusProfilerPtr statusProfiler,
    NProfiling::TProfiler profiler = {},
    i64 discoverPeriod = 10,
    std::string prefix = "payload")
{
    auto context = New<TResourceControllerContext>();
    context->ResourceId = TResourceId("test");
    context->ResourceSpec = MakeResourceSpec(
        TypeName<TFakeFileSource>(),
        std::move(prefix));
    context->Invoker = invoker;
    context->Logger = NLogging::TLogger("FileResourceControllerTest");
    context->StatusProfiler = std::move(statusProfiler);
    context->Profiler = std::move(profiler);

    auto dynamicContext = New<TDynamicResourceControllerContext>();
    dynamicContext->DynamicResourceSpec = New<TDynamicResourceSpec>();
    dynamicContext->DynamicResourceSpec->Parameters =
        ConvertTo<IMapNodePtr>(TYsonString(Format("{discover_period=%v;}", discoverPeriod)));
    return New<TFileResourceController>(std::move(context), std::move(dynamicContext));
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

TEST_F(TFileResourceTest, ControllerDeduplicatesAndPreservesRevisionAcrossNullDiscovery)
{
    TFakeFileSource::SetDiscoveryError();

    auto queue = New<TActionQueue>();
    auto statusProfiler = CreateSyncStatusProfiler();
    auto controller = MakeController(queue->GetInvoker(), statusProfiler);
    controller->Init(nullptr);

    WaitForPredicate(
        [&] {
            return statusProfiler->GetStatus().Errors.contains("/discovery");
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });

    TFakeFileSource::PushDiscoveryRevision("v1");
    WaitForPredicate(
        [&] {
            return controller->BuildTargetRevisionSpec() &&
                !statusProfiler->GetStatus().Errors.contains("/discovery");
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(10),
        });

    auto published = New<TVersionedValue<INodePtr>>();
    published->TrySetValue(controller->BuildTargetRevisionSpec(), TestVersionProvider());
    auto version = published->GetVersion();

    auto discoverCount = TFakeFileSource::GetDiscoverCount();
    TFakeFileSource::PushDiscoveryRevision("v1");
    WaitForPredicate(
        [&] {
            return TFakeFileSource::GetDiscoverCount() >= discoverCount + 2;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(10),
        });
    published->TrySetValue(controller->BuildTargetRevisionSpec(), TestVersionProvider());
    EXPECT_EQ(published->GetVersion(), version);

    discoverCount = TFakeFileSource::GetDiscoverCount();
    TFakeFileSource::PushNullDiscovery();
    WaitForPredicate(
        [&] {
            return TFakeFileSource::GetDiscoverCount() >= discoverCount + 2;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(10),
        });
    auto preservedRevision = ConvertTo<TFileSourceRevisionPtr>(controller->BuildTargetRevisionSpec());
    EXPECT_EQ(preservedRevision->ObjectId.Underlying(), "v1");
}

TEST_F(TFileResourceTest, ControllerReportsNullUntilFirstRevision)
{
    auto queue = New<TActionQueue>();
    auto statusProfiler = CreateSyncStatusProfiler();
    auto controller = MakeController(queue->GetInvoker(), statusProfiler);
    controller->Init(nullptr);

    WaitForPredicate(
        [&] {
            return statusProfiler->GetStatus().Errors.contains("/discovery");
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    EXPECT_FALSE(controller->BuildTargetRevisionSpec());

    TFakeFileSource::PushDiscoveryRevision("v1");
    WaitForPredicate(
        [&] {
            return controller->BuildTargetRevisionSpec() &&
                !statusProfiler->GetStatus().Errors.contains("/discovery");
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(10),
        });

    auto discoverCount = TFakeFileSource::GetDiscoverCount();
    TFakeFileSource::PushNullDiscovery();
    WaitForPredicate(
        [&] {
            return TFakeFileSource::GetDiscoverCount() >= discoverCount + 2;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(10),
        });

    EXPECT_FALSE(statusProfiler->GetStatus().Errors.contains("/discovery"));
    auto preservedRevision = ConvertTo<TFileSourceRevisionPtr>(controller->BuildTargetRevisionSpec());
    EXPECT_EQ(preservedRevision->ObjectId.Underlying(), "v1");
}

TEST_F(TFileResourceTest, ControllerRestoresLastRevision)
{
    auto queue = New<TActionQueue>();
    auto stateManager = New<TStateManagerMock>();
    auto controller = MakeController(
        queue->GetInvoker(),
        CreateSyncStatusProfiler(),
        {},
        1);
    controller->Init(stateManager->CreateContext());

    TFakeFileSource::PushDiscoveryRevision("v1");
    WaitForPredicate(
        [&] {
            return static_cast<bool>(controller->BuildTargetRevisionSpec());
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(10),
        });
    stateManager->Sync();
    controller.Reset();

    TFakeFileSource::SetDiscoveryError();
    auto restoredController = MakeController(
        queue->GetInvoker(),
        CreateSyncStatusProfiler(),
        {},
        TDuration::Hours(1).MilliSeconds());
    restoredController->Init(stateManager->CreateContext());

    auto restoredRevision = ConvertTo<TFileSourceRevisionPtr>(
        restoredController->BuildTargetRevisionSpec());
    ASSERT_TRUE(restoredRevision);
    EXPECT_EQ(restoredRevision->ObjectId.Underlying(), "v1");

    restoredController.Reset();
    auto changedSourceController = MakeController(
        queue->GetInvoker(),
        CreateSyncStatusProfiler(),
        {},
        TDuration::Hours(1).MilliSeconds(),
        "changed-source");
    changedSourceController->Init(stateManager->CreateContext());
    EXPECT_FALSE(changedSourceController->BuildTargetRevisionSpec());
}

TEST_F(TFileResourceTest, ControllerDropsRevisionGaugeWhenWorkerDisappears)
{
    auto queue = New<TActionQueue>();
    auto statusProfiler = CreateSyncStatusProfiler();
    NProfiling::TProfiler profiler("/file_resource_controller_test");
    auto controller = MakeController(queue->GetInvoker(), statusProfiler, profiler);

    auto status = New<TWorkerResourceStatus>();
    status->AppliedRevisionId = 7;
    controller->CollectStatuses({{"worker", status}}, nullptr);

    auto counts = controller->GetView()
        ->GetChildOrThrow("revision_instance_counts")
        ->AsMap();
    EXPECT_EQ(counts->GetChildValueOrThrow<i64>("7/applied"), 1);

    controller->CollectStatuses({}, nullptr);
    EXPECT_TRUE(controller->GetView()
            ->GetChildOrThrow("revision_instance_counts")
            ->AsMap()
            ->GetChildren()
            .empty());
}

TEST_F(TFileResourceTest, ControllerReconfiguresDiscoverPeriod)
{
    auto queue = New<TActionQueue>();
    auto controller = MakeController(
        queue->GetInvoker(),
        CreateSyncStatusProfiler(),
        {},
        TDuration::Hours(1).MilliSeconds());
    controller->Init(nullptr);
    WaitForPredicate(
        [] {
            return TFakeFileSource::GetDiscoverCount() > 0;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
    auto discoverCount = TFakeFileSource::GetDiscoverCount();

    auto dynamicContext = New<TDynamicResourceControllerContext>();
    dynamicContext->DynamicResourceSpec = New<TDynamicResourceSpec>();
    dynamicContext->DynamicResourceSpec->Parameters =
        ConvertTo<IMapNodePtr>(TYsonString(TStringBuf("{discover_period=1;}")));
    controller->Reconfigure(dynamicContext);

    WaitForPredicate(
        [&] {
            return TFakeFileSource::GetDiscoverCount() >= discoverCount + 2;
        },
        TWaitForPredicateOptions{
            .IterationCount = 100,
            .Period = TDuration::MilliSeconds(5),
        });
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
    EXPECT_EQ(resource->GetRevisionState().UpdateState, EFileResourceUpdateState::WaitingForRetry);
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
    EXPECT_EQ(resource->GetRevisionState().UpdateState, EFileResourceUpdateState::WaitingForRetry);
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
                return statusProfiler->GetStatus().Errors.contains("/file_update") &&
                    resource->GetRevisionState().UpdateState == EFileResourceUpdateState::WaitingForRetry;
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
    EXPECT_EQ(accessor.GetSourceRevision()->ObjectId.Underlying(), "v1");
    EXPECT_EQ(accessor.GetDeliveryRevisionId(), 1);
    EXPECT_EQ(resource->GetRevisionState().AppliedRevisionId, 1);
    EXPECT_EQ(resource->GetRevisionState().TargetRevisionId, 1);
    EXPECT_EQ(TFakeFileSource::GetDownloadCount("v1"), 1);
    EXPECT_EQ(TTestFileResource::GetInitializeCount("payload:v1"), 1);
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

TEST_F(TFileResourceTest, PendingUpdateStateIsCollectedBeforeInitialLoadCompletes)
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
    EXPECT_EQ(statuses[TResourceId("test")]->UpdateState, EFileResourceUpdateState::Downloading);

    TFakeFileSource::Unblock();
    WaitFor(loadFuture.WithTimeout(TDuration::Seconds(5))).ThrowOnError();
}

TEST_F(TFileResourceTest, LockKeepsPreviousStateAndCachedDirectoryAlive)
{
    auto queue = New<TActionQueue>();
    auto resource = MakeResource(queue->GetInvoker(), MakeTarget(1, "v1"));
    WaitFor(resource->Load({})).ThrowOnError();

    std::string oldFilePath;
    {
        auto oldAccessor = resource->Lock();
        oldFilePath = oldAccessor->FilePath;

        resource->Reconfigure(MakeDynamicContext(MakeTarget(2, "v2")));
        WaitForAppliedRevision(resource, 2);

        auto newAccessor = resource->Lock();
        EXPECT_EQ(oldAccessor->Value, "payload:v1");
        EXPECT_EQ(newAccessor->Value, "payload:v2");
        EXPECT_TRUE(TFsPath(oldFilePath).Exists());
    }

    EXPECT_TRUE(TFsPath(oldFilePath).Exists());
}

TEST_F(TFileResourceTest, EqualContentSkipsReinitialization)
{
    auto queue = New<TActionQueue>();
    auto resource = MakeResource(queue->GetInvoker(), MakeTarget(1, "same"));
    WaitFor(resource->Load({})).ThrowOnError();

    resource->Reconfigure(MakeDynamicContext(MakeTarget(2, "same")));
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
    TFakeFileSource::Unblock();
    WaitFor(loadFuture.WithTimeout(TDuration::Seconds(5)))
        .ThrowOnError();

    auto accessor = resource->Lock();
    EXPECT_EQ(accessor->Value, "payload:v2");
    EXPECT_EQ(accessor.GetDeliveryRevisionId(), 2);
    EXPECT_EQ(resource->GetRevisionState().AppliedRevisionId, 2);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
