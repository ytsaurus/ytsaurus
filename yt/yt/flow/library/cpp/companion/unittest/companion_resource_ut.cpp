#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/companion/companion_model.h>
#include <yt/yt/flow/library/cpp/companion/companion_resource.h>

#include <yt/yt/flow/library/cpp/common/resource_manager.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NFlow::NCompanion {
namespace {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

struct TResourceExecuteCall
{
    TResourceId ResourceId;
    ECompanionResourceCommand Command;
    TYsonString Argument;
};

//! Companion client whose ResourceExecute responses are scripted by the test.
class TFakeCompanionClient
    : public ICompanionClient
{
public:
    //! |statuses| are consumed by successive ResourceExecute calls; Ok when exhausted.
    explicit TFakeCompanionClient(std::vector<ECompanionResourceExecuteStatus> statuses = {})
        : ScriptedStatuses_(std::move(statuses))
    {
        CompanionInfo_ = New<TCompanionInfo>();
    }

    TCompanionResponsePtr DoProcessWithCompanionSync(
        const TCompanionProcessRequestPtr& /*companionRequest*/,
        const IExternalPerformanceMetricsReporterPtr& /*reporter*/) override
    {
        YT_UNIMPLEMENTED();
    }

    TCompanionInfoPtr GetCompanionInfo() override
    {
        auto guard = Guard(Lock_);
        return CompanionInfo_;
    }

    TFuture<void> RemoveJob(const TJobId& /*jobId*/) override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<TCompanionJobList> ListJobs() override
    {
        return MakeFuture(TCompanionJobList{});
    }

    TCompanionPutJobResponsePtr PutJob(
        const TCompanionPutJobRequestPtr& /*putJobRequest*/,
        const IExternalPerformanceMetricsReporterPtr& /*reporter*/) override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<TCompanionResourceExecuteResponsePtr> ResourceExecute(
        const TResourceId& resourceId,
        ECompanionResourceCommand command,
        const TYsonString& argument) override
    {
        auto guard = Guard(Lock_);
        auto callIndex = Calls_.size();
        Calls_.push_back({resourceId, command, argument});
        if (auto it = DeferredResponses_.find(callIndex); it != DeferredResponses_.end()) {
            return it->second;
        }
        auto response = New<TCompanionResourceExecuteResponse>();
        response->Status = callIndex < ScriptedStatuses_.size()
            ? ScriptedStatuses_[callIndex]
            : ECompanionResourceExecuteStatus::Ok;
        if (response->Status != ECompanionResourceExecuteStatus::Ok) {
            response->Error = TError("Scripted failure");
        }
        return MakeFuture(std::move(response));
    }

    TPromise<TCompanionResourceExecuteResponsePtr> DeferResponse(size_t callIndex)
    {
        auto promise = NewPromise<TCompanionResourceExecuteResponsePtr>();
        auto guard = Guard(Lock_);
        DeferredResponses_[callIndex] = promise.ToFuture();
        return promise;
    }

    std::vector<TResourceExecuteCall> GetCalls() const
    {
        auto guard = Guard(Lock_);
        return Calls_;
    }

    int GetCommandCount(ECompanionResourceCommand command) const
    {
        auto guard = Guard(Lock_);
        int result = 0;
        for (const auto& call : Calls_) {
            result += call.Command == command ? 1 : 0;
        }
        return result;
    }

private:
    const std::vector<ECompanionResourceExecuteStatus> ScriptedStatuses_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    TCompanionInfoPtr CompanionInfo_;
    std::vector<TResourceExecuteCall> Calls_;
    THashMap<size_t, TFuture<TCompanionResourceExecuteResponsePtr>> DeferredResponses_;
};

using TFakeCompanionClientPtr = TIntrusivePtr<TFakeCompanionClient>;

////////////////////////////////////////////////////////////////////////////////

//! Companion resource with the client minting stubbed out by the test.
class TTestCompanionResource
    : public TCompanionResource
{
public:
    TTestCompanionResource(
        TResourceContextPtr context,
        TDynamicResourceContextPtr dynamicContext,
        ICompanionClientPtr client)
        : TCompanionResource(std::move(context), std::move(dynamicContext))
        , Client_(std::move(client))
    { }

protected:
    ICompanionClientPtr CreateCompanionClient(
        const THashMap<TResourceId, IResourcePtr>& /*dependencies*/) const override
    {
        return Client_;
    }

private:
    const ICompanionClientPtr Client_;
};

using TTestCompanionResourcePtr = TIntrusivePtr<TTestCompanionResource>;

////////////////////////////////////////////////////////////////////////////////

class TDelayedCompanionResource
    : public TTestCompanionResource
{
public:
    using TTestCompanionResource::TTestCompanionResource;

    void SetDelayPreparation(bool value)
    {
        auto guard = Guard(Lock_);
        DelayPreparation_ = value;
    }

    size_t GetPendingPreparationCount() const
    {
        auto guard = Guard(Lock_);
        return PendingPreparations_.size();
    }

    void CompletePreparation(int index, const TResourceRevisionPtr& revision)
    {
        TPromise<TResourceRevisionPtr> promise;
        {
            auto guard = Guard(Lock_);
            promise = PendingPreparations_.at(index);
        }
        promise.Set(revision);
    }

    void FailPreparation(int index, const TError& error)
    {
        TPromise<TResourceRevisionPtr> promise;
        {
            auto guard = Guard(Lock_);
            promise = PendingPreparations_.at(index);
        }
        promise.Set(error);
    }

protected:
    TFuture<TResourceRevisionPtr> PrepareResourceRevision(
        const TResourceRevisionPtr& targetRevision) override
    {
        auto guard = Guard(Lock_);
        if (!DelayPreparation_) {
            return MakeFuture(targetRevision);
        }
        auto promise = NewPromise<TResourceRevisionPtr>();
        PendingPreparations_.push_back(promise);
        return promise.ToFuture();
    }

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    bool DelayPreparation_ = false;
    std::vector<TPromise<TResourceRevisionPtr>> PendingPreparations_;
};

using TDelayedCompanionResourcePtr = TIntrusivePtr<TDelayedCompanionResource>;

////////////////////////////////////////////////////////////////////////////////

class TSwitchingResourceManager
    : public IResourceManager
{
public:
    void Set(const TResourceId& resourceId, IResourcePtr resource)
    {
        Resources_[resourceId] = std::move(resource);
    }

    IResourcePtr Get(TResourceId resourceId) override
    {
        return GetOrCrash(Resources_, resourceId);
    }

    TFuture<void> Load(TResourceId /*resourceId*/) override
    {
        return OKFuture;
    }

    TFuture<void> LoadRequiredResources(const THashSet<TResourceId>& /*resourceIds*/) override
    {
        return OKFuture;
    }

    void FeedStatus(TResourceId /*resourceId*/, i64 /*morePushedToQueue*/, i64 /*moreFetchedFromQueue*/) override
    { }

    void Reconfigure(
        const THashMap<TResourceId, TDynamicResourceSpecPtr>& /*dynamicSpecs*/,
        const THashMap<TResourceId, TResourceRevisionPtr>& /*targetRevisions*/) override
    { }

    THashMap<TResourceId, TWorkerResourceStatusPtr> CollectResourceStatuses() override
    {
        return {};
    }

    void UpdatePreloadedResources(const THashSet<TResourceId>& /*resourceIds*/) override
    { }

    THashMap<TResourceId, EPreloadedResourceState> GetPreloadedStates() const override
    {
        return {};
    }

    THashMap<TResourceId, TResourceInstanceState> GetResourceInstanceStates() const override
    {
        return {};
    }

private:
    THashMap<TResourceId, IResourcePtr> Resources_;
};

using TSwitchingResourceManagerPtr = TIntrusivePtr<TSwitchingResourceManager>;

////////////////////////////////////////////////////////////////////////////////

class TCompanionResourceTest
    : public ::testing::Test
{
protected:
    TResourceContextPtr CreateResourceContext(
        const std::string& keepAliveInterval = "60s",
        const std::string& initMinBackoff = "1ms",
        const TResourceId& resourceId = TResourceId("my_resource"),
        std::optional<std::pair<TResourceId, TResourceId>> companionDependency = std::nullopt)
    {
        auto context = New<TResourceContext>();
        context->ResourceId = resourceId;
        context->ResourceInstanceId = TResourceInstanceId(TGuid::Create());
        context->ResourceSpec = ConvertTo<TResourceSpecPtr>(TYsonString(Format(R""""(
            {
                resource_class_name="NYT::NFlow::NCompanion::TCompanionResource";
                parameters={
                    companion_resource_class="com.example.MyResource";
                    keep_alive_interval="%v";
                    init_backoff={
                        invocation_count=3;
                        min_backoff="%v";
                        max_backoff="5ms";
                    };
                };
            }
        )"""",
            keepAliveInterval,
            initMinBackoff)));
        if (companionDependency) {
            auto description = New<TResourceDescription>();
            description->Alias = companionDependency->second;
            context->ResourceSpec->Dependencies[companionDependency->first] = std::move(description);
        }
        context->Invoker = ActionQueue_->GetInvoker();
        context->Logger = NLogging::TLogger("TestCompanionResource");
        context->StatusProfiler = CreateSyncStatusProfiler();
        return context;
    }

    static TResourceRevisionPtr CreateResourceRevision(
        i64 revisionId,
        const std::string& preparedPath = {})
    {
        auto revision = New<TResourceRevision>();
        revision->RevisionId = revisionId;
        if (!preparedPath.empty()) {
            revision->Spec = ConvertToNode(TYsonString(Format("{prepared_path=%Qv}",
                preparedPath)));
        }
        return revision;
    }

    static TDynamicResourceContextPtr CreateDynamicResourceContext(
        i64 threshold,
        TResourceRevisionPtr targetRevision = nullptr)
    {
        auto dynamicContext = New<TDynamicResourceContext>();
        dynamicContext->DynamicResourceSpec = ConvertTo<TDynamicResourceSpecPtr>(
            TYsonString(Format("{parameters={threshold=%v}}", threshold)));
        dynamicContext->TargetRevision = std::move(targetRevision);
        return dynamicContext;
    }

    TTestCompanionResourcePtr CreateLoadedResource(const TFakeCompanionClientPtr& client)
    {
        auto resource = New<TTestCompanionResource>(
            CreateResourceContext(),
            CreateDynamicResourceContext(42),
            client);
        NConcurrency::WaitFor(resource->Load({})).ThrowOnError();
        return resource;
    }

    NConcurrency::TActionQueuePtr ActionQueue_ = New<NConcurrency::TActionQueue>("Test");
};

TEST(TCompanionResourceParametersTest, KeepAliveIntervalMustBePositive)
{
    auto parse = [] (TStringBuf keepAliveInterval) {
        auto yson = Format(R""""(
            {
                companion_resource_class="com.example.MyResource";
                keep_alive_interval=%v;
            }
        )"""",
            keepAliveInterval);
        return ConvertTo<TCompanionResourceParametersPtr>(TYsonString(yson));
    };

    EXPECT_EQ(TDuration::Seconds(5), parse("\"5s\"")->KeepAliveInterval);
    // A non-positive period turns the keep-alive into an unpaced RPC loop.
    EXPECT_THROW_WITH_SUBSTRING(parse("\"0s\""), "keep_alive_interval");
    EXPECT_THROW_WITH_SUBSTRING(parse("-1000"), "keep_alive_interval");
}

TEST_F(TCompanionResourceTest, LoadSendsInitWithFullSpecs)
{
    auto client = New<TFakeCompanionClient>();
    auto resource = CreateLoadedResource(client);

    auto calls = client->GetCalls();
    ASSERT_EQ(1u, calls.size());
    EXPECT_EQ(TResourceId("my_resource"), calls[0].ResourceId);
    EXPECT_EQ(ECompanionResourceCommand::Init, calls[0].Command);

    auto argument = ConvertTo<IMapNodePtr>(calls[0].Argument);
    auto spec = argument->GetChildOrThrow("spec")->AsMap();
    EXPECT_EQ(
        "NYT::NFlow::NCompanion::TCompanionResource",
        spec->GetChildValueOrThrow<std::string>("resource_class_name"));
    EXPECT_EQ(
        "com.example.MyResource",
        spec->GetChildOrThrow("parameters")->AsMap()->GetChildValueOrThrow<std::string>("companion_resource_class"));
    auto dynamicSpec = argument->GetChildOrThrow("dynamic_spec")->AsMap();
    EXPECT_EQ(
        42,
        dynamicSpec->GetChildOrThrow("parameters")->AsMap()->GetChildValueOrThrow<i64>("threshold"));
    EXPECT_EQ(
        resource->GetContext()->ResourceInstanceId,
        argument->GetChildValueOrThrow<TResourceInstanceId>("incarnation_id"));
    EXPECT_EQ(0u, argument->GetChildValueOrThrow<ui64>("incarnation_generation"));
    EXPECT_EQ(0u, argument->GetChildValueOrThrow<ui64>("configuration_generation"));
    EXPECT_TRUE(argument->GetChildOrThrow("dependencies")->AsList()->GetChildren().empty());
}

TEST_F(TCompanionResourceTest, LoadRetriesOnErrorThenSucceeds)
{
    auto client = New<TFakeCompanionClient>(std::vector{
        ECompanionResourceExecuteStatus::Error,
        ECompanionResourceExecuteStatus::Ok,
    });
    auto resource = CreateLoadedResource(client);

    EXPECT_EQ(2, client->GetCommandCount(ECompanionResourceCommand::Init));
}

TEST_F(TCompanionResourceTest, LoadFailsAfterInitBackoffExhaustion)
{
    auto client = New<TFakeCompanionClient>(std::vector{
        ECompanionResourceExecuteStatus::Error,
        ECompanionResourceExecuteStatus::Error,
        ECompanionResourceExecuteStatus::Error,
        ECompanionResourceExecuteStatus::Error,
        ECompanionResourceExecuteStatus::Error,
    });
    auto resource = New<TTestCompanionResource>(
        CreateResourceContext(),
        CreateDynamicResourceContext(42),
        client);

    auto error = NConcurrency::WaitFor(resource->Load({}));
    EXPECT_FALSE(error.IsOK());
    // The backoff is configured for three invocations: the initial attempt plus three retries.
    EXPECT_EQ(4, client->GetCommandCount(ECompanionResourceCommand::Init));
}

TEST_F(TCompanionResourceTest, LoadFailsFastOnResourceNotFound)
{
    auto client = New<TFakeCompanionClient>(std::vector{
        ECompanionResourceExecuteStatus::ResourceNotFound,
    });
    auto resource = New<TTestCompanionResource>(
        CreateResourceContext(),
        CreateDynamicResourceContext(42),
        client);

    auto error = NConcurrency::WaitFor(resource->Load({}));
    EXPECT_FALSE(error.IsOK());
    EXPECT_THAT(
        error.GetMessage(),
        testing::HasSubstr("no factory for the companion resource class"));
    // No retries: the companion code has no such class.
    EXPECT_EQ(1, client->GetCommandCount(ECompanionResourceCommand::Init));
}

TEST_F(TCompanionResourceTest, LoadFailsFastOnUnsupportedCommand)
{
    auto client = New<TFakeCompanionClient>(std::vector{
        ECompanionResourceExecuteStatus::Unsupported,
    });
    auto resource = New<TTestCompanionResource>(
        CreateResourceContext(),
        CreateDynamicResourceContext(42),
        client);

    auto error = NConcurrency::WaitFor(resource->Load({}));
    EXPECT_FALSE(error.IsOK());
    EXPECT_EQ(1, client->GetCommandCount(ECompanionResourceCommand::Init));
}

TEST_F(TCompanionResourceTest, LoadFailsWithoutCompanionManagerDependency)
{
    // The stock resource resolves the manager from the dependencies map.
    auto resource = New<TCompanionResource>(
        CreateResourceContext(),
        CreateDynamicResourceContext(42));

    auto error = NConcurrency::WaitFor(resource->Load({}));
    EXPECT_FALSE(error.IsOK());
    EXPECT_THAT(
        error.GetMessage(),
        testing::HasSubstr("must declare a dependency on a companion manager"));
}

TEST_F(TCompanionResourceTest, ReconfigureSendsConvergentInitWithFreshDynamicSpec)
{
    auto client = New<TFakeCompanionClient>();
    auto resource = CreateLoadedResource(client);

    resource->Reconfigure(CreateDynamicResourceContext(43));

    WaitForPredicate([&] {
        return client->GetCommandCount(ECompanionResourceCommand::Init) >= 2;
    });
    auto calls = client->GetCalls();
    const auto& reconfigureCall = calls.back();
    EXPECT_EQ(ECompanionResourceCommand::Init, reconfigureCall.Command);
    auto argument = ConvertTo<IMapNodePtr>(reconfigureCall.Argument);
    EXPECT_EQ(
        43,
        argument->GetChildOrThrow("dynamic_spec")->AsMap()->GetChildOrThrow("parameters")->AsMap()->GetChildValueOrThrow<i64>("threshold"));
    EXPECT_EQ(
        resource->GetContext()->ResourceInstanceId,
        argument->GetChildValueOrThrow<TResourceInstanceId>("incarnation_id"));
    EXPECT_EQ(1u, argument->GetChildValueOrThrow<ui64>("configuration_generation"));
}

TEST_F(TCompanionResourceTest, DelayedPreparationPublishesAndAdvancesAppliedRevision)
{
    auto client = New<TFakeCompanionClient>();
    auto resource = New<TDelayedCompanionResource>(
        CreateResourceContext(),
        CreateDynamicResourceContext(42),
        client);
    NConcurrency::WaitFor(resource->Load({})).ThrowOnError();
    resource->SetDelayPreparation(true);

    std::atomic<int> stateChangeCount = 0;
    resource->SubscribeCompanionStateChanged(BIND([&] {
        ++stateChangeCount;
    }));

    resource->Reconfigure(CreateDynamicResourceContext(
        43,
        CreateResourceRevision(7)));

    WaitForPredicate([&] {
        return resource->GetPendingPreparationCount() == 1;
    });
    auto pendingState = resource->GetRevisionState();
    EXPECT_FALSE(pendingState.AppliedRevisionId);
    EXPECT_EQ(7, pendingState.TargetRevisionId);
    EXPECT_EQ(1, client->GetCommandCount(ECompanionResourceCommand::Init));
    EXPECT_EQ(0, stateChangeCount.load());

    resource->CompletePreparation(0, CreateResourceRevision(7, "/tmp/prepared"));
    WaitForPredicate([&] {
        return client->GetCommandCount(ECompanionResourceCommand::Init) >= 2;
    });
    auto argument = ConvertTo<TInitResourceCommandArg>(client->GetCalls().back().Argument);
    ASSERT_TRUE(argument.ResourceRevision);
    EXPECT_EQ(7, argument.ResourceRevision->RevisionId);
    EXPECT_EQ(
        "/tmp/prepared",
        argument.ResourceRevision->Spec->AsMap()->GetChildValueOrThrow<std::string>("prepared_path"));
    EXPECT_EQ(1u, argument.ConfigurationGeneration);

    auto appliedState = resource->GetRevisionState();
    EXPECT_EQ(7, appliedState.AppliedRevisionId);
    EXPECT_EQ(7, appliedState.TargetRevisionId);
    EXPECT_EQ(1, stateChangeCount.load());

    auto computationClient = New<TFakeCompanionClient>();
    resource->InitInCompanion(computationClient);
    ASSERT_EQ(1u, computationClient->GetCalls().size());
    auto replayedArgument = ConvertTo<TInitResourceCommandArg>(
        computationClient->GetCalls()[0].Argument);
    EXPECT_EQ(1u, replayedArgument.ConfigurationGeneration);
    ASSERT_TRUE(replayedArgument.ResourceRevision);
    EXPECT_EQ(7, replayedArgument.ResourceRevision->RevisionId);
    EXPECT_EQ(1, stateChangeCount.load());
}

TEST_F(TCompanionResourceTest, ObsoletePreparationDoesNotPublish)
{
    auto client = New<TFakeCompanionClient>();
    auto resource = New<TDelayedCompanionResource>(
        CreateResourceContext(),
        CreateDynamicResourceContext(42),
        client);
    NConcurrency::WaitFor(resource->Load({})).ThrowOnError();
    resource->SetDelayPreparation(true);
    std::atomic<int> stateChangeCount = 0;
    resource->SubscribeCompanionStateChanged(BIND([&] {
        ++stateChangeCount;
    }));

    resource->Reconfigure(CreateDynamicResourceContext(
        43,
        CreateResourceRevision(1)));
    WaitForPredicate([&] {
        return resource->GetPendingPreparationCount() == 1;
    });
    resource->Reconfigure(CreateDynamicResourceContext(
        44,
        CreateResourceRevision(2)));
    WaitForPredicate([&] {
        return resource->GetPendingPreparationCount() == 2;
    });

    resource->CompletePreparation(0, CreateResourceRevision(1, "/tmp/obsolete"));
    NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(10));
    EXPECT_EQ(1, client->GetCommandCount(ECompanionResourceCommand::Init));
    EXPECT_EQ(0, stateChangeCount.load());

    resource->CompletePreparation(1, CreateResourceRevision(2, "/tmp/current"));
    WaitForPredicate([&] {
        return client->GetCommandCount(ECompanionResourceCommand::Init) >= 2;
    });
    auto argument = ConvertTo<TInitResourceCommandArg>(client->GetCalls().back().Argument);
    EXPECT_EQ(1u, argument.ConfigurationGeneration);
    ASSERT_TRUE(argument.ResourceRevision);
    EXPECT_EQ(2, argument.ResourceRevision->RevisionId);
    EXPECT_EQ(1, stateChangeCount.load());
}

TEST_F(TCompanionResourceTest, FailedPublicationDoesNotAdvanceAppliedRevision)
{
    auto client = New<TFakeCompanionClient>(std::vector{
        ECompanionResourceExecuteStatus::Ok,
        ECompanionResourceExecuteStatus::Error,
    });
    auto resource = New<TDelayedCompanionResource>(
        CreateResourceContext(),
        CreateDynamicResourceContext(42),
        client);
    NConcurrency::WaitFor(resource->Load({})).ThrowOnError();
    resource->SetDelayPreparation(true);
    std::atomic<int> stateChangeCount = 0;
    resource->SubscribeCompanionStateChanged(BIND([&] {
        ++stateChangeCount;
    }));

    resource->Reconfigure(CreateDynamicResourceContext(
        43,
        CreateResourceRevision(8)));
    WaitForPredicate([&] {
        return resource->GetPendingPreparationCount() == 1;
    });
    resource->CompletePreparation(0, CreateResourceRevision(8, "/tmp/prepared"));
    WaitForPredicate([&] {
        return client->GetCommandCount(ECompanionResourceCommand::Init) >= 2;
    });

    auto state = resource->GetRevisionState();
    EXPECT_FALSE(state.AppliedRevisionId);
    EXPECT_EQ(8, state.TargetRevisionId);
    EXPECT_EQ(0u, resource->GetReference().ConfigurationGeneration);
    EXPECT_EQ(0, stateChangeCount.load());
    auto argument = ConvertTo<TInitResourceCommandArg>(client->GetCalls().back().Argument);
    EXPECT_EQ(1u, argument.ConfigurationGeneration);
}

TEST_F(TCompanionResourceTest, OverlappingPublicationsExposeOnlyCompletedGeneration)
{
    auto client = New<TFakeCompanionClient>();
    auto firstResponse = client->DeferResponse(1);
    auto secondResponse = client->DeferResponse(2);
    auto resource = New<TDelayedCompanionResource>(
        CreateResourceContext(),
        CreateDynamicResourceContext(42),
        client);
    NConcurrency::WaitFor(resource->Load({})).ThrowOnError();
    resource->SetDelayPreparation(true);

    NThreading::TSpinLock signalLock;
    std::vector<ui64> signaledGenerations;
    resource->SubscribeCompanionStateChanged(BIND([&] {
        auto generation = resource->GetReference().ConfigurationGeneration;
        auto guard = Guard(signalLock);
        signaledGenerations.push_back(generation);
    }));

    resource->Reconfigure(CreateDynamicResourceContext(
        43,
        CreateResourceRevision(1)));
    WaitForPredicate([&] {
        return resource->GetPendingPreparationCount() == 1;
    });
    resource->CompletePreparation(0, CreateResourceRevision(1, "/tmp/first"));
    WaitForPredicate([&] {
        return client->GetCommandCount(ECompanionResourceCommand::Init) == 2;
    });

    resource->Reconfigure(CreateDynamicResourceContext(
        44,
        CreateResourceRevision(2)));
    WaitForPredicate([&] {
        return resource->GetPendingPreparationCount() == 2;
    });
    resource->CompletePreparation(1, CreateResourceRevision(2, "/tmp/second"));
    WaitForPredicate([&] {
        return client->GetCommandCount(ECompanionResourceCommand::Init) == 3;
    });

    auto response = New<TCompanionResourceExecuteResponse>();
    response->Status = ECompanionResourceExecuteStatus::Ok;
    firstResponse.Set(response);
    WaitForPredicate([&] {
        auto guard = Guard(signalLock);
        return signaledGenerations.size() == 1;
    });
    EXPECT_EQ(1u, resource->GetReference().ConfigurationGeneration);

    response = New<TCompanionResourceExecuteResponse>();
    response->Status = ECompanionResourceExecuteStatus::Ok;
    secondResponse.Set(response);
    WaitForPredicate([&] {
        auto guard = Guard(signalLock);
        return signaledGenerations.size() == 2;
    });
    EXPECT_EQ(2u, resource->GetReference().ConfigurationGeneration);

    auto guard = Guard(signalLock);
    EXPECT_EQ((std::vector<ui64>{1, 2}), signaledGenerations);
}

TEST_F(TCompanionResourceTest, FailedPreparationRetriesWithoutPublishingStaleState)
{
    auto client = New<TFakeCompanionClient>();
    auto resource = New<TDelayedCompanionResource>(
        CreateResourceContext(/*keepAliveInterval*/ "10ms"),
        CreateDynamicResourceContext(42),
        client);
    NConcurrency::WaitFor(resource->Load({})).ThrowOnError();
    resource->SetDelayPreparation(true);

    resource->Reconfigure(CreateDynamicResourceContext(
        43,
        CreateResourceRevision(9)));
    WaitForPredicate([&] {
        return resource->GetPendingPreparationCount() == 1;
    });
    resource->FailPreparation(0, TError("preparation failed"));
    WaitForPredicate([&] {
        return resource->GetPendingPreparationCount() == 2;
    });

    EXPECT_EQ(1, client->GetCommandCount(ECompanionResourceCommand::Init));
    auto state = resource->GetRevisionState();
    EXPECT_FALSE(state.AppliedRevisionId);
    EXPECT_EQ(9, state.TargetRevisionId);

    resource->CompletePreparation(1, CreateResourceRevision(9, "/tmp/prepared"));
    WaitForPredicate([&] {
        return client->GetCommandCount(ECompanionResourceCommand::Init) >= 2;
    });
    state = resource->GetRevisionState();
    EXPECT_EQ(9, state.AppliedRevisionId);
}

TEST_F(TCompanionResourceTest, ReconfigureDuringInitialPreparationIsNotLost)
{
    auto client = New<TFakeCompanionClient>();
    auto resource = New<TDelayedCompanionResource>(
        CreateResourceContext(),
        CreateDynamicResourceContext(42),
        client);
    resource->SetDelayPreparation(true);

    auto loadFuture = resource->Load({});
    WaitForPredicate([&] {
        return resource->GetPendingPreparationCount() == 1;
    });
    resource->Reconfigure(CreateDynamicResourceContext(
        43,
        CreateResourceRevision(10)));
    resource->CompletePreparation(0, nullptr);
    NConcurrency::WaitFor(loadFuture).ThrowOnError();

    WaitForPredicate([&] {
        return resource->GetPendingPreparationCount() == 2;
    });
    resource->CompletePreparation(1, CreateResourceRevision(10, "/tmp/prepared"));
    WaitForPredicate([&] {
        return client->GetCommandCount(ECompanionResourceCommand::Init) >= 2;
    });

    auto argument = ConvertTo<TInitResourceCommandArg>(client->GetCalls().back().Argument);
    EXPECT_EQ(43, argument.DynamicSpec->Parameters->GetChildValueOrThrow<i64>("threshold"));
    ASSERT_TRUE(argument.ResourceRevision);
    EXPECT_EQ(10, argument.ResourceRevision->RevisionId);
}

TEST_F(TCompanionResourceTest, KeepAliveSendsIdempotentInit)
{
    auto client = New<TFakeCompanionClient>();
    auto resource = New<TTestCompanionResource>(
        CreateResourceContext(/*keepAliveInterval*/ "10ms"),
        CreateDynamicResourceContext(42),
        client);
    NConcurrency::WaitFor(resource->Load({})).ThrowOnError();

    WaitForPredicate([&] {
        return client->GetCommandCount(ECompanionResourceCommand::Init) >= 3;
    });
}

TEST_F(TCompanionResourceTest, KeepAliveStopsAfterStaleIncarnation)
{
    auto client = New<TFakeCompanionClient>(std::vector{
        ECompanionResourceExecuteStatus::Ok,
        ECompanionResourceExecuteStatus::StaleResourceIncarnation,
    });
    auto resource = New<TTestCompanionResource>(
        CreateResourceContext(/*keepAliveInterval*/ "10ms"),
        CreateDynamicResourceContext(42),
        client);
    NConcurrency::WaitFor(resource->Load({})).ThrowOnError();

    WaitForPredicate([&] {
        return client->GetCommandCount(ECompanionResourceCommand::Init) == 2;
    });
    NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(50));
    EXPECT_EQ(2, client->GetCommandCount(ECompanionResourceCommand::Init));
}

TEST_F(TCompanionResourceTest, KeepAliveReplaysPreparedRevisionAndGeneration)
{
    auto client = New<TFakeCompanionClient>();
    auto resource = New<TTestCompanionResource>(
        CreateResourceContext(/*keepAliveInterval*/ "10ms"),
        CreateDynamicResourceContext(
            42,
            CreateResourceRevision(3, "/tmp/prepared")),
        client);
    NConcurrency::WaitFor(resource->Load({})).ThrowOnError();

    WaitForPredicate([&] {
        return client->GetCommandCount(ECompanionResourceCommand::Init) >= 3;
    });
    for (const auto& call : client->GetCalls()) {
        if (call.Command != ECompanionResourceCommand::Init) {
            continue;
        }
        auto argument = ConvertTo<TInitResourceCommandArg>(call.Argument);
        EXPECT_EQ(0u, argument.ConfigurationGeneration);
        ASSERT_TRUE(argument.ResourceRevision);
        EXPECT_EQ(3, argument.ResourceRevision->RevisionId);
    }
}

TEST_F(TCompanionResourceTest, DestructorSendsBestEffortUnload)
{
    auto client = New<TFakeCompanionClient>();
    auto resource = CreateLoadedResource(client);

    resource.Reset();

    WaitForPredicate([&] {
        return client->GetCommandCount(ECompanionResourceCommand::Unload) >= 1;
    });
    auto calls = client->GetCalls();
    const auto& unloadCall = calls.back();
    EXPECT_EQ(TResourceId("my_resource"), unloadCall.ResourceId);
    auto argument = ConvertTo<TUnloadResourceCommandArg>(unloadCall.Argument);
    EXPECT_NE(TResourceInstanceId{}, argument.IncarnationId);
}

TEST_F(TCompanionResourceTest, DestructorUnloadsEveryInitializedCompanionProcess)
{
    auto primaryClient = New<TFakeCompanionClient>();
    auto firstComputationClient = New<TFakeCompanionClient>();
    auto secondComputationClient = New<TFakeCompanionClient>();
    auto resource = CreateLoadedResource(primaryClient);

    resource->InitInCompanion(firstComputationClient, 101);
    resource->InitInCompanion(secondComputationClient, 102);
    resource.Reset();

    WaitForPredicate([&] {
        return primaryClient->GetCommandCount(ECompanionResourceCommand::Unload) >= 1 &&
            firstComputationClient->GetCommandCount(ECompanionResourceCommand::Unload) >= 1 &&
            secondComputationClient->GetCommandCount(ECompanionResourceCommand::Unload) >= 1;
    });
}

TEST_F(TCompanionResourceTest, InitInCompanionUsesProvidedClient)
{
    auto loadClient = New<TFakeCompanionClient>();
    auto resource = CreateLoadedResource(loadClient);

    // The computation shim initializes the resource through its own channel.
    auto shimClient = New<TFakeCompanionClient>();
    resource->InitInCompanion(shimClient);

    ASSERT_EQ(1u, shimClient->GetCalls().size());
    EXPECT_EQ(ECompanionResourceCommand::Init, shimClient->GetCalls()[0].Command);
    EXPECT_EQ(1, loadClient->GetCommandCount(ECompanionResourceCommand::Init));
}

TEST_F(TCompanionResourceTest, ResolvesCurrentObjectAfterResourceManagerRecreation)
{
    auto manager = New<TSwitchingResourceManager>();
    auto client = New<TFakeCompanionClient>();

    auto staleContext = CreateResourceContext();
    staleContext->ResourceManager = MakeWeak(manager.Get());
    auto stale = New<TTestCompanionResource>(
        staleContext,
        CreateDynamicResourceContext(1),
        client);

    auto currentContext = CreateResourceContext();
    currentContext->ResourceManager = MakeWeak(manager.Get());
    auto current = New<TTestCompanionResource>(
        currentContext,
        CreateDynamicResourceContext(2),
        client);
    manager->Set(currentContext->ResourceId, current);

    EXPECT_EQ(current, stale->GetCurrentResource());
}

TEST_F(TCompanionResourceTest, InitializesDependencyGraphInTopologicalOrder)
{
    auto loadClient = New<TFakeCompanionClient>();
    auto dependency = New<TTestCompanionResource>(
        CreateResourceContext("60s", "1ms", TResourceId("dependency")),
        CreateDynamicResourceContext(1),
        loadClient);
    NConcurrency::WaitFor(dependency->Load({})).ThrowOnError();

    auto parent = New<TTestCompanionResource>(
        CreateResourceContext(
            "60s",
            "1ms",
            TResourceId("parent"),
            std::pair(TResourceId("dependency"), TResourceId("Dictionary"))),
        CreateDynamicResourceContext(2),
        loadClient);
    NConcurrency::WaitFor(parent->Load({{TResourceId("Dictionary"), dependency}})).ThrowOnError();

    auto calls = loadClient->GetCalls();
    ASSERT_GE(calls.size(), 3u);
    EXPECT_EQ(TResourceId("dependency"), calls[calls.size() - 2].ResourceId);
    EXPECT_EQ(TResourceId("parent"), calls[calls.size() - 1].ResourceId);

    auto parentArgument = ConvertTo<TInitResourceCommandArg>(calls.back().Argument);
    ASSERT_EQ(1u, parentArgument.Dependencies.size());
    EXPECT_EQ(TResourceId("dependency"), parentArgument.Dependencies[0].ResourceId);
    ASSERT_TRUE(parentArgument.Dependencies[0].Alias);
    EXPECT_EQ(TResourceId("Dictionary"), *parentArgument.Dependencies[0].Alias);

    auto references = parent->GetCompanionResourceReferences(TResourceId("Greeting"));
    ASSERT_EQ(2u, references.size());
    EXPECT_EQ(TResourceId("dependency"), references[0].ResourceId);
    EXPECT_FALSE(references[0].Alias);
    EXPECT_EQ(TResourceId("parent"), references[1].ResourceId);
    ASSERT_TRUE(references[1].Alias);
    EXPECT_EQ(TResourceId("Greeting"), *references[1].Alias);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanion
