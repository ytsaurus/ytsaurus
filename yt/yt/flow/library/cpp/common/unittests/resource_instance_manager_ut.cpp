#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/resource.h>
#include <yt/yt/flow/library/cpp/common/resource_manager.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/context_switch.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <util/system/type_name.h>

namespace NYT::NFlow {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TControlledInstanceResource
    : public TResourceLifecycleBase
{
public:
    TControlledInstanceResource(TResourceContextPtr, TDynamicResourceContextPtr)
    { }

    TPromise<void> Started = NewPromise<void>();
    TPromise<void> Completion = NewPromise<void>();
    std::atomic<int> LoadCount = 0;
    THashMap<TResourceId, IResourcePtr> Dependencies;

    TFuture<void> Load(const THashMap<TResourceId, IResourcePtr>& dependencies) override
    {
        ++LoadCount;
        Dependencies = dependencies;
        Started.TrySet();
        return Completion.ToFuture();
    }

    void Reconfigure(const TDynamicResourceContextPtr&) override
    { }

    TResourceRevisionState GetRevisionState() const override
    {
        return {};
    }

    NYTree::TYsonStructPtr GetParametersBase() const override
    {
        return nullptr;
    }

    NYTree::TYsonStructPtr GetDynamicParametersBase() const override
    {
        return nullptr;
    }
};

YT_FLOW_DEFINE_RESOURCE(TControlledInstanceResource);

class TFiberInstanceResource
    : public TControlledInstanceResource
{
public:
    TFiberInstanceResource(TResourceContextPtr context, TDynamicResourceContextPtr dynamicContext)
        : TControlledInstanceResource(context, std::move(dynamicContext))
        , Invoker_(context->Invoker)
    { }

    TFuture<void> Load(const THashMap<TResourceId, IResourcePtr>& dependencies) override
    {
        return BIND([this, this_ = MakeStrong(this), dependencies] {
            WaitFor(TControlledInstanceResource::Load(dependencies)).ThrowOnError();
        }).AsyncVia(Invoker_)
            .Run();
    }

private:
    const IInvokerPtr Invoker_;
};

YT_FLOW_DEFINE_RESOURCE(TFiberInstanceResource);

class TYieldingDestructorInstanceResource
    : public TControlledInstanceResource
{
public:
    TYieldingDestructorInstanceResource(TResourceContextPtr context, TDynamicResourceContextPtr dynamicContext)
        : TControlledInstanceResource(std::move(context), std::move(dynamicContext))
    { }

    ~TYieldingDestructorInstanceResource() override
    {
        DestructionStarted.Set();
        WaitFor(DestructionCompletion.ToFuture()).ThrowOnError();
    }

    TPromise<void> DestructionStarted = NewPromise<void>();
    TPromise<void> DestructionCompletion = NewPromise<void>();
};

YT_FLOW_DEFINE_RESOURCE(TYieldingDestructorInstanceResource);

class TResourceInstanceManagerTest
    : public ::testing::Test
{
protected:
    const TActionQueuePtr Queue_ = New<TActionQueue>("ResourceTest");
    IResourceManagerPtr Manager_;
    THashMap<TResourceId, TResourceSpecPtr> Specs_;
    THashMap<TResourceInstanceId, TResourceInstancePtr> Instances_;
    THashMap<TResourceInstanceId, TResourceInstanceDynamicSpecPtr> DynamicSpecs_;

    void SetUp() override
    {
        auto context = New<TResourceManagerContext>();
        context->Invoker = Queue_->GetInvoker();
        context->StatusProfiler = CreateSyncStatusProfiler();
        Manager_ = CreateResourceManager(context);
    }

    void TearDown() override
    {
        Manager_.Reset();
        Queue_->Shutdown();
    }

    TResourceInstanceId Add(TResourceId resourceId = TResourceId("resource"))
    {
        auto spec = New<TResourceSpec>();
        spec->ResourceClassName = TypeName<TControlledInstanceResource>();
        Specs_[resourceId] = spec;
        auto instance = New<TResourceInstance>();
        instance->ResourceId = resourceId;
        instance->ResourceInstanceId = TResourceInstanceId(TGuid::Create());
        Instances_[instance->ResourceInstanceId] = instance;
        DynamicSpecs_[instance->ResourceInstanceId] = New<TResourceInstanceDynamicSpec>();
        return instance->ResourceInstanceId;
    }

    void FlushInvoker()
    {
        WaitFor(BIND([] {
        }).AsyncVia(Queue_->GetInvoker())
                .Run())
            .ThrowOnError();
    }

    void Reconfigure()
    {
        Manager_->ReconfigureInstances(Specs_, Instances_, DynamicSpecs_);
    }

    TIntrusivePtr<TControlledInstanceResource> Get(TResourceInstanceId id)
    {
        return DynamicPointerCast<TControlledInstanceResource>(Manager_->Get(id));
    }
};

TEST_F(TResourceInstanceManagerTest, LazyAndSharedLoad)
{
    auto id = Add();
    Reconfigure();
    auto resource = Get(id);
    EXPECT_EQ(Manager_->CollectResourceInstanceStatuses().at(id)->LoadState, EResourceLoadState::NotLoaded);
    EXPECT_EQ(resource->LoadCount, 0);
    auto first = Manager_->Load({id});
    auto second = Manager_->Load({id});
    WaitFor(resource->Started.ToFuture()).ThrowOnError();
    EXPECT_EQ(resource->LoadCount, 1);
    EXPECT_EQ(Manager_->CollectResourceInstanceStatuses().at(id)->LoadState, EResourceLoadState::Loading);
    resource->Completion.Set();
    WaitFor(first).ThrowOnError();
    WaitFor(second).ThrowOnError();
    EXPECT_EQ(Manager_->CollectResourceInstanceStatuses().at(id)->LoadState, EResourceLoadState::Loaded);
}

TEST_F(TResourceInstanceManagerTest, FailureIsCachedUntilReplacement)
{
    auto id = Add();
    Reconfigure();
    auto resource = Get(id);
    auto first = Manager_->Load({id});
    auto error = TError("Instance failed");
    resource->Completion.Set(error);
    EXPECT_FALSE(WaitFor(first).IsOK());
    EXPECT_FALSE(WaitFor(Manager_->Load({id})).IsOK());
    auto status = Manager_->CollectResourceInstanceStatuses().at(id);
    EXPECT_EQ(status->LoadState, EResourceLoadState::Failed);
    EXPECT_EQ(status->LoadError, error);
    Reconfigure();
    EXPECT_EQ(Get(id), resource);
    EXPECT_EQ(resource->LoadCount, 1);
    Instances_.clear();
    auto replacement = Add();
    Reconfigure();
    EXPECT_NE(Get(replacement), resource);
    EXPECT_EQ(Manager_->CollectResourceInstanceStatuses().at(replacement)->LoadState, EResourceLoadState::NotLoaded);
    EXPECT_FALSE(Manager_->CollectResourceInstanceStatuses().contains(id));
    EXPECT_THROW(Manager_->Get(id), std::exception);
}

TEST_F(TResourceInstanceManagerTest, RemovalReleasesOwnershipAndKeepsOldStatusSeparate)
{
    auto id = Add();
    Reconfigure();
    auto resource = Get(id);
    auto weak = MakeWeak(resource);
    auto destroyed = resource->GetDestroyedFuture();
    Instances_.clear();
    auto replacement = Add();
    Reconfigure();
    Manager_->FeedStatus(id, 7, 2);
    Manager_->FeedStatus(replacement, 3, 1);
    auto statuses = Manager_->CollectResourceInstanceStatuses();
    EXPECT_EQ(statuses.size(), 1u);
    EXPECT_FALSE(statuses.contains(id));
    EXPECT_TRUE(statuses.contains(replacement));
    resource.Reset();
    WaitFor(destroyed.WithTimeout(TDuration::Seconds(3))).ThrowOnError();
    EXPECT_TRUE(weak.IsExpired());
    EXPECT_FALSE(Manager_->CollectResourceInstanceStatuses().contains(id));
}

TEST_F(TResourceInstanceManagerTest, ReplacementWaitsForYieldingPredecessorDestruction)
{
    auto oldId = Add();
    Specs_.at(TResourceId("resource"))->ResourceClassName = TypeName<TYieldingDestructorInstanceResource>();
    Reconfigure();
    auto oldResource = DynamicPointerCast<TYieldingDestructorInstanceResource>(Manager_->Get(oldId));
    auto destructionStarted = oldResource->DestructionStarted.ToFuture();
    auto destructionCompletion = oldResource->DestructionCompletion;
    auto destroyed = oldResource->GetDestroyedFuture();
    oldResource.Reset();

    Instances_.clear();
    auto replacementId = Add();
    WaitFor(BIND([&] {
        TForbidContextSwitchGuard guard;
        Reconfigure();
    }).AsyncVia(Queue_->GetInvoker())
            .Run())
        .ThrowOnError();
    WaitFor(destructionStarted.WithTimeout(TDuration::Seconds(3))).ThrowOnError();

    auto replacement = Get(replacementId);
    auto loaded = Manager_->Load({replacementId});
    FlushInvoker();
    EXPECT_FALSE(destroyed.IsSet());
    EXPECT_FALSE(replacement->Started.IsSet());
    EXPECT_FALSE(loaded.IsSet());

    destructionCompletion.Set();
    WaitFor(destroyed.WithTimeout(TDuration::Seconds(3))).ThrowOnError();
    WaitFor(replacement->Started.ToFuture().WithTimeout(TDuration::Seconds(3))).ThrowOnError();
    replacement->Completion.Set();
    WaitFor(loaded).ThrowOnError();
}

TEST_F(TResourceInstanceManagerTest, CancelingLoadReleasesRetiredDependencyOutsideHeartbeat)
{
    auto oldId = Add();
    auto dependencyId = Add(TResourceId("dependency"));
    Specs_.at(TResourceId("dependency"))->ResourceClassName = TypeName<TYieldingDestructorInstanceResource>();
    Reconfigure();
    auto oldResource = Get(oldId);
    auto dependency = DynamicPointerCast<TYieldingDestructorInstanceResource>(Manager_->Get(dependencyId));
    dependency->Completion.Set();
    WaitFor(Manager_->Load({dependencyId})).ThrowOnError();
    auto destructionStarted = dependency->DestructionStarted.ToFuture();
    auto destructionCompletion = dependency->DestructionCompletion;
    auto destroyed = dependency->GetDestroyedFuture();
    dependency.Reset();

    Instances_.erase(oldId);
    auto id = Add();
    Specs_.at(TResourceId("resource"))->Dependencies.emplace(TResourceId("dependency"), New<TResourceDescription>());
    Reconfigure();
    auto loaded = Manager_->Load({id});
    FlushInvoker();

    Instances_.erase(dependencyId);
    auto replacementId = Add(TResourceId("dependency"));
    Reconfigure();
    FlushInvoker();
    EXPECT_FALSE(destroyed.IsSet());

    Instances_.erase(id);
    WaitFor(BIND([&] {
        TForbidContextSwitchGuard guard;
        Reconfigure();
    }).AsyncVia(Queue_->GetInvoker())
            .Run())
        .ThrowOnError();
    WaitFor(destructionStarted.WithTimeout(TDuration::Seconds(3))).ThrowOnError();

    auto replacement = Get(replacementId);
    auto replacementLoad = Manager_->Load({replacementId});
    FlushInvoker();
    EXPECT_FALSE(destroyed.IsSet());
    EXPECT_FALSE(replacement->Started.IsSet());

    destructionCompletion.Set();
    WaitFor(destroyed.WithTimeout(TDuration::Seconds(3))).ThrowOnError();
    EXPECT_FALSE(WaitFor(loaded.WithTimeout(TDuration::Seconds(3))).IsOK());
    EXPECT_TRUE(loaded.IsSet());
    WaitFor(replacement->Started.ToFuture().WithTimeout(TDuration::Seconds(3))).ThrowOnError();
    replacement->Completion.Set();
    WaitFor(replacementLoad).ThrowOnError();
}

TEST_F(TResourceInstanceManagerTest, DependenciesUsePinnedInstances)
{
    auto dependencyId = Add(TResourceId("dependency"));
    auto id = Add();
    auto description = New<TResourceDescription>();
    description->Alias = TResourceId("alias");
    Specs_.at(TResourceId("resource"))->Dependencies.emplace(TResourceId("dependency"), description);
    Reconfigure();
    auto dependency = Get(dependencyId);
    auto resource = Get(id);
    auto loaded = Manager_->Load({id});
    WaitFor(dependency->Started.ToFuture().WithTimeout(TDuration::Seconds(3))).ThrowOnError();
    EXPECT_EQ(resource->LoadCount, 0);
    dependency->Completion.Set();
    WaitFor(resource->Started.ToFuture()).ThrowOnError();
    EXPECT_EQ(resource->Dependencies.at(TResourceId("alias")), dependency);
    resource->Completion.Set();
    WaitFor(loaded).ThrowOnError();
    Instances_.erase(dependencyId);
    auto replacement = Add(TResourceId("dependency"));
    Reconfigure();
    EXPECT_EQ(resource->Dependencies.at(TResourceId("alias")), dependency);
    EXPECT_NE(Get(replacement), dependency);
}

TEST_F(TResourceInstanceManagerTest, PendingLoadDoesNotBlockRemoval)
{
    auto id = Add();
    Reconfigure();
    auto resource = Get(id);
    auto completion = resource->Completion;
    auto started = resource->Started.ToFuture();
    auto loaded = Manager_->Load({id});
    WaitFor(started).ThrowOnError();
    FlushInvoker();
    Instances_.clear();
    Reconfigure();
    resource.Reset();
    EXPECT_TRUE(Manager_->CollectResourceInstanceStatuses().empty());
    EXPECT_FALSE(WaitFor(loaded.WithTimeout(TDuration::Seconds(3))).IsOK());
    EXPECT_TRUE(loaded.IsSet());
    EXPECT_TRUE(completion.IsCanceled());
    WaitFor(BIND([] {
    }).AsyncVia(Queue_->GetInvoker())
            .Run())
        .ThrowOnError();
    EXPECT_TRUE(Manager_->CollectResourceInstanceStatuses().empty());
}

TEST_F(TResourceInstanceManagerTest, OldLoadCompletionDoesNotChangeReplacementStatus)
{
    auto oldId = Add();
    Reconfigure();
    auto oldResource = Get(oldId);
    // Model a load that acknowledges cancellation only when the test completes it.
    oldResource->Completion.OnCanceled(BIND([] (const TError&) {
    }));
    auto oldLoad = Manager_->Load({oldId});
    WaitFor(oldResource->Started.ToFuture()).ThrowOnError();
    Instances_.clear();
    auto newId = Add();
    Reconfigure();
    auto newResource = Get(newId);
    auto newLoad = Manager_->Load({newId});
    FlushInvoker();
    EXPECT_FALSE(newResource->Started.IsSet());
    oldResource->Completion.Set(TError("Old instance failed"));
    EXPECT_FALSE(WaitFor(oldLoad).IsOK());
    FlushInvoker();
    EXPECT_FALSE(newResource->Started.IsSet());
    oldResource.Reset();
    WaitFor(newResource->Started.ToFuture().WithTimeout(TDuration::Seconds(3))).ThrowOnError();
    Manager_->FeedStatus(oldId, 100, 0);
    auto statuses = Manager_->CollectResourceInstanceStatuses();
    ASSERT_EQ(statuses.size(), 1u);
    EXPECT_EQ(statuses.at(newId)->LoadState, EResourceLoadState::Loading);
    EXPECT_TRUE(statuses.at(newId)->LoadError.IsOK());
    EXPECT_FALSE(WaitFor(Manager_->Load({oldId})).IsOK());
    newResource->Completion.Set();
    WaitFor(newLoad).ThrowOnError();
    EXPECT_EQ(Manager_->CollectResourceInstanceStatuses().at(newId)->LoadState, EResourceLoadState::Loaded);
}

TEST_F(TResourceInstanceManagerTest, ReplacementWaitsForRetainedPredecessor)
{
    auto oldId = Add();
    Reconfigure();
    auto oldResource = Get(oldId);
    auto oldLoad = Manager_->Load({oldId});
    oldResource->Completion.Set();
    WaitFor(oldLoad).ThrowOnError();

    Instances_.clear();
    Reconfigure();
    auto newId = Add();
    Reconfigure();
    auto newResource = Get(newId);
    auto loaded = Manager_->Load({newId});
    FlushInvoker();
    EXPECT_FALSE(newResource->Started.IsSet());
    EXPECT_FALSE(loaded.IsSet());

    oldResource.Reset();
    WaitFor(newResource->Started.ToFuture().WithTimeout(TDuration::Seconds(3))).ThrowOnError();
    newResource->Completion.Set();
    WaitFor(loaded).ThrowOnError();
}

TEST_F(TResourceInstanceManagerTest, SkippedReplacementPreservesRetirementBarrier)
{
    auto firstId = Add();
    Reconfigure();
    auto first = Get(firstId);
    Instances_.clear();
    auto middleId = Add();
    Reconfigure();
    auto middle = Get(middleId);
    auto middleStarted = middle->Started.ToFuture();
    auto middleLoad = Manager_->Load({middleId});
    FlushInvoker();
    EXPECT_FALSE(middleStarted.IsSet());
    middle->Completion.Set();
    Instances_.clear();
    Reconfigure();
    auto middleWeak = MakeWeak(middle);
    auto middleDestroyed = middle->GetDestroyedFuture();
    middle.Reset();
    WaitFor(middleDestroyed.WithTimeout(TDuration::Seconds(3))).ThrowOnError();
    EXPECT_TRUE(middleWeak.IsExpired());

    auto lastId = Add();
    Reconfigure();
    auto last = Get(lastId);
    auto lastLoad = Manager_->Load({lastId});
    FlushInvoker();
    EXPECT_FALSE(last->Started.IsSet());
    first.Reset();
    WaitFor(last->Started.ToFuture().WithTimeout(TDuration::Seconds(3))).ThrowOnError();
    EXPECT_FALSE(WaitFor(middleLoad).IsOK());
    EXPECT_FALSE(WaitFor(middleStarted).IsOK());
    last->Completion.Set();
    WaitFor(lastLoad).ThrowOnError();
}

TEST_F(TResourceInstanceManagerTest, PendingLoadRetainsPredecessorUntilCompletion)
{
    auto oldId = Add();
    Reconfigure();
    auto oldResource = Get(oldId);
    // Model a load that acknowledges cancellation only when the test completes it.
    oldResource->Completion.OnCanceled(BIND([] (const TError&) {
    }));
    auto completion = oldResource->Completion;
    auto weak = MakeWeak(oldResource);
    auto oldLoad = Manager_->Load({oldId});
    WaitFor(oldResource->Started.ToFuture()).ThrowOnError();
    Instances_.clear();
    auto newId = Add();
    Reconfigure();
    auto newResource = Get(newId);
    auto newLoad = Manager_->Load({newId});
    oldResource.Reset();
    FlushInvoker();
    EXPECT_FALSE(weak.IsExpired());
    EXPECT_FALSE(newResource->Started.IsSet());
    completion.Set();
    WaitFor(oldLoad).ThrowOnError();
    WaitFor(newResource->Started.ToFuture().WithTimeout(TDuration::Seconds(3))).ThrowOnError();
    EXPECT_TRUE(weak.IsExpired());
    newResource->Completion.Set();
    WaitFor(newLoad).ThrowOnError();
}

TEST_F(TResourceInstanceManagerTest, MissingDependencyCompletesCachedLoadWithError)
{
    auto id = Add();
    Specs_.at(TResourceId("resource"))->Dependencies.emplace(TResourceId("missing"), New<TResourceDescription>());
    Reconfigure();
    auto first = Manager_->Load({id});
    EXPECT_FALSE(WaitFor(first.WithTimeout(TDuration::Seconds(3))).IsOK());
    EXPECT_TRUE(first.IsSet());
    auto second = Manager_->Load({id});
    EXPECT_FALSE(WaitFor(second.WithTimeout(TDuration::Seconds(3))).IsOK());
    EXPECT_TRUE(second.IsSet());
    EXPECT_EQ(Get(id)->LoadCount, 0);
}

TEST_F(TResourceInstanceManagerTest, RemovedLogicalResourceReturnsError)
{
    auto id = Add();
    Reconfigure();
    auto resource = Get(id);
    Instances_.clear();
    Reconfigure();
    EXPECT_THROW(Manager_->Get(TResourceId("resource")), std::exception);
    EXPECT_FALSE(WaitFor(Manager_->Load(TResourceId("resource"))).IsOK());
    Specs_.clear();
    Reconfigure();
    EXPECT_THROW(Manager_->Get(TResourceId("resource")), std::exception);
    EXPECT_FALSE(WaitFor(Manager_->Load(TResourceId("resource"))).IsOK());
}

TEST_F(TResourceInstanceManagerTest, EagerAssignmentsAndEmptyLayout)
{
    auto id = Add();
    Specs_.at(TResourceId("resource"))->AlwaysOn = true;
    Reconfigure();
    auto resource = Get(id);
    WaitFor(resource->Started.ToFuture()).ThrowOnError();
    resource->Completion.Set();
    WaitFor(Manager_->Load(THashSet<TResourceInstanceId>{})).ThrowOnError();
    Instances_.clear();
    Reconfigure();
    resource.Reset();
    EXPECT_TRUE(Manager_->CollectResourceInstanceStatuses().empty());
    Reconfigure();
    EXPECT_TRUE(Manager_->CollectResourceInstanceStatuses().empty());
}

TEST_F(TResourceInstanceManagerTest, RemovalCancelsRunningLoad)
{
    auto id = Add();
    Reconfigure();
    auto resource = Get(id);
    auto loaded = Manager_->Load({id});
    WaitFor(resource->Started.ToFuture()).ThrowOnError();
    FlushInvoker();

    Instances_.clear();
    Reconfigure();
    EXPECT_FALSE(WaitFor(loaded.WithTimeout(TDuration::Seconds(3))).IsOK());
    EXPECT_TRUE(loaded.IsSet());
    EXPECT_TRUE(resource->Completion.IsCanceled());
}

TEST_F(TResourceInstanceManagerTest, RemovalCancelsPendingLoadWithoutCancelingDependency)
{
    auto dependencyId = Add(TResourceId("dependency"));
    auto id = Add();
    Specs_.at(TResourceId("resource"))->Dependencies.emplace(TResourceId("dependency"), New<TResourceDescription>());
    Reconfigure();
    auto dependency = Get(dependencyId);
    auto resource = Get(id);
    auto dependencyLoad = Manager_->Load({dependencyId});
    auto loaded = Manager_->Load({id});
    WaitFor(dependency->Started.ToFuture()).ThrowOnError();

    Instances_.erase(id);
    Reconfigure();
    EXPECT_FALSE(WaitFor(loaded.WithTimeout(TDuration::Seconds(3))).IsOK());
    EXPECT_TRUE(loaded.IsSet());
    EXPECT_FALSE(dependency->Completion.IsCanceled());
    EXPECT_FALSE(resource->Started.IsSet());

    dependency->Completion.Set();
    WaitFor(dependencyLoad).ThrowOnError();
    EXPECT_FALSE(WaitFor(loaded).IsOK());
}

TEST_F(TResourceInstanceManagerTest, CancelingCallerDoesNotCancelSharedLoad)
{
    auto id = Add();
    Reconfigure();
    auto resource = Get(id);
    auto first = Manager_->Load({id});
    auto second = Manager_->Load({id});
    WaitFor(resource->Started.ToFuture()).ThrowOnError();
    first.Cancel(TError("Job stopped"));
    EXPECT_FALSE(resource->Completion.IsCanceled());
    resource->Completion.Set();
    WaitFor(second).ThrowOnError();
}

TEST_F(TResourceInstanceManagerTest, ReplacementWaitsForCancellationAcknowledgement)
{
    auto id = Add();
    Reconfigure();
    auto resource = Get(id);
    auto completion = resource->Completion;
    auto cancellationRequested = NewPromise<void>();
    completion.OnCanceled(BIND([manager = MakeWeak(Manager_), cancellationRequested] (const TError&) {
        // Cancellation callbacks must run outside the manager lock.
        auto strongManager = manager.Lock();
        EXPECT_EQ(strongManager->CollectResourceInstanceStatuses().size(), 1u);
        cancellationRequested.TrySet();
    }));
    auto loaded = Manager_->Load({id});
    WaitFor(resource->Started.ToFuture()).ThrowOnError();
    FlushInvoker();
    auto weakResource = MakeWeak(resource);
    Instances_.clear();
    auto replacementId = Add();
    Reconfigure();
    auto replacement = Get(replacementId);
    auto replacementLoad = Manager_->Load({replacementId});
    resource.Reset();
    FlushInvoker();
    WaitFor(cancellationRequested.ToFuture().WithTimeout(TDuration::Seconds(3))).ThrowOnError();
    EXPECT_FALSE(weakResource.IsExpired());
    EXPECT_FALSE(replacement->Started.IsSet());

    completion.Set(TError(NYT::EErrorCode::Canceled, "Load canceled"));
    EXPECT_FALSE(WaitFor(loaded).IsOK());
    WaitFor(replacement->Started.ToFuture().WithTimeout(TDuration::Seconds(3))).ThrowOnError();
    EXPECT_TRUE(weakResource.IsExpired());
    replacement->Completion.Set();
    WaitFor(replacementLoad).ThrowOnError();
}

TEST_F(TResourceInstanceManagerTest, ManagerDestructionCancelsRunningLoad)
{
    auto id = Add();
    Reconfigure();
    auto resource = Get(id);
    auto loaded = Manager_->Load({id});
    WaitFor(resource->Started.ToFuture()).ThrowOnError();
    FlushInvoker();
    Manager_.Reset();
    EXPECT_TRUE(resource->Completion.IsCanceled());
    resource->Completion.TrySet(TError("Test cleanup"));
    EXPECT_FALSE(WaitFor(loaded).IsOK());
}

TEST_F(TResourceInstanceManagerTest, RemovalCancelsFiberLoad)
{
    auto id = Add();
    Specs_.at(TResourceId("resource"))->ResourceClassName = TypeName<TFiberInstanceResource>();
    Reconfigure();
    auto resource = Get(id);
    auto completion = resource->Completion;
    auto loaded = Manager_->Load({id});
    WaitFor(resource->Started.ToFuture()).ThrowOnError();
    auto destroyed = resource->GetDestroyedFuture();
    resource.Reset();

    Instances_.clear();
    Reconfigure();
    EXPECT_FALSE(WaitFor(loaded.WithTimeout(TDuration::Seconds(3))).IsOK());
    EXPECT_TRUE(loaded.IsSet());
    EXPECT_TRUE(completion.IsCanceled());
    completion.TrySet(TError("Test cleanup"));
    WaitFor(destroyed.WithTimeout(TDuration::Seconds(3))).ThrowOnError();
}

TEST_F(TResourceInstanceManagerTest, RemovalCancelsWaitForRetainedPredecessor)
{
    auto oldId = Add();
    Reconfigure();
    auto oldResource = Get(oldId);
    Instances_.clear();
    auto id = Add();
    Reconfigure();
    auto resource = Get(id);
    auto loaded = Manager_->Load({id});
    Instances_.clear();
    Reconfigure();
    EXPECT_FALSE(WaitFor(loaded.WithTimeout(TDuration::Seconds(3))).IsOK());
    EXPECT_TRUE(loaded.IsSet());
    EXPECT_FALSE(resource->Started.IsSet());
    oldResource.Reset();
    EXPECT_FALSE(WaitFor(loaded).IsOK());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
