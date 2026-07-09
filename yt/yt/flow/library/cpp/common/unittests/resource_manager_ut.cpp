#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/resource.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/common/resource_manager.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/actions/future.h>

#include <util/system/type_name.h>

namespace NYT::NFlow::NWorker {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(NLogging::TLogger, Logger, "TestLogger");

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSharedVector);

class TSharedVector
    : public IResource
{
public:
    TSharedVector(TResourceContextPtr context, TDynamicResourceContextPtr /*dynamicContext*/)
        : Spec_(context->ResourceSpec)
    { }

    TFuture<void> Load(const THashMap<TResourceId, IResourcePtr>& /*dependencies*/) override
    {
        return OKFuture;
    }

    void Reconfigure(const TDynamicResourceContextPtr& /*dynamicContext*/) override
    { }

    void AddItem(std::string item)
    {
        Items_.push_back(std::move(item));
    }

    std::vector<std::string> GetItems() const
    {
        return Items_;
    }

    TYsonStructPtr GetParametersBase() const final
    {
        return nullptr;
    }

    TYsonStructPtr GetDynamicParametersBase() const final
    {
        return nullptr;
    }

private:
    const TResourceSpecPtr Spec_;

    std::vector<std::string> Items_;
};

DEFINE_REFCOUNTED_TYPE(TSharedVector);

YT_FLOW_DEFINE_RESOURCE(TSharedVector);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTestResource);

class TTestResource
    : public IResource
{
public:
    TTestResource(TResourceContextPtr context, TDynamicResourceContextPtr /*dynamicContext*/)
        : Spec_(context->ResourceSpec)
        , Name_(Spec_->Parameters->GetChildValueOrThrow<std::string>("name"))
    { }

    TFuture<void> Load(const THashMap<TResourceId, IResourcePtr>& dependencies) override
    {
        dependencies.at("base")->As<TSharedVector>()->AddItem(Name_);
        return OKFuture;
    }

    void Reconfigure(const TDynamicResourceContextPtr& /*dynamicContext*/) override
    { }

    size_t GetDependenciesCount() const
    {
        return Spec_->Dependencies.size();
    }

    TYsonStructPtr GetParametersBase() const final
    {
        return nullptr;
    }

    TYsonStructPtr GetDynamicParametersBase() const final
    {
        return nullptr;
    }

private:
    const TResourceSpecPtr Spec_;
    std::string Name_;
};

DEFINE_REFCOUNTED_TYPE(TTestResource);

YT_FLOW_DEFINE_RESOURCE(TTestResource);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TReconfigurableResource);

class TReconfigurableResource
    : public IResource
{
public:
    TReconfigurableResource(TResourceContextPtr context, TDynamicResourceContextPtr /*dynamicContext*/)
        : Spec_(context->ResourceSpec)
    { }

    TFuture<void> Load(const THashMap<TResourceId, IResourcePtr>& /*dependencies*/) override
    {
        return OKFuture;
    }

    void Reconfigure(const TDynamicResourceContextPtr& /*dynamicContext*/) override
    {
        ++ReconfigureCount_;
    }

    int GetReconfigureCount() const
    {
        return ReconfigureCount_;
    }

    TYsonStructPtr GetParametersBase() const final
    {
        return nullptr;
    }

    TYsonStructPtr GetDynamicParametersBase() const final
    {
        return nullptr;
    }

private:
    const TResourceSpecPtr Spec_;
    int ReconfigureCount_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TReconfigurableResource);

YT_FLOW_DEFINE_RESOURCE(TReconfigurableResource);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSlowResource);

// A resource whose Load() completes only when the test explicitly resolves the promise.
class TSlowResource
    : public IResource
{
public:
    TSlowResource(TResourceContextPtr context, TDynamicResourceContextPtr /*dynamicContext*/)
        : ResourceId_(context->ResourceId)
    {
        LastResource_.store(this);
        // Register by resource id so a test can address several slow resources at once.
        auto guard = Guard(InstancesLock_);
        Instances_[ResourceId_] = this;
    }

    ~TSlowResource() override
    {
        // Use CAS to avoid a TOCTOU race: only clear LastResource_ if it still
        // points to this instance.
        TSlowResource* expected = this;
        LastResource_.compare_exchange_strong(expected, nullptr);
        auto guard = Guard(InstancesLock_);
        if (auto it = Instances_.find(ResourceId_); it != Instances_.end() && it->second == this) {
            Instances_.erase(it);
        }
    }

    TFuture<void> Load(const THashMap<TResourceId, IResourcePtr>& /*dependencies*/) override
    {
        LoadStarted_ = true;
        return LoadPromise_.ToFuture();
    }

    void Reconfigure(const TDynamicResourceContextPtr& /*dynamicContext*/) override
    { }

    TYsonStructPtr GetParametersBase() const final
    {
        return nullptr;
    }

    TYsonStructPtr GetDynamicParametersBase() const final
    {
        return nullptr;
    }

    void CompleteLoad()
    {
        LoadPromise_.Set();
    }

    void FailLoad(const TError& error)
    {
        LoadPromise_.Set(error);
    }

    bool IsLoadStarted() const
    {
        return LoadStarted_;
    }

    static TSlowResourcePtr GetLastResource()
    {
        return MakeStrong(LastResource_.load());
    }

    // Looks up a slow resource by its resource id. Lets a test drive several slow
    // resources independently (GetLastResource only tracks the most recently created one).
    static TSlowResourcePtr GetById(const TResourceId& resourceId)
    {
        auto guard = Guard(InstancesLock_);
        auto it = Instances_.find(resourceId);
        return it != Instances_.end() ? MakeStrong(it->second) : nullptr;
    }

private:
    const TResourceId ResourceId_;
    TPromise<void> LoadPromise_ = NewPromise<void>();
    bool LoadStarted_ = false;
    static std::atomic<TSlowResource*> LastResource_;
    static NThreading::TSpinLock InstancesLock_;
    static THashMap<TResourceId, TSlowResource*> Instances_;
};

std::atomic<TSlowResource*> TSlowResource::LastResource_;
NThreading::TSpinLock TSlowResource::InstancesLock_;
THashMap<TResourceId, TSlowResource*> TSlowResource::Instances_;

DEFINE_REFCOUNTED_TYPE(TSlowResource);

YT_FLOW_DEFINE_RESOURCE(TSlowResource);

////////////////////////////////////////////////////////////////////////////////

class TResourceManagerTest
    : public ::testing::Test
{
public:
    TResourceSpecPtr BuildTestResourceSpec(TResourceId name, std::vector<TResourceId> dependencies)
    {
        auto spec = New<TResourceSpec>();
        spec->ResourceClassName = TypeName<TTestResource>();
        spec->Parameters->AddChild("name", ConvertToNode(TYsonStringBuf(name.Underlying())));

        THashMap<TResourceId, TResourceDescriptionPtr> fullDependencies;
        for (const auto& resourceId : dependencies) {
            fullDependencies[resourceId] = New<TResourceDescription>();
        }
        fullDependencies["base_global"] = New<TResourceDescription>();
        fullDependencies["base_global"]->Alias = "base";
        spec->Dependencies = std::move(fullDependencies);
        return spec;
    }

    bool IsBefore(const std::string& first, const std::string& second, const std::vector<std::string>& items)
    {
        auto firstPos = std::find(items.begin(), items.end(), first) - items.begin();
        auto secondPos = std::find(items.begin(), items.end(), second) - items.begin();

        return firstPos < secondPos;
    }

    TResourceSpecPtr BuildReconfigurableResourceSpec()
    {
        auto spec = New<TResourceSpec>();
        spec->ResourceClassName = TypeName<TReconfigurableResource>();
        return spec;
    }

    TDynamicResourceSpecPtr BuildDynamicResourceSpec(const TString& paramValue)
    {
        auto dynamicSpec = New<TDynamicResourceSpec>();
        dynamicSpec->Parameters->AddChild("value", ConvertToNode(TYsonStringBuf(paramValue)));
        return dynamicSpec;
    }

    // A slow resource (retrievable via TSlowResource::GetById by its resource id) with optional
    // dependencies and an optional always-on flag.
    TResourceSpecPtr BuildSlowResourceSpec(
        std::vector<TResourceId> dependencies = {},
        bool alwaysOn = false)
    {
        auto spec = New<TResourceSpec>();
        spec->ResourceClassName = TypeName<TSlowResource>();
        spec->AlwaysOn = alwaysOn;

        THashMap<TResourceId, TResourceDescriptionPtr> fullDependencies;
        for (const auto& resourceId : dependencies) {
            fullDependencies[resourceId] = New<TResourceDescription>();
        }
        spec->Dependencies = std::move(fullDependencies);
        return spec;
    }

    TResourceSpecPtr BuildPreloadRequiredSlowResourceSpec()
    {
        auto spec = BuildSlowResourceSpec();
        spec->PreloadRequired = true;
        return spec;
    }

    IResourceManagerPtr CreateManager(
        const THashMap<TResourceId, TResourceSpecPtr>& resources,
        IInvokerPtr invoker = nullptr)
    {
        auto context = New<TResourceManagerContext>();
        context->Invoker = invoker ? invoker : GetCurrentInvoker();
        context->Logger = Logger();
        context->StatusProfiler = CreateStatusProfiler();
        return CreateResourceManager(std::move(context), resources, {});
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TResourceManagerTest, BasicBamboo)
{

    THashMap<TResourceId, TResourceSpecPtr> resources;
    resources["base_global"] = New<TResourceSpec>();
    resources["base_global"]->ResourceClassName = TypeName<TSharedVector>();
    resources["first"] = BuildTestResourceSpec("first", {});
    resources["second"] = BuildTestResourceSpec("second", {"first"});
    resources["third"] = BuildTestResourceSpec("third", {"second"});

    auto context = New<TResourceManagerContext>();
    context->Invoker = GetCurrentInvoker();
    context->Logger = Logger();
    context->StatusProfiler = CreateStatusProfiler();
    auto resourceManager = CreateResourceManager(std::move(context), resources, {});

    WaitFor(resourceManager->Load("base_global")).ThrowOnError();

    const auto base = resourceManager->Get("base_global")->As<TSharedVector>();

    WaitFor(resourceManager->Load("first")).ThrowOnError();
    ASSERT_EQ(base->GetItems(), std::vector<std::string>({"first"}));

    WaitFor(resourceManager->Load("second")).ThrowOnError();
    ASSERT_EQ(base->GetItems(), std::vector<std::string>({"first", "second"}));

    WaitFor(resourceManager->Load("third")).ThrowOnError();
    ASSERT_EQ(base->GetItems(), std::vector<std::string>({"first", "second", "third"}));
}

TEST_F(TResourceManagerTest, MultipleDependencies)
{
    THashMap<TResourceId, TResourceSpecPtr> resources;
    resources["base_global"] = New<TResourceSpec>();
    resources["base_global"]->ResourceClassName = TypeName<TSharedVector>();
    resources["first"] = BuildTestResourceSpec("first", {});
    resources["second"] = BuildTestResourceSpec("second", {});
    resources["third"] = BuildTestResourceSpec("third", {"first", "second"});
    resources["fourth"] = BuildTestResourceSpec("third", {"third"});

    auto context = New<TResourceManagerContext>();
    context->Invoker = GetCurrentInvoker();
    context->Logger = Logger();
    context->StatusProfiler = CreateStatusProfiler();
    auto resourceManager = CreateResourceManager(std::move(context), resources, {});

    WaitFor(resourceManager->Load("fourth")).ThrowOnError();
    WaitFor(resourceManager->Load("fourth")).ThrowOnError();

    auto items = resourceManager->Get("base_global")->As<TSharedVector>()->GetItems();
    EXPECT_TRUE(IsBefore("first", "third", items));
    EXPECT_TRUE(IsBefore("second", "third", items));
    EXPECT_TRUE(IsBefore("third", "fourth", items));
}

TEST_F(TResourceManagerTest, AsThrows)
{
    auto context = New<TResourceContext>();
    context->ResourceSpec = New<TResourceSpec>();
    auto dynamicContext = New<TDynamicResourceContext>();
    auto resource = New<TSharedVector>(std::move(context), std::move(dynamicContext));

    EXPECT_THROW(
        resource->As<TTestResource>(),
        NYT::TErrorException);
}

TEST_F(TResourceManagerTest, ReconfigureBasic)
{
    THashMap<TResourceId, TResourceSpecPtr> resources;
    resources["res"] = BuildReconfigurableResourceSpec();

    auto context = New<TResourceManagerContext>();
    context->Invoker = GetCurrentInvoker();
    context->Logger = Logger();
    context->StatusProfiler = CreateStatusProfiler();
    auto resourceManager = CreateResourceManager(std::move(context), resources, {});

    WaitFor(resourceManager->Load("res")).ThrowOnError();

    auto resource = resourceManager->Get("res")->As<TReconfigurableResource>();
    ASSERT_EQ(resource->GetReconfigureCount(), 0);

    // Reconfigure with a new dynamic spec.
    THashMap<TResourceId, TDynamicResourceSpecPtr> newDynamicSpecs;
    newDynamicSpecs["res"] = BuildDynamicResourceSpec("value1");

    resourceManager->Reconfigure(newDynamicSpecs);

    ASSERT_EQ(resource->GetReconfigureCount(), 1);
}

TEST_F(TResourceManagerTest, ReconfigureSkipsUnchangedSpec)
{
    THashMap<TResourceId, TResourceSpecPtr> resources;
    resources["res"] = BuildReconfigurableResourceSpec();

    auto context = New<TResourceManagerContext>();
    context->Invoker = GetCurrentInvoker();
    context->Logger = Logger();
    context->StatusProfiler = CreateStatusProfiler();
    auto resourceManager = CreateResourceManager(std::move(context), resources, {});

    WaitFor(resourceManager->Load("res")).ThrowOnError();

    auto resource = resourceManager->Get("res")->As<TReconfigurableResource>();
    ASSERT_EQ(resource->GetReconfigureCount(), 0);

    // Initial reconfigure.
    THashMap<TResourceId, TDynamicResourceSpecPtr> dynamicSpecs;
    dynamicSpecs["res"] = BuildDynamicResourceSpec("value1");

    resourceManager->Reconfigure(dynamicSpecs);

    ASSERT_EQ(resource->GetReconfigureCount(), 1);

    // Reconfigure with the same dynamic spec parameters — should be skipped.
    THashMap<TResourceId, TDynamicResourceSpecPtr> sameDynamicSpecs;
    sameDynamicSpecs["res"] = BuildDynamicResourceSpec("value1");

    resourceManager->Reconfigure(sameDynamicSpecs);

    ASSERT_EQ(resource->GetReconfigureCount(), 1);
}

TEST_F(TResourceManagerTest, ReconfigureSkipsUnknownResource)
{
    THashMap<TResourceId, TResourceSpecPtr> resources;
    resources["res"] = BuildReconfigurableResourceSpec();

    auto context = New<TResourceManagerContext>();
    context->Invoker = GetCurrentInvoker();
    context->Logger = Logger();
    context->StatusProfiler = CreateStatusProfiler();
    auto resourceManager = CreateResourceManager(std::move(context), resources, {});

    WaitFor(resourceManager->Load("res")).ThrowOnError();

    auto resource = resourceManager->Get("res")->As<TReconfigurableResource>();

    // Reconfigure with a dynamic spec for a non-existent resource — should be silently skipped.
    THashMap<TResourceId, TDynamicResourceSpecPtr> newDynamicSpecs;
    newDynamicSpecs["unknown_resource"] = BuildDynamicResourceSpec("value1");

    resourceManager->Reconfigure(newDynamicSpecs);

    ASSERT_EQ(resource->GetReconfigureCount(), 0);
}

TEST_F(TResourceManagerTest, ReconfigureMultipleResources)
{
    THashMap<TResourceId, TResourceSpecPtr> resources;
    resources["res1"] = BuildReconfigurableResourceSpec();
    resources["res2"] = BuildReconfigurableResourceSpec();

    auto context = New<TResourceManagerContext>();
    context->Invoker = GetCurrentInvoker();
    context->Logger = Logger();
    context->StatusProfiler = CreateStatusProfiler();
    auto resourceManager = CreateResourceManager(std::move(context), resources, {});

    WaitFor(resourceManager->Load("res1")).ThrowOnError();
    WaitFor(resourceManager->Load("res2")).ThrowOnError();

    auto resource1 = resourceManager->Get("res1")->As<TReconfigurableResource>();
    auto resource2 = resourceManager->Get("res2")->As<TReconfigurableResource>();

    // Reconfigure both resources at once.
    THashMap<TResourceId, TDynamicResourceSpecPtr> newDynamicSpecs;
    newDynamicSpecs["res1"] = BuildDynamicResourceSpec("a");
    newDynamicSpecs["res2"] = BuildDynamicResourceSpec("b");

    resourceManager->Reconfigure(newDynamicSpecs);

    ASSERT_EQ(resource1->GetReconfigureCount(), 1);
    ASSERT_EQ(resource2->GetReconfigureCount(), 1);
}

TEST_F(TResourceManagerTest, ReconfigureTriggersOnChangedSpec)
{
    THashMap<TResourceId, TResourceSpecPtr> resources;
    resources["res"] = BuildReconfigurableResourceSpec();

    auto initialDynamicSpec = BuildDynamicResourceSpec("initial");

    auto context = New<TResourceManagerContext>();
    context->Invoker = GetCurrentInvoker();
    context->Logger = Logger();
    context->StatusProfiler = CreateStatusProfiler();
    auto resourceManager = CreateResourceManager(std::move(context), resources, {});

    WaitFor(resourceManager->Load("res")).ThrowOnError();

    auto resource = resourceManager->Get("res")->As<TReconfigurableResource>();
    ASSERT_EQ(resource->GetReconfigureCount(), 0);

    // Reconfigure with a different value — should trigger reconfiguration.
    THashMap<TResourceId, TDynamicResourceSpecPtr> newDynamicSpecs;
    newDynamicSpecs["res"] = BuildDynamicResourceSpec("updated");

    resourceManager->Reconfigure(newDynamicSpecs);

    ASSERT_EQ(resource->GetReconfigureCount(), 1);

    // Reconfigure again with the same updated value — should be skipped.
    THashMap<TResourceId, TDynamicResourceSpecPtr> sameDynamicSpecs;
    sameDynamicSpecs["res"] = BuildDynamicResourceSpec("updated");

    resourceManager->Reconfigure(sameDynamicSpecs);

    ASSERT_EQ(resource->GetReconfigureCount(), 1);

    // Reconfigure with yet another value — should trigger again.
    THashMap<TResourceId, TDynamicResourceSpecPtr> anotherDynamicSpecs;
    anotherDynamicSpecs["res"] = BuildDynamicResourceSpec("updated_again");

    resourceManager->Reconfigure(anotherDynamicSpecs);

    ASSERT_EQ(resource->GetReconfigureCount(), 2);
}

TEST_F(TResourceManagerTest, ReconfigurePartialUpdate)
{
    THashMap<TResourceId, TResourceSpecPtr> resources;
    resources["res1"] = BuildReconfigurableResourceSpec();
    resources["res2"] = BuildReconfigurableResourceSpec();

    auto initialDynamicSpec = BuildDynamicResourceSpec("initial");

    auto context = New<TResourceManagerContext>();
    context->Invoker = GetCurrentInvoker();
    context->Logger = Logger();
    context->StatusProfiler = CreateStatusProfiler();
    auto resourceManager = CreateResourceManager(std::move(context), resources, {});

    WaitFor(resourceManager->Load("res1")).ThrowOnError();
    WaitFor(resourceManager->Load("res2")).ThrowOnError();

    auto resource1 = resourceManager->Get("res1")->As<TReconfigurableResource>();
    auto resource2 = resourceManager->Get("res2")->As<TReconfigurableResource>();

    // Reconfigure only res1 with the same spec — should be skipped.
    // Reconfigure only res2 with a new spec — should trigger.
    THashMap<TResourceId, TDynamicResourceSpecPtr> newDynamicSpecs;
    newDynamicSpecs["res1"] = BuildDynamicResourceSpec("initial");
    newDynamicSpecs["res2"] = BuildDynamicResourceSpec("initial");

    resourceManager->Reconfigure(newDynamicSpecs);

    ASSERT_EQ(resource1->GetReconfigureCount(), 1);
    ASSERT_EQ(resource2->GetReconfigureCount(), 1);

    newDynamicSpecs["res1"] = BuildDynamicResourceSpec("initial");
    newDynamicSpecs["res2"] = BuildDynamicResourceSpec("new_value");

    resourceManager->Reconfigure(newDynamicSpecs);

    ASSERT_EQ(resource1->GetReconfigureCount(), 1);
    ASSERT_EQ(resource2->GetReconfigureCount(), 2);
}

TEST_F(TResourceManagerTest, ReconfigureWithFailedResource)
{
    // Use a non-existent resource class name to trigger TFailedResource creation.
    auto failedSpec = New<TResourceSpec>();
    failedSpec->ResourceClassName = "NonExistentResourceClass";

    THashMap<TResourceId, TResourceSpecPtr> resources;
    resources["failed_res"] = failedSpec;

    auto context = New<TResourceManagerContext>();
    context->Invoker = GetCurrentInvoker();
    context->Logger = Logger();
    context->StatusProfiler = CreateStatusProfiler();
    auto resourceManager = CreateResourceManager(std::move(context), resources, {});

    // Reconfigure with a dynamic spec for the failed resource — should not throw.
    THashMap<TResourceId, TDynamicResourceSpecPtr> newDynamicSpecs;
    newDynamicSpecs["failed_res"] = BuildDynamicResourceSpec("value1");

    EXPECT_NO_THROW(resourceManager->Reconfigure(newDynamicSpecs));
}

TEST_F(TResourceManagerTest, ReconfigureWithFailedAndReconfigurableResources)
{
    // Use a non-existent resource class name to trigger TFailedResource creation.
    auto failedSpec = New<TResourceSpec>();
    failedSpec->ResourceClassName = "NonExistentResourceClass";

    THashMap<TResourceId, TResourceSpecPtr> resources;
    resources["failed_res"] = failedSpec;
    resources["good_res"] = BuildReconfigurableResourceSpec();

    auto context = New<TResourceManagerContext>();
    context->Invoker = GetCurrentInvoker();
    context->Logger = Logger();
    context->StatusProfiler = CreateStatusProfiler();
    auto resourceManager = CreateResourceManager(std::move(context), resources, {});

    WaitFor(resourceManager->Load("good_res")).ThrowOnError();

    auto goodResource = resourceManager->Get("good_res")->As<TReconfigurableResource>();
    ASSERT_EQ(goodResource->GetReconfigureCount(), 0);

    // Reconfigure both resources — should not throw.
    THashMap<TResourceId, TDynamicResourceSpecPtr> newDynamicSpecs;
    newDynamicSpecs["failed_res"] = BuildDynamicResourceSpec("value1");
    newDynamicSpecs["good_res"] = BuildDynamicResourceSpec("value2");

    EXPECT_NO_THROW(resourceManager->Reconfigure(newDynamicSpecs));

    // Verify that the reconfigurable resource was successfully reconfigured.
    ASSERT_EQ(goodResource->GetReconfigureCount(), 1);
}

////////////////////////////////////////////////////////////////////////////////

// Posts a no-op closure to |invoker| and waits for it to complete.
// Because TActionQueue is a FIFO queue, this guarantees that all previously
// enqueued callbacks (e.g. Subscribe handlers) have already run by the time
// DrainInvoker returns.
void DrainInvoker(const IInvokerPtr& invoker)
{
    auto noop = BIND([] {
    });
    WaitFor(noop.AsyncVia(invoker).Run())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TResourceManagerTest, PreloadInitiallyEmpty)
{
    THashMap<TResourceId, TResourceSpecPtr> resources;
    resources["res"] = BuildPreloadRequiredSlowResourceSpec();

    auto actionQueue = New<TActionQueue>();
    auto resourceManager = CreateManager(resources, actionQueue->GetInvoker());

    // Before any UpdatePreloadedResources call, GetPreloadedStates should be empty.
    EXPECT_TRUE(resourceManager->GetPreloadedStates().empty());
}

TEST_F(TResourceManagerTest, PreloadAddResource)
{
    THashMap<TResourceId, TResourceSpecPtr> resources;
    resources["res"] = BuildPreloadRequiredSlowResourceSpec();

    auto actionQueue = New<TActionQueue>();
    auto resourceManager = CreateManager(resources, actionQueue->GetInvoker());

    // Add "res" to the preload set — state should become Preloading.
    resourceManager->UpdatePreloadedResources({"res"});

    auto states = resourceManager->GetPreloadedStates();
    ASSERT_EQ(states.size(), 1u);
    EXPECT_EQ(states.at("res"), EPreloadedResourceState::Preloading);
}

TEST_F(TResourceManagerTest, PreloadResourceBecomesPreloaded)
{
    THashMap<TResourceId, TResourceSpecPtr> resources;
    resources["res"] = BuildPreloadRequiredSlowResourceSpec();

    auto actionQueue = New<TActionQueue>();
    auto resourceManager = CreateManager(resources, actionQueue->GetInvoker());

    resourceManager->UpdatePreloadedResources({"res"});

    // Capture the resource after starting preload via GetLastResource (avoids calling Get on a
    // preload-required resource that is not yet loaded).
    auto slowResource = TSlowResource::GetLastResource();

    // State should be Preloading while the load is in progress.
    {
        auto states = resourceManager->GetPreloadedStates();
        ASSERT_EQ(states.count("res"), 1u);
        EXPECT_EQ(states.at("res"), EPreloadedResourceState::Preloading);
    }

    // Get the load future (same future that UpdatePreloadedResources subscribed to).
    auto loadFuture = resourceManager->Load("res");

    // Complete the load — this resolves the promise inside TSlowResource.
    slowResource->CompleteLoad();

    // Wait for the load future to complete (runs the Apply callback on the invoker).
    WaitFor(loadFuture).ThrowOnError();

    // Drain the invoker once more so the Subscribe callback (which updates State) runs.
    DrainInvoker(actionQueue->GetInvoker());

    // State should now be Preloaded.
    auto states = resourceManager->GetPreloadedStates();
    ASSERT_EQ(states.count("res"), 1u);
    EXPECT_EQ(states.at("res"), EPreloadedResourceState::Preloaded);
}

TEST_F(TResourceManagerTest, PreloadFailedResourceDroppedFromStatus)
{
    THashMap<TResourceId, TResourceSpecPtr> resources;
    resources["res"] = BuildPreloadRequiredSlowResourceSpec();

    auto actionQueue = New<TActionQueue>();
    auto resourceManager = CreateManager(resources, actionQueue->GetInvoker());

    resourceManager->UpdatePreloadedResources({"res"});

    // Capture the resource after starting preload via GetLastResource (avoids calling Get on a
    // preload-required resource that is not yet loaded).
    auto slowResource = TSlowResource::GetLastResource();

    // Get the load future.
    auto loadFuture = resourceManager->Load("res");

    // Fail the load.
    slowResource->FailLoad(TError("load failed"));

    // Wait for the load future to settle (it will carry the error).
    WaitUntilSet(loadFuture);

    // Drain the invoker so the Subscribe callback runs.
    DrainInvoker(actionQueue->GetInvoker());

    // On failure the entry should be dropped from PreloadStatus to allow retry.
    auto states = resourceManager->GetPreloadedStates();
    EXPECT_EQ(states.count("res"), 0u);
}

TEST_F(TResourceManagerTest, PreloadRemoveWhileLoading)
{
    THashMap<TResourceId, TResourceSpecPtr> resources;
    resources["res"] = BuildPreloadRequiredSlowResourceSpec();

    auto actionQueue = New<TActionQueue>();
    auto resourceManager = CreateManager(resources, actionQueue->GetInvoker());

    resourceManager->UpdatePreloadedResources({"res"});

    // Capture the resource object after starting preload via GetLastResource.
    // It will be replaced when the resource is removed from the preload set.
    auto slowResource = TSlowResource::GetLastResource();

    // Remove "res" from the preload set while it is still loading.
    // This recreates the resource object, so Get("res") now returns a different instance.
    resourceManager->UpdatePreloadedResources({});

    // The entry should be gone immediately.
    EXPECT_TRUE(resourceManager->GetPreloadedStates().empty());

    // Complete the load on the original (now-orphaned) resource.
    // The Subscribe callback should be a no-op because Cancelled is set.
    slowResource->CompleteLoad();

    // Drain the invoker so the Subscribe callback runs.
    DrainInvoker(actionQueue->GetInvoker());

    // Still empty — the cancelled callback must not re-insert the entry.
    EXPECT_TRUE(resourceManager->GetPreloadedStates().empty());
}

TEST_F(TResourceManagerTest, PreloadRemoveRecreatesResource)
{
    THashMap<TResourceId, TResourceSpecPtr> resources;
    resources["res"] = BuildPreloadRequiredSlowResourceSpec();

    auto actionQueue = New<TActionQueue>();
    auto resourceManager = CreateManager(resources, actionQueue->GetInvoker());

    resourceManager->UpdatePreloadedResources({"res"});
    // Capture the original resource via GetLastResource after preload started.
    auto originalResource = TSlowResource::GetLastResource();

    // Remove — should recreate the resource.
    resourceManager->UpdatePreloadedResources({});

    // After removal the resource is recreated; GetLastResource now returns the new instance.
    auto newResource = TSlowResource::GetLastResource();

    // The resource object must have been replaced.
    EXPECT_NE(originalResource.Get(), newResource.Get());
}

TEST_F(TResourceManagerTest, PreloadIdempotentUpdate)
{
    THashMap<TResourceId, TResourceSpecPtr> resources;
    resources["res"] = BuildPreloadRequiredSlowResourceSpec();

    auto actionQueue = New<TActionQueue>();
    auto resourceManager = CreateManager(resources, actionQueue->GetInvoker());

    resourceManager->UpdatePreloadedResources({"res"});
    // Calling again with the same set should not create a second entry or restart loading.
    resourceManager->UpdatePreloadedResources({"res"});

    auto states = resourceManager->GetPreloadedStates();
    ASSERT_EQ(states.size(), 1u);
    EXPECT_EQ(states.at("res"), EPreloadedResourceState::Preloading);
}

TEST_F(TResourceManagerTest, PreloadMultipleResources)
{
    THashMap<TResourceId, TResourceSpecPtr> resources;
    resources["res1"] = BuildPreloadRequiredSlowResourceSpec();
    resources["res2"] = BuildPreloadRequiredSlowResourceSpec();

    auto actionQueue = New<TActionQueue>();
    auto resourceManager = CreateManager(resources, actionQueue->GetInvoker());

    resourceManager->UpdatePreloadedResources({"res1", "res2"});

    auto states = resourceManager->GetPreloadedStates();
    ASSERT_EQ(states.size(), 2u);
    EXPECT_EQ(states.at("res1"), EPreloadedResourceState::Preloading);
    EXPECT_EQ(states.at("res2"), EPreloadedResourceState::Preloading);

    // Complete res2 and wait for its load future.
    auto res2 = TSlowResource::GetLastResource();
    auto res2LoadFuture = resourceManager->Load("res2");
    res2->CompleteLoad();
    WaitFor(res2LoadFuture).ThrowOnError();

    // Drain the invoker so the Subscribe callback for res1 runs.
    DrainInvoker(actionQueue->GetInvoker());

    states = resourceManager->GetPreloadedStates();
    EXPECT_EQ(states.at("res1"), EPreloadedResourceState::Preloading);
    EXPECT_EQ(states.at("res2"), EPreloadedResourceState::Preloaded);
}

////////////////////////////////////////////////////////////////////////////////

// Tests that Get() and Load() throw for preload-required resources that are not yet loaded.

TEST_F(TResourceManagerTest, GetThrowsForPreloadRequiredNotStarted)
{
    // Resource has PreloadRequired=true but UpdatePreloadedResources was never called.
    THashMap<TResourceId, TResourceSpecPtr> resources;
    resources["res"] = BuildPreloadRequiredSlowResourceSpec();

    auto actionQueue = New<TActionQueue>();
    auto resourceManager = CreateManager(resources, actionQueue->GetInvoker());

    EXPECT_THROW(resourceManager->Get("res"), NYT::TErrorException);
}

TEST_F(TResourceManagerTest, GetThrowsForPreloadRequiredNotLoaded)
{
    // Resource has PreloadRequired=true and UpdatePreloadedResources was called,
    // but the load has not completed yet (still Preloading).
    THashMap<TResourceId, TResourceSpecPtr> resources;
    resources["res"] = BuildPreloadRequiredSlowResourceSpec();

    auto actionQueue = New<TActionQueue>();
    auto resourceManager = CreateManager(resources, actionQueue->GetInvoker());

    resourceManager->UpdatePreloadedResources({"res"});

    // State is Preloading — Get must throw.
    EXPECT_THROW(resourceManager->Get("res"), NYT::TErrorException);
}

TEST_F(TResourceManagerTest, LoadThrowsForPreloadRequiredNotStarted)
{
    // Resource has PreloadRequired=true but UpdatePreloadedResources was never called.
    THashMap<TResourceId, TResourceSpecPtr> resources;
    resources["res"] = BuildPreloadRequiredSlowResourceSpec();

    auto actionQueue = New<TActionQueue>();
    auto resourceManager = CreateManager(resources, actionQueue->GetInvoker());

    EXPECT_THROW(std::ignore = resourceManager->Load("res"), NYT::TErrorException);
}

TEST_F(TResourceManagerTest, LoadSuccessForPreloadRequiredNotLoaded)
{
    // Resource has PreloadRequired=true and UpdatePreloadedResources was called,
    // but the load has not completed yet (still Preloading).
    THashMap<TResourceId, TResourceSpecPtr> resources;
    resources["res"] = BuildPreloadRequiredSlowResourceSpec();

    auto actionQueue = New<TActionQueue>();
    auto resourceManager = CreateManager(resources, actionQueue->GetInvoker());

    resourceManager->UpdatePreloadedResources({"res"});

    // State is Preloading — Load returns non ready future.
    auto future = resourceManager->Load("res");
    EXPECT_FALSE(future.IsSet());
}

//! An always-on resource is Load'ed eagerly at manager construction, without anyone calling Load()
//! or scheduling a preload. This is what keeps node-wide singletons (e.g. a REX server) alive
//! independent of jobs.
TEST_F(TResourceManagerTest, AlwaysOnResourceIsLoadedEagerly)
{
    THashMap<TResourceId, TResourceSpecPtr> resources;
    auto spec = BuildSlowResourceSpec();
    spec->AlwaysOn = true;
    resources["always"] = spec;

    auto resourceManager = CreateManager(resources);

    // Pump the current invoker so the eagerly-scheduled load actually runs.
    WaitFor(BIND([] {
    }).AsyncVia(GetCurrentInvoker())
            .Run())
        .ThrowOnError();

    auto resource = TSlowResource::GetLastResource();
    ASSERT_TRUE(resource);
    EXPECT_TRUE(resource->IsLoadStarted());
}

//! LoadRequiredResources({}) resolves only once every always-on resource has finished loading.
TEST_F(TResourceManagerTest, ReadyFutureAwaitsAllAlwaysOnLoads)
{
    auto actionQueue = New<TActionQueue>();

    THashMap<TResourceId, TResourceSpecPtr> resources;
    resources["a"] = BuildSlowResourceSpec(/*dependencies*/ {}, /*alwaysOn*/ true);
    resources["b"] = BuildSlowResourceSpec(/*dependencies*/ {}, /*alwaysOn*/ true);

    auto resourceManager = CreateManager(resources, actionQueue->GetInvoker());
    auto ready = resourceManager->LoadRequiredResources({});

    // Both always-on resources start loading eagerly, but neither has completed.
    DrainInvoker(actionQueue->GetInvoker());
    auto a = TSlowResource::GetById("a");
    auto b = TSlowResource::GetById("b");
    ASSERT_TRUE(a);
    ASSERT_TRUE(b);
    EXPECT_TRUE(a->IsLoadStarted());
    EXPECT_TRUE(b->IsLoadStarted());
    EXPECT_FALSE(ready.IsSet());

    // Completing just one load is not enough: LoadRequiredResources awaits all of them.
    a->CompleteLoad();
    DrainInvoker(actionQueue->GetInvoker());
    EXPECT_FALSE(ready.IsSet());

    // Once the last always-on load completes, the ready future resolves.
    b->CompleteLoad();
    WaitFor(ready).ThrowOnError();
    EXPECT_TRUE(ready.IsSet());
}

//! Two dependent always-on resources: the dependent waits for its dependency, and
//! LoadRequiredResources({}) resolves only after both have loaded.
TEST_F(TResourceManagerTest, ReadyFutureAwaitsDependentAlwaysOnLoads)
{
    auto actionQueue = New<TActionQueue>();

    THashMap<TResourceId, TResourceSpecPtr> resources;
    resources["dep"] = BuildSlowResourceSpec(/*dependencies*/ {}, /*alwaysOn*/ true);
    resources["main"] = BuildSlowResourceSpec(/*dependencies*/ {"dep"}, /*alwaysOn*/ true);

    auto resourceManager = CreateManager(resources, actionQueue->GetInvoker());
    auto ready = resourceManager->LoadRequiredResources({});

    // The dependency starts loading; the dependent must wait for it.
    DrainInvoker(actionQueue->GetInvoker());
    auto dep = TSlowResource::GetById("dep");
    auto main = TSlowResource::GetById("main");
    ASSERT_TRUE(dep);
    ASSERT_TRUE(main);
    EXPECT_TRUE(dep->IsLoadStarted());
    EXPECT_FALSE(main->IsLoadStarted());
    EXPECT_FALSE(ready.IsSet());

    // Completing the dependency unblocks the dependent's load, but the ready future
    // still waits for the dependent itself.
    dep->CompleteLoad();
    DrainInvoker(actionQueue->GetInvoker());
    EXPECT_TRUE(main->IsLoadStarted());
    EXPECT_FALSE(ready.IsSet());

    main->CompleteLoad();
    WaitFor(ready).ThrowOnError();
    EXPECT_TRUE(ready.IsSet());
}

//! preload_required and always_on are mutually exclusive.
TEST_F(TResourceManagerTest, PreloadAndAlwaysOnAreMutuallyExclusive)
{
    auto yson = TYsonStringBuf("{resource_class_name=Foo; preload_required=%true; always_on=%true}");
    EXPECT_THROW(ConvertTo<NYT::NFlow::TResourceSpecPtr>(yson), NYT::TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NWorker
