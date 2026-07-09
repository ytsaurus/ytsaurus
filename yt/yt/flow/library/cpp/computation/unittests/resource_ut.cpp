#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/resources/resource_base.h>

#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/resource.h>
#include <yt/yt/flow/library/cpp/common/resource_manager.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/actions/future.h>

#include <util/system/type_name.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(NLogging::TLogger, Logger, "TestLogger");

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TReconfigurableTestResource);

class TReconfigurableTestResource
    : public TResourceBase
{
public:
    TReconfigurableTestResource(TResourceContextPtr context, TDynamicResourceContextPtr dynamicContext)
        : TResourceBase(std::move(context), std::move(dynamicContext))
    {
        SubscribeReconfigured(BIND([this] (const TDynamicResourceContextPtr& /*dynamicContext*/) {
            DoReconfigure();
        }));
    }

    int GetDoReconfigureCount() const
    {
        return DoReconfigureCount_;
    }

protected:
    void DoReconfigure()
    {
        ++DoReconfigureCount_;
    }

private:
    int DoReconfigureCount_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TReconfigurableTestResource);

YT_FLOW_DEFINE_RESOURCE(TReconfigurableTestResource);

////////////////////////////////////////////////////////////////////////////////

class TResourceBaseTest
    : public ::testing::Test
{
public:
    TResourceSpecPtr BuildReconfigurableResourceSpec()
    {
        auto spec = New<TResourceSpec>();
        spec->ResourceClassName = TypeName<TReconfigurableTestResource>();
        return spec;
    }

    TDynamicResourceSpecPtr BuildDynamicResourceSpec(const TString& paramValue)
    {
        auto dynamicSpec = New<TDynamicResourceSpec>();
        dynamicSpec->Parameters->AddChild("value", ConvertToNode(TYsonStringBuf(paramValue)));
        return dynamicSpec;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TResourceBaseTest, ReconfigureBasic)
{
    THashMap<TResourceId, TResourceSpecPtr> resources;
    resources["res"] = BuildReconfigurableResourceSpec();

    auto context = New<TResourceManagerContext>();
    context->Invoker = GetCurrentInvoker();
    context->Logger = Logger();
    context->StatusProfiler = CreateStatusProfiler();
    auto resourceManager = CreateResourceManager(std::move(context), resources, {});

    WaitFor(resourceManager->Load("res"))
        .ThrowOnError();

    auto resource = resourceManager->Get("res")->As<TReconfigurableTestResource>();
    ASSERT_EQ(resource->GetDoReconfigureCount(), 0);

    // Reconfigure with a new dynamic spec.
    THashMap<TResourceId, TDynamicResourceSpecPtr> newDynamicSpecs;
    newDynamicSpecs["res"] = BuildDynamicResourceSpec("value1");

    resourceManager->Reconfigure(newDynamicSpecs);

    ASSERT_EQ(resource->GetDoReconfigureCount(), 1);

    // Verify that GetDynamicSpec returns the updated spec.
    auto dynamicSpec = resource->GetDynamicSpec();
    ASSERT_TRUE(dynamicSpec);
    ASSERT_TRUE(dynamicSpec->Parameters);
    ASSERT_EQ(dynamicSpec->Parameters->GetChildValueOrThrow<std::string>("value"), "value1");
}

TEST_F(TResourceBaseTest, ReconfigureWithFailedAndReconfigurableResources)
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

    WaitFor(resourceManager->Load("good_res"))
        .ThrowOnError();

    auto goodResource = resourceManager->Get("good_res")->As<TReconfigurableTestResource>();
    ASSERT_EQ(goodResource->GetDoReconfigureCount(), 0);

    // Reconfigure both resources — should not throw.
    THashMap<TResourceId, TDynamicResourceSpecPtr> newDynamicSpecs;
    newDynamicSpecs["failed_res"] = BuildDynamicResourceSpec("value1");
    newDynamicSpecs["good_res"] = BuildDynamicResourceSpec("value2");

    EXPECT_NO_THROW(resourceManager->Reconfigure(newDynamicSpecs));

    // Verify that the reconfigurable resource was successfully reconfigured.
    ASSERT_EQ(goodResource->GetDoReconfigureCount(), 1);

    // Verify that GetDynamicSpec returns the updated spec.
    auto dynamicSpec = goodResource->GetDynamicSpec();
    ASSERT_TRUE(dynamicSpec);
    ASSERT_TRUE(dynamicSpec->Parameters);
    ASSERT_EQ(dynamicSpec->Parameters->GetChildValueOrThrow<std::string>("value"), "value2");
}

TEST_F(TResourceBaseTest, ReconfigureSkipsUnchangedSpec)
{
    THashMap<TResourceId, TResourceSpecPtr> resources;
    resources["res"] = BuildReconfigurableResourceSpec();

    auto context = New<TResourceManagerContext>();
    context->Invoker = GetCurrentInvoker();
    context->Logger = Logger();
    context->StatusProfiler = CreateStatusProfiler();
    auto resourceManager = CreateResourceManager(std::move(context), resources, {});

    WaitFor(resourceManager->Load("res"))
        .ThrowOnError();

    auto resource = resourceManager->Get("res")->As<TReconfigurableTestResource>();
    ASSERT_EQ(resource->GetDoReconfigureCount(), 0);

    // Initial reconfigure.
    THashMap<TResourceId, TDynamicResourceSpecPtr> dynamicSpecs;
    dynamicSpecs["res"] = BuildDynamicResourceSpec("value1");

    resourceManager->Reconfigure(dynamicSpecs);

    ASSERT_EQ(resource->GetDoReconfigureCount(), 1);

    // Reconfigure with the same dynamic spec parameters — should be skipped.
    THashMap<TResourceId, TDynamicResourceSpecPtr> sameDynamicSpecs;
    sameDynamicSpecs["res"] = BuildDynamicResourceSpec("value1");

    resourceManager->Reconfigure(sameDynamicSpecs);

    // DoReconfigure should not be called again since the spec hasn't changed.
    ASSERT_EQ(resource->GetDoReconfigureCount(), 1);

    // Verify that GetDynamicSpec still returns the same spec.
    auto dynamicSpec = resource->GetDynamicSpec();
    ASSERT_TRUE(dynamicSpec);
    ASSERT_TRUE(dynamicSpec->Parameters);
    ASSERT_EQ(dynamicSpec->Parameters->GetChildValueOrThrow<std::string>("value"), "value1");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
