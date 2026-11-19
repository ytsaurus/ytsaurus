#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/resources/resource_base.h>
#include <yt/yt/flow/library/cpp/resources/resource_controller_base.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/resource.h>
#include <yt/yt/flow/library/cpp/common/resource_controller.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/common/unittests/mock/time_provider.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/system/type_name.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

struct TChannelParameters
    : public virtual TYsonStruct
{
    std::string Tag;

    REGISTER_YSON_STRUCT(TChannelParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("tag", &TThis::Tag)
            .Default("none");
    }
};

class TChannelController
    : public TResourceControllerBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TChannelParameters);

    using TResourceControllerBase::TResourceControllerBase;

    INodePtr TargetSpec;

    INodePtr DoBuildTargetRevisionSpec() override
    {
        return TargetSpec;
    }

    void DoCollectStatuses(
        const THashMap<std::string, TWorkerResourceStatusPtr>& /*workerStatuses*/,
        const TWorkerResourceStatusPtr& /*controllerStatus*/) override
    { }

    IMapNodePtr DoGetView() override
    {
        return nullptr;
    }
};

class TChannelResource
    : public TResourceBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TChannelParameters);

    using TController = TChannelController;

    using TResourceBase::TResourceBase;
};

YT_FLOW_DEFINE_RESOURCE(TChannelResource);

class TPlainResource
    : public TResourceBase
{
public:
    using TResourceBase::TResourceBase;
};

YT_FLOW_DEFINE_RESOURCE(TPlainResource);

////////////////////////////////////////////////////////////////////////////////

TResourceSpecPtr MakeResourceSpec(const std::string& className, const std::string& parametersYson)
{
    auto spec = New<TResourceSpec>();
    spec->ResourceClassName = className;
    spec->Parameters = ConvertTo<IMapNodePtr>(TYsonString(parametersYson));
    return spec;
}

TResourceControllerContextPtr MakeControllerContext(const TResourceSpecPtr& spec)
{
    auto context = New<TResourceControllerContext>();
    context->ResourceId = TResourceId("TestResource");
    context->ResourceSpec = spec;
    context->Invoker = GetSyncInvoker();
    context->VersionProvider = TestVersionProvider();
    return context;
}

TDynamicResourceControllerContextPtr MakeDynamicControllerContext()
{
    auto context = New<TDynamicResourceControllerContext>();
    context->DynamicResourceSpec = New<TDynamicResourceSpec>();
    return context;
}

TResourceContextPtr MakeResourceContext(const TResourceSpecPtr& spec)
{
    auto context = New<TResourceContext>();
    context->ResourceId = TResourceId("TestResource");
    context->ResourceSpec = spec;
    return context;
}

TDynamicResourceContextPtr MakeDynamicResourceContext()
{
    auto context = New<TDynamicResourceContext>();
    context->DynamicResourceSpec = New<TDynamicResourceSpec>();
    return context;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TResourceControllerRegistryTest, PlainResourceGetsBaseController)
{
    auto spec = MakeResourceSpec(TypeName<TPlainResource>(), "{}");
    auto controller = TRegistry::Get()->CreateResourceController(
        MakeControllerContext(spec),
        MakeDynamicControllerContext());
    ASSERT_TRUE(controller);
    EXPECT_TRUE(DynamicPointerCast<TResourceControllerBase>(controller));
    controller->Init(nullptr);
    ASSERT_TRUE(controller->GetPreparingFuture().IsSet());
    NConcurrency::WaitFor(controller->GetPreparingFuture())
        .ThrowOnError();
    EXPECT_FALSE(controller->BuildTargetRevision());
    EXPECT_FALSE(controller->GetView());
}

TEST(TResourceControllerRegistryTest, ControllableResourceCreatesController)
{
    auto spec = MakeResourceSpec(TypeName<TChannelResource>(), "{tag = \"from-spec\"}");
    auto controller = TRegistry::Get()->CreateResourceController(
        MakeControllerContext(spec),
        MakeDynamicControllerContext());
    ASSERT_TRUE(controller);

    auto typedController = DynamicPointerCast<TChannelController>(controller);
    ASSERT_TRUE(typedController);
    EXPECT_EQ(typedController->GetParameters()->Tag, "from-spec");
}

TEST(TResourceControllerRegistryTest, TargetRevisionIdentityFollowsContentAndReconfiguration)
{
    auto context = MakeControllerContext(MakeResourceSpec(TypeName<TChannelResource>(), "{}"));
    auto dynamicContext = MakeDynamicControllerContext();
    auto controller = New<TChannelController>(context, dynamicContext);
    controller->TargetSpec = ConvertToNode("first");
    controller->Init(nullptr);
    auto build = [&] {
        return controller->BuildTargetRevision();
    };

    auto first = build();
    ASSERT_TRUE(first);
    EXPECT_GT(first->RevisionId, 0);
    EXPECT_EQ(build(), first);

    controller->TargetSpec->AsString()->SetValue("second");
    auto second = build();
    EXPECT_GT(second->RevisionId, first->RevisionId);
    EXPECT_EQ(ConvertTo<std::string>(first->Spec), "first");
    EXPECT_EQ(build(), second);

    auto changedContext = MakeDynamicControllerContext();
    changedContext->DynamicResourceSpec->FileProviderDiscoverPeriod = TDuration::Seconds(31);
    controller->Reconfigure(changedContext);
    auto reconfigured = build();
    EXPECT_GT(reconfigured->RevisionId, second->RevisionId);
    EXPECT_TRUE(AreNodesEqual(reconfigured->Spec, second->Spec));
    EXPECT_EQ(build(), reconfigured);

    controller = New<TChannelController>(context, changedContext);
    controller->TargetSpec = ConvertToNode("second");
    controller->Init(nullptr);
    EXPECT_GT(build()->RevisionId, reconfigured->RevisionId);
}

TEST(TResourceBaseTest, DestructionCancelsContextAndDependents)
{
    auto spec = MakeResourceSpec(TypeName<TPlainResource>(), "{}");
    auto resource = New<TPlainResource>(MakeResourceContext(spec), MakeDynamicResourceContext());
    auto context = resource->GetCancelableContext();
    auto dependent = New<TCancelableContext>();
    context->PropagateTo(dependent);
    auto futureDestroyed = resource->GetDestroyedFuture();
    auto weakResource = MakeWeak(resource);
    int cancellationCount = 0;
    context->SubscribeCanceled(BIND([&] (const TError& error) {
        ++cancellationCount;
        EXPECT_TRUE(error.FindMatching(NYT::EErrorCode::Canceled));
        EXPECT_TRUE(weakResource.IsExpired());
        EXPECT_FALSE(futureDestroyed.IsSet());
    }));

    resource.Reset();

    EXPECT_EQ(cancellationCount, 1);
    EXPECT_TRUE(context->IsCanceled());
    EXPECT_TRUE(dependent->IsCanceled());
    EXPECT_TRUE(futureDestroyed.IsSet());
}

TEST(TResourceBaseTest, StopPreservesFirstErrorThroughDestruction)
{
    auto spec = MakeResourceSpec(TypeName<TPlainResource>(), "{}");
    auto dynamicContext = MakeDynamicResourceContext();
    dynamicContext->TargetRevision = New<TResourceRevision>();
    dynamicContext->TargetRevision->RevisionId = 7;
    auto resource = New<TPlainResource>(MakeResourceContext(spec), dynamicContext);
    auto context = resource->GetCancelableContext();
    auto status = resource->GetStatus();
    EXPECT_TRUE(status.Error.IsOK());
    EXPECT_EQ(status.RevisionState.AppliedRevisionId, 7);

    resource->Stop(TError(TErrorCode(12345), "Original failure"));
    resource->Stop(TError(TErrorCode(12346), "Later failure"));

    status = resource->GetStatus();
    EXPECT_TRUE(status.Error.FindMatching(TErrorCode(12345)));
    EXPECT_FALSE(status.RevisionState.AppliedRevisionId);
    EXPECT_FALSE(status.RevisionState.TargetRevisionId);
    resource.Reset();
    ASSERT_TRUE(context->IsCanceled());
    EXPECT_TRUE(context->GetCancelationError().FindMatching(TErrorCode(12345)));
}

TEST(TResourceBaseTest, ReconfigureStopsOnFailure)
{
    auto spec = MakeResourceSpec(TypeName<TPlainResource>(), "{}");
    auto resource = New<TPlainResource>(MakeResourceContext(spec), MakeDynamicResourceContext());
    auto stopped = NewPromise<void>();
    resource->GetCancelableContext()->SubscribeCanceled(BIND([stopped] (const TError& error) {
        stopped.TrySet(error);
    }));
    int reconfigureCount = 0;
    resource->SubscribeReconfigured(BIND([&] (const TDynamicResourceContextPtr&) {
        ++reconfigureCount;
        THROW_ERROR_EXCEPTION(TErrorCode(12344 + reconfigureCount), "Failed to reconfigure resource");
    }));
    resource->SubscribeReconfigured(BIND([] (const TDynamicResourceContextPtr&) {
        ADD_FAILURE() << "Reconfiguration continued after failure";
    }));
    EXPECT_NO_THROW(resource->Reconfigure(MakeDynamicResourceContext()));
    EXPECT_NO_THROW(resource->Reconfigure(MakeDynamicResourceContext()));
    EXPECT_EQ(reconfigureCount, 2);
    EXPECT_TRUE(resource->GetStatus().Error.FindMatching(TErrorCode(12345)));
    auto error = stopped.ToFuture().TryGet();
    ASSERT_TRUE(error);
    EXPECT_TRUE(error->FindMatching(TErrorCode(12345)));
}

TEST(TResourceControllerBaseTest, ReconfigureStopsOnEveryFailure)
{
    auto spec = MakeResourceSpec(TypeName<TPlainResource>(), "{}");
    auto controller = TRegistry::Get()->CreateResourceController(
        MakeControllerContext(spec),
        MakeDynamicControllerContext());
    int reconfigureCount = 0;
    controller->SubscribeReconfigured(BIND([&] (const TDynamicResourceControllerContextPtr&) {
        ++reconfigureCount;
        THROW_ERROR_EXCEPTION(TErrorCode(12344 + reconfigureCount), "Failed to reconfigure controller");
    }));
    controller->SubscribeReconfigured(BIND([] (const TDynamicResourceControllerContextPtr&) {
        ADD_FAILURE() << "Reconfiguration continued after failure";
    }));

    EXPECT_NO_THROW(controller->Reconfigure(MakeDynamicControllerContext()));
    EXPECT_NO_THROW(controller->Reconfigure(MakeDynamicControllerContext()));
    EXPECT_EQ(reconfigureCount, 2);
    auto error = controller->GetStatus();
    ASSERT_TRUE(error);
    EXPECT_TRUE(error->FindMatching(TErrorCode(12345)));
}

TEST(TResourceBaseTest, RevisionStateFollowsDeliveredTarget)
{
    auto spec = MakeResourceSpec(TypeName<TPlainResource>(), "{}");
    auto resourceContext = MakeResourceContext(spec);
    resourceContext->ResourceInstanceId = TResourceInstanceId(TGuid::Create());
    resourceContext->ResourceIncarnationGeneration = 5;
    auto resource = TRegistry::Get()->CreateResource(
        resourceContext,
        MakeDynamicResourceContext());

    EXPECT_EQ(resource->GetRevisionState().AppliedRevisionId, std::nullopt);
    EXPECT_EQ(resource->GetRevisionState().TargetRevisionId, std::nullopt);

    auto revision = New<TResourceRevision>();
    revision->RevisionId = 7;
    auto targetContext = MakeDynamicResourceContext();
    targetContext->TargetRevision = revision;
    resource->Reconfigure(targetContext);

    // The base class treats switching as instant: both ids equal the delivered target.
    EXPECT_EQ(resource->GetRevisionState().AppliedRevisionId, std::optional<i64>(7));
    EXPECT_EQ(resource->GetRevisionState().TargetRevisionId, std::optional<i64>(7));
    EXPECT_EQ(resource->GetRevisionState().ResourceInstanceId, resourceContext->ResourceInstanceId);
    EXPECT_EQ(resource->GetRevisionState().ResourceIncarnationGeneration, 5u);

    resource->Reconfigure(MakeDynamicResourceContext());

    EXPECT_EQ(resource->GetRevisionState().AppliedRevisionId, std::nullopt);
    EXPECT_EQ(resource->GetRevisionState().TargetRevisionId, std::nullopt);
}

TEST(TResourceControllerRegistryTest, ControllerParametersShareResourceSpec)
{
    auto spec = MakeResourceSpec(TypeName<TChannelResource>(), "{tag = \"shared\"}");

    // The controller and the worker-side resource parse the same spec `parameters` block.
    auto controller = TRegistry::Get()->CreateResourceController(
        MakeControllerContext(spec),
        MakeDynamicControllerContext());
    ASSERT_TRUE(controller);
    EXPECT_EQ(DynamicPointerCast<TChannelController>(controller)->GetParameters()->Tag, "shared");

    auto resource = TRegistry::Get()->CreateResource(
        MakeResourceContext(spec),
        MakeDynamicResourceContext());
    ASSERT_TRUE(resource);
    EXPECT_EQ(resource->As<TChannelResource>()->GetParameters()->Tag, "shared");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
