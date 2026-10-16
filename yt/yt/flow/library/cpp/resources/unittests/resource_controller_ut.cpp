#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/resources/resource_base.h>
#include <yt/yt/flow/library/cpp/resources/resource_controller_base.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/resource.h>
#include <yt/yt/flow/library/cpp/common/resource_controller.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

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

    INodePtr DoBuildTargetRevisionSpec() override
    {
        return nullptr;
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
