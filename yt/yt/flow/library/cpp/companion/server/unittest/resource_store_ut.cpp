#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/companion/server/resource_store.h>
#include <yt/yt/flow/library/cpp/companion/server/runtime_init_context.h>
#include <yt/yt/flow/library/cpp/companion/server/server.h>

#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/resource.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/companion/companion_model.h>
#include <yt/yt/flow/library/cpp/companion/companion_proxy.h>

#include <yt/yt/flow/library/cpp/process_function/testing/entity_builders.h>

#include <yt/yt/flow/library/cpp/resources/resource_base.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/testing/common/network.h>

#include <util/generic/map.h>
#include <util/system/type_name.h>

namespace NYT::NFlow::NCompanionServer {

using namespace NYTree;

using NCompanion::ECompanionResourceCommand;
using NCompanion::ECompanionResourceExecuteStatus;

////////////////////////////////////////////////////////////////////////////////

struct TUnittestDictionaryParameters
    : public NYTree::TYsonStruct
{
    std::string Path;

    REGISTER_YSON_STRUCT(TUnittestDictionaryParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("path", &TThis::Path)
            .Default();
    }
};

struct TUnittestDictionaryDynamicParameters
    : public NYTree::TYsonStruct
{
    std::string DynamicValue;

    REGISTER_YSON_STRUCT(TUnittestDictionaryDynamicParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("dynamic_value", &TThis::DynamicValue)
            .Default();
    }
};

//! A well-behaved companion-hosted resource capturing its lifecycle.
class TUnittestDictionaryResource
    : public TResourceBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TUnittestDictionaryParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TUnittestDictionaryDynamicParameters);

    static inline std::atomic<int> LoadCount{0};

    using TResourceBase::TResourceBase;

    TFuture<void> Load(const THashMap<TResourceId, IResourcePtr>& /*dependencies*/) override
    {
        ++LoadCount;
        return OKFuture;
    }
};

//! Counts reconfigurations and can fail after the base publishes new dynamic parameters.
class TUnittestReconfigurableResource
    : public TResourceBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TUnittestDictionaryParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TUnittestDictionaryDynamicParameters);

    static inline std::atomic<int> ConstructionCount{0};
    static inline std::atomic<int> LoadCount{0};
    static inline std::atomic<int> ReconfigureCount{0};
    static inline std::atomic<bool> FailReconfigure{false};
    static inline std::atomic<i64> ConstructionRevisionId{-1};
    static inline std::atomic<i64> ReconfigureRevisionId{-1};

    TUnittestReconfigurableResource(
        TResourceContextPtr context,
        TDynamicResourceContextPtr dynamicContext)
        : TResourceBase(std::move(context), std::move(dynamicContext))
    {
        ++ConstructionCount;
        auto initialDynamicContext = GetDynamicContext();
        ConstructionRevisionId = initialDynamicContext->TargetRevision
            ? initialDynamicContext->TargetRevision->RevisionId
            : -1;
        SubscribeReconfigured(BIND([] (const TDynamicResourceContextPtr& dynamicContext) {
            ++ReconfigureCount;
            ReconfigureRevisionId = dynamicContext->TargetRevision
                ? dynamicContext->TargetRevision->RevisionId
                : -1;
            if (FailReconfigure) {
                THROW_ERROR_EXCEPTION("Reconfigure failure");
            }
        }));
    }

    TFuture<void> Load(const THashMap<TResourceId, IResourcePtr>& /*dependencies*/) override
    {
        ++LoadCount;
        return OKFuture;
    }
};

//! Hands the target revision over to an asynchronous switch: Reconfigure
//! returns at once while GetRevisionState keeps reporting the previous applied
//! id until the test advances it.
class TUnittestSlowRevisionResource
    : public TResourceBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TUnittestDictionaryParameters);
    YT_FLOW_EXTEND_DYNAMIC_PARAMETERS(TUnittestDictionaryDynamicParameters);

    static inline std::atomic<int> LoadCount{0};
    static inline std::atomic<int> ReconfigureCount{0};
    //! Negative means "no id".
    static inline std::atomic<i64> AppliedRevisionId{-1};
    static inline std::atomic<i64> TargetRevisionId{-1};

    TUnittestSlowRevisionResource(
        TResourceContextPtr context,
        TDynamicResourceContextPtr dynamicContext)
        : TResourceBase(std::move(context), std::move(dynamicContext))
    {
        auto initialDynamicContext = GetDynamicContext();
        TargetRevisionId = initialDynamicContext->TargetRevision
            ? initialDynamicContext->TargetRevision->RevisionId
            : -1;
        SubscribeReconfigured(BIND([] (const TDynamicResourceContextPtr& dynamicContext) {
            ++ReconfigureCount;
            TargetRevisionId = dynamicContext->TargetRevision
                ? dynamicContext->TargetRevision->RevisionId
                : -1;
        }));
    }

    TFuture<void> Load(const THashMap<TResourceId, IResourcePtr>& /*dependencies*/) override
    {
        ++LoadCount;
        // A completed Load means the instance already serves its initial revision.
        AppliedRevisionId = TargetRevisionId.load();
        return OKFuture;
    }

    TResourceRevisionState GetRevisionState() const override
    {
        auto toOptional = [] (i64 value) -> std::optional<i64> {
            return value < 0 ? std::nullopt : std::make_optional(value);
        };
        return {
            .AppliedRevisionId = toOptional(AppliedRevisionId.load()),
            .TargetRevisionId = toOptional(TargetRevisionId.load()),
        };
    }
};

//! Captures the dependencies map its Load receives.
class TUnittestDependentResource
    : public TResourceBase
{
public:
    static inline THashMap<TResourceId, IResourcePtr> LastDependencies;

    using TResourceBase::TResourceBase;

    TFuture<void> Load(const THashMap<TResourceId, IResourcePtr>& dependencies) override
    {
        LastDependencies = dependencies;
        return OKFuture;
    }
};

//! Fails its load while |FailLoad| is set.
class TUnittestFlakyResource
    : public TResourceBase
{
public:
    static inline std::atomic<bool> FailLoad{false};

    using TResourceBase::TResourceBase;

    TFuture<void> Load(const THashMap<TResourceId, IResourcePtr>& /*dependencies*/) override
    {
        if (FailLoad) {
            return MakeFuture(TError("Flaky load failure"));
        }
        return OKFuture;
    }
};

//! Blocks its load on |Gate| so tests can observe the loading window.
class TUnittestGatedResource
    : public TResourceBase
{
public:
    static inline std::atomic<int> LoadCount{0};
    static inline TPromise<void> LoadStarted;
    static inline TFuture<void> Gate;

    using TResourceBase::TResourceBase;

    TFuture<void> Load(const THashMap<TResourceId, IResourcePtr>& /*dependencies*/) override
    {
        ++LoadCount;
        if (LoadStarted) {
            LoadStarted.TrySet();
        }
        return Gate ? Gate : OKFuture;
    }
};

//! Resolves the dictionary resource during Init, also through WithPrefix.
class TUnittestResourceConsumerFunction
    : public IProcessFunction
{
public:
    static inline std::atomic<bool> ProcessCalled{false};
    static inline std::atomic<int> ProcessCount{0};
    static inline std::atomic<bool> ResourceIdWasVisible{false};
    static inline std::string ObservedPath;
    static inline TPromise<void> ProcessStarted;
    static inline TFuture<void> ProcessGate;

    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        auto resource = initContext->GetStaticResource(TResourceId("dictionary_alias"));
        ObservedPath = resource->As<TUnittestDictionaryResource>()->GetParameters()->Path;
        // WithPrefix must preserve the resource lookup.
        Y_UNUSED(initContext->WithPrefix("sub")->GetStaticResource(TResourceId("dictionary_alias")));
        try {
            Y_UNUSED(initContext->GetStaticResource(TResourceId("my_dictionary")));
            ResourceIdWasVisible = true;
        } catch (const std::exception&) {
            ResourceIdWasVisible = false;
        }
    }

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        ProcessCalled = true;
        ++ProcessCount;
        if (ProcessStarted) {
            ProcessStarted.TrySet();
        }
        if (ProcessGate) {
            NConcurrency::WaitFor(ProcessGate).ThrowOnError();
        }
        output->AddMessage(context->ConvertToOutputMessage(*message));
    }
};

////////////////////////////////////////////////////////////////////////////////

namespace {

using NYT::FromProto;
using NYT::ToProto;

const TResourceInstanceId DefaultIncarnationId(TGuid::Create());

TResourceInstanceId MakeIncarnationId()
{
    return TResourceInstanceId(TGuid::Create());
}

NCompanion::TCompanionResourceInstanceReference MakeReference(
    const std::string& resourceId,
    const TResourceInstanceId& incarnationId,
    ui64 configurationGeneration,
    std::optional<TResourceId> alias = std::nullopt)
{
    NCompanion::TCompanionResourceInstanceReference reference;
    reference.ResourceId = TResourceId(resourceId);
    reference.IncarnationId = incarnationId;
    reference.ConfigurationGeneration = configurationGeneration;
    reference.Alias = std::move(alias);
    return reference;
}

TResourceSpecPtr BuildResourceSpec(
    const std::string& className,
    const std::string& path = "//path",
    const std::vector<std::pair<TResourceId, std::optional<TResourceId>>>& dependencies = {})
{
    auto spec = New<TResourceSpec>();
    // On the wire the class names the worker-side proxy; the companion must
    // instantiate by |companion_resource_class| instead.
    spec->ResourceClassName = "NYT::NFlow::NCompanion::TCompanionResource";
    spec->Parameters = BuildYsonNodeFluently()
        .BeginMap()
        .Item("companion_resource_class")
        .Value(className)
        .Item("path")
        .Value(path)
        .EndMap()
        ->AsMap();
    for (const auto& [dependencyId, alias] : dependencies) {
        auto description = New<TResourceDescription>();
        description->Alias = alias;
        description->Worker = true;
        spec->Dependencies[dependencyId] = std::move(description);
    }
    return spec;
}

TDynamicResourceSpecPtr BuildDynamicResourceSpec(const std::string& dynamicValue = "v1")
{
    auto spec = New<TDynamicResourceSpec>();
    spec->Parameters = BuildYsonNodeFluently()
        .BeginMap()
        .Item("dynamic_value")
        .Value(dynamicValue)
        .EndMap()
        ->AsMap();
    return spec;
}

TResourceRevisionPtr BuildResourceRevision(i64 revisionId, const std::string& path)
{
    auto revision = New<TResourceRevision>();
    revision->RevisionId = revisionId;
    revision->Spec = BuildYsonNodeFluently()
        .BeginMap()
        .Item("path")
        .Value(path)
        .EndMap();
    return revision;
}

NYson::TYsonString BuildInitArgument(
    const TResourceSpecPtr& spec,
    const TDynamicResourceSpecPtr& dynamicSpec,
    const TResourceInstanceId& incarnationId = DefaultIncarnationId,
    ui64 configurationGeneration = 1,
    ui64 incarnationGeneration = 0,
    std::vector<NCompanion::TCompanionResourceInstanceReference> dependencies = {},
    TResourceRevisionPtr resourceRevision = nullptr)
{
    NCompanion::TInitResourceCommandArg arg;
    arg.Spec = spec;
    arg.DynamicSpec = dynamicSpec;
    arg.IncarnationId = incarnationId;
    arg.IncarnationGeneration = incarnationGeneration;
    arg.ConfigurationGeneration = configurationGeneration;
    arg.Dependencies = std::move(dependencies);
    arg.ResourceRevision = std::move(resourceRevision);
    return NYson::ConvertToYsonString(arg);
}

NYson::TYsonString BuildUnloadArgument(
    const TResourceInstanceId& incarnationId = DefaultIncarnationId)
{
    NCompanion::TUnloadResourceCommandArg arg;
    arg.IncarnationId = incarnationId;
    return NYson::ConvertToYsonString(arg);
}

////////////////////////////////////////////////////////////////////////////////

class TResourceStoreTest
    : public ::testing::Test
{
protected:
    NConcurrency::TActionQueuePtr Queue_ = New<NConcurrency::TActionQueue>("Test");
    TResourceStorePtr Store_;

    void SetUp() override
    {
        TPipeline pipeline;
        pipeline.AddResource<TUnittestDictionaryResource>();
        pipeline.AddResource<TUnittestReconfigurableResource>();
        pipeline.AddResource<TUnittestSlowRevisionResource>();
        pipeline.AddResource<TUnittestDependentResource>();
        pipeline.AddResource<TUnittestFlakyResource>();
        pipeline.AddResource<TUnittestGatedResource>();
        Store_ = New<TResourceStore>(
            pipeline.GetResourceClassNames(),
            Queue_->GetInvoker());

        TUnittestDictionaryResource::LoadCount = 0;
        TUnittestReconfigurableResource::ConstructionCount = 0;
        TUnittestReconfigurableResource::LoadCount = 0;
        TUnittestReconfigurableResource::ReconfigureCount = 0;
        TUnittestReconfigurableResource::FailReconfigure = false;
        TUnittestReconfigurableResource::ConstructionRevisionId = -1;
        TUnittestReconfigurableResource::ReconfigureRevisionId = -1;
        TUnittestSlowRevisionResource::LoadCount = 0;
        TUnittestSlowRevisionResource::ReconfigureCount = 0;
        TUnittestSlowRevisionResource::AppliedRevisionId = -1;
        TUnittestSlowRevisionResource::TargetRevisionId = -1;
        TUnittestDependentResource::LastDependencies.clear();
        TUnittestFlakyResource::FailLoad = false;
        TUnittestGatedResource::LoadCount = 0;
        TUnittestGatedResource::LoadStarted = {};
        TUnittestGatedResource::Gate = {};
    }

    TResourceCommandOutcome Execute(
        const std::string& resourceId,
        ECompanionResourceCommand command,
        const NYson::TYsonString& argument = {})
    {
        auto outcome = Store_->Execute(TResourceId(resourceId), command, argument)
            .BlockingGet()
            .ValueOrThrow();
        if (outcome.Status != ECompanionResourceExecuteStatus::Ok) {
            return outcome;
        }

        if (command == ECompanionResourceCommand::Init) {
            auto arg = ConvertTo<NCompanion::TInitResourceCommandArg>(argument);
            auto reference = MakeReference(
                resourceId,
                arg.IncarnationId,
                arg.ConfigurationGeneration);
            if (Store_->FindInitializedResource(reference)) {
                SuccessfulReferences_[TResourceId(resourceId)] = std::move(reference);
            }
        } else if (command == ECompanionResourceCommand::Unload) {
            if (auto it = SuccessfulReferences_.find(TResourceId(resourceId));
                it != SuccessfulReferences_.end() &&
                !Store_->FindInitializedResource(it->second))
            {
                SuccessfulReferences_.erase(it);
            }
        }
        return outcome;
    }

    IResourcePtr Find(const std::string& resourceId)
    {
        auto it = SuccessfulReferences_.find(TResourceId(resourceId));
        if (it == SuccessfulReferences_.end()) {
            return nullptr;
        }
        return Store_->FindInitializedResource(it->second);
    }

    const NCompanion::TCompanionResourceInstanceReference& GetReference(const std::string& resourceId) const
    {
        return GetOrCrash(SuccessfulReferences_, TResourceId(resourceId));
    }

    THashMap<TResourceId, NCompanion::TCompanionResourceInstanceReference> SuccessfulReferences_;
};

TEST_F(TResourceStoreTest, InitOnceAndConvergeNoOp)
{
    auto argument = BuildInitArgument(
        BuildResourceSpec(TypeName<TUnittestDictionaryResource>()),
        BuildDynamicResourceSpec());

    EXPECT_FALSE(Find("dict"));
    EXPECT_EQ(Execute("dict", ECompanionResourceCommand::Init, argument).Status, ECompanionResourceExecuteStatus::Ok);
    auto resource = Find("dict");
    ASSERT_TRUE(resource);
    EXPECT_EQ(TUnittestDictionaryResource::LoadCount, 1);
    EXPECT_EQ(resource->As<TUnittestDictionaryResource>()->GetParameters()->Path, "//path");

    // Equal specs converge to a no-op on the same instance.
    EXPECT_EQ(Execute("dict", ECompanionResourceCommand::Init, argument).Status, ECompanionResourceExecuteStatus::Ok);
    EXPECT_EQ(TUnittestDictionaryResource::LoadCount, 1);
    EXPECT_EQ(Find("dict"), resource);
}

TEST_F(TResourceStoreTest, ConvergeOnChangedDynamicSpec)
{
    auto spec = BuildResourceSpec(TypeName<TUnittestDictionaryResource>());
    EXPECT_EQ(
        Execute("dict", ECompanionResourceCommand::Init, BuildInitArgument(spec, BuildDynamicResourceSpec("v1")))
            .Status,
        ECompanionResourceExecuteStatus::Ok);
    auto resource = Find("dict");

    EXPECT_EQ(
        Execute(
            "dict",
            ECompanionResourceCommand::Init,
            BuildInitArgument(spec, BuildDynamicResourceSpec("v2"), DefaultIncarnationId, 2))
            .Status,
        ECompanionResourceExecuteStatus::Ok);
    // Reconfigured in place: same instance, no reload, fresh dynamic parameters.
    EXPECT_EQ(Find("dict"), resource);
    EXPECT_EQ(TUnittestDictionaryResource::LoadCount, 1);
    EXPECT_EQ(
        resource->As<TUnittestDictionaryResource>()->GetDynamicParameters()->DynamicValue,
        "v2");
}

TEST_F(TResourceStoreTest, StaticSpecChangeRejected)
{
    auto dynamicSpec = BuildDynamicResourceSpec();
    EXPECT_EQ(
        Execute(
            "dict",
            ECompanionResourceCommand::Init,
            BuildInitArgument(
                BuildResourceSpec(TypeName<TUnittestDictionaryResource>(), "//a"),
                dynamicSpec))
            .Status,
        ECompanionResourceExecuteStatus::Ok);
    auto resource = Find("dict");

    auto outcome = Execute(
        "dict",
        ECompanionResourceCommand::Init,
        BuildInitArgument(
            BuildResourceSpec(TypeName<TUnittestDictionaryResource>(), "//b"),
            dynamicSpec));
    EXPECT_EQ(outcome.Status, ECompanionResourceExecuteStatus::Error);
    EXPECT_THAT(outcome.Error.GetMessage(), testing::HasSubstr("Static resource spec changed"));

    // The old instance keeps serving.
    EXPECT_EQ(Find("dict"), resource);
}

TEST_F(TResourceStoreTest, UnloadBarsNewAcquisitionAndKeepsHeldReferences)
{
    auto argument = BuildInitArgument(
        BuildResourceSpec(TypeName<TUnittestDictionaryResource>()),
        BuildDynamicResourceSpec());
    EXPECT_EQ(Execute("dict", ECompanionResourceCommand::Init, argument).Status, ECompanionResourceExecuteStatus::Ok);
    auto held = Find("dict");
    ASSERT_TRUE(held);

    EXPECT_EQ(
        Execute("dict", ECompanionResourceCommand::Unload, BuildUnloadArgument()).Status,
        ECompanionResourceExecuteStatus::Ok);
    // Barred from new jobs; the held reference stays usable until released.
    EXPECT_FALSE(Find("dict"));
    EXPECT_EQ(held->As<TUnittestDictionaryResource>()->GetParameters()->Path, "//path");

    // Unloading an unloaded resource is a converged no-op.
    EXPECT_EQ(
        Execute("dict", ECompanionResourceCommand::Unload, BuildUnloadArgument()).Status,
        ECompanionResourceExecuteStatus::Ok);
}

TEST_F(TResourceStoreTest, ReinitAfterUnloadCreatesFreshInstance)
{
    auto firstIncarnationId = MakeIncarnationId();
    auto successorIncarnationId = MakeIncarnationId();
    auto argument = BuildInitArgument(
        BuildResourceSpec(TypeName<TUnittestDictionaryResource>()),
        BuildDynamicResourceSpec(),
        firstIncarnationId);
    EXPECT_EQ(Execute("dict", ECompanionResourceCommand::Init, argument).Status, ECompanionResourceExecuteStatus::Ok);
    auto old = Find("dict");

    EXPECT_EQ(
        Execute(
            "dict",
            ECompanionResourceCommand::Unload,
            BuildUnloadArgument(firstIncarnationId))
            .Status,
        ECompanionResourceExecuteStatus::Ok);
    EXPECT_EQ(
        Execute(
            "dict",
            ECompanionResourceCommand::Init,
            BuildInitArgument(
                BuildResourceSpec(TypeName<TUnittestDictionaryResource>()),
                BuildDynamicResourceSpec(),
                successorIncarnationId,
                1,
                1))
            .Status,
        ECompanionResourceExecuteStatus::Ok);

    auto fresh = Find("dict");
    ASSERT_TRUE(fresh);
    EXPECT_NE(fresh, old);
    EXPECT_EQ(TUnittestDictionaryResource::LoadCount, 2);
}

TEST_F(TResourceStoreTest, LateUnloadCannotRetireSuccessor)
{
    auto firstIncarnationId = MakeIncarnationId();
    auto successorIncarnationId = MakeIncarnationId();
    auto spec = BuildResourceSpec(TypeName<TUnittestDictionaryResource>());

    EXPECT_EQ(
        Execute(
            "dict",
            ECompanionResourceCommand::Init,
            BuildInitArgument(spec, BuildDynamicResourceSpec(), firstIncarnationId))
            .Status,
        ECompanionResourceExecuteStatus::Ok);
    EXPECT_EQ(
        Execute(
            "dict",
            ECompanionResourceCommand::Init,
            BuildInitArgument(
                spec,
                BuildDynamicResourceSpec(),
                successorIncarnationId,
                1,
                1))
            .Status,
        ECompanionResourceExecuteStatus::Ok);

    EXPECT_EQ(
        Execute(
            "dict",
            ECompanionResourceCommand::Unload,
            BuildUnloadArgument(firstIncarnationId))
            .Status,
        ECompanionResourceExecuteStatus::Ok);
    EXPECT_TRUE(Store_->FindInitializedResource(
        MakeReference("dict", successorIncarnationId, 1)));
}

TEST_F(TResourceStoreTest, MismatchingUnloadDoesNotFenceFutureSuccessor)
{
    auto currentIncarnationId = MakeIncarnationId();
    auto successorIncarnationId = MakeIncarnationId();
    auto spec = BuildResourceSpec(TypeName<TUnittestDictionaryResource>());

    EXPECT_EQ(
        Execute(
            "dict",
            ECompanionResourceCommand::Init,
            BuildInitArgument(spec, BuildDynamicResourceSpec(), currentIncarnationId))
            .Status,
        ECompanionResourceExecuteStatus::Ok);
    auto currentResource = Find("dict");

    EXPECT_EQ(
        Execute(
            "dict",
            ECompanionResourceCommand::Unload,
            BuildUnloadArgument(successorIncarnationId))
            .Status,
        ECompanionResourceExecuteStatus::Ok);
    EXPECT_EQ(Find("dict"), currentResource);

    EXPECT_EQ(
        Execute(
            "dict",
            ECompanionResourceCommand::Init,
            BuildInitArgument(
                spec,
                BuildDynamicResourceSpec(),
                successorIncarnationId,
                1,
                1))
            .Status,
        ECompanionResourceExecuteStatus::Ok);
    EXPECT_TRUE(Store_->FindInitializedResource(
        MakeReference("dict", successorIncarnationId, 1)));
}

TEST_F(TResourceStoreTest, OutOfOrderInitConvergesToNewestIncarnation)
{
    auto firstIncarnationId = MakeIncarnationId();
    auto middleIncarnationId = MakeIncarnationId();
    auto newestIncarnationId = MakeIncarnationId();
    auto spec = BuildResourceSpec(TypeName<TUnittestDictionaryResource>());

    EXPECT_EQ(
        Execute(
            "dict",
            ECompanionResourceCommand::Init,
            BuildInitArgument(spec, BuildDynamicResourceSpec(), firstIncarnationId))
            .Status,
        ECompanionResourceExecuteStatus::Ok);
    EXPECT_EQ(
        Execute(
            "dict",
            ECompanionResourceCommand::Init,
            BuildInitArgument(
                spec,
                BuildDynamicResourceSpec(),
                newestIncarnationId,
                1,
                2))
            .Status,
        ECompanionResourceExecuteStatus::Ok);

    auto stale = Execute(
        "dict",
        ECompanionResourceCommand::Init,
        BuildInitArgument(
            spec,
            BuildDynamicResourceSpec(),
            middleIncarnationId,
            1,
            1));
    EXPECT_EQ(stale.Status, ECompanionResourceExecuteStatus::StaleResourceIncarnation);
}

TEST_F(TResourceStoreTest, RetiredIncarnationCannotBeRevived)
{
    auto retiredIncarnationId = MakeIncarnationId();
    auto successorIncarnationId = MakeIncarnationId();
    auto spec = BuildResourceSpec(TypeName<TUnittestDictionaryResource>());
    auto retiredArgument =
        BuildInitArgument(spec, BuildDynamicResourceSpec(), retiredIncarnationId);

    EXPECT_EQ(
        Execute("dict", ECompanionResourceCommand::Init, retiredArgument).Status,
        ECompanionResourceExecuteStatus::Ok);
    EXPECT_EQ(
        Execute(
            "dict",
            ECompanionResourceCommand::Unload,
            BuildUnloadArgument(retiredIncarnationId))
            .Status,
        ECompanionResourceExecuteStatus::Ok);
    EXPECT_EQ(
        Execute("dict", ECompanionResourceCommand::Init, retiredArgument).Status,
        ECompanionResourceExecuteStatus::StaleResourceIncarnation);

    EXPECT_EQ(
        Execute(
            "dict",
            ECompanionResourceCommand::Init,
            BuildInitArgument(
                spec,
                BuildDynamicResourceSpec(),
                successorIncarnationId,
                1,
                1))
            .Status,
        ECompanionResourceExecuteStatus::Ok);
    EXPECT_TRUE(Find("dict"));
}

TEST_F(TResourceStoreTest, ConfigurationGenerationsConverge)
{
    auto incarnationId = MakeIncarnationId();
    auto spec = BuildResourceSpec(TypeName<TUnittestReconfigurableResource>());
    EXPECT_EQ(
        Execute(
            "resource",
            ECompanionResourceCommand::Init,
            BuildInitArgument(spec, BuildDynamicResourceSpec("v1"), incarnationId, 1))
            .Status,
        ECompanionResourceExecuteStatus::Ok);

    auto reconfigureV2 = BuildInitArgument(
        spec,
        BuildDynamicResourceSpec("v2"),
        incarnationId,
        2);
    EXPECT_EQ(
        Execute("resource", ECompanionResourceCommand::Init, reconfigureV2).Status,
        ECompanionResourceExecuteStatus::Ok);
    EXPECT_EQ(
        Execute("resource", ECompanionResourceCommand::Init, reconfigureV2).Status,
        ECompanionResourceExecuteStatus::Ok);
    EXPECT_EQ(TUnittestReconfigurableResource::ReconfigureCount, 1);

    EXPECT_EQ(
        Execute(
            "resource",
            ECompanionResourceCommand::Init,
            BuildInitArgument(spec, BuildDynamicResourceSpec("old"), incarnationId, 1))
            .Status,
        ECompanionResourceExecuteStatus::Ok);
    EXPECT_EQ(TUnittestReconfigurableResource::ReconfigureCount, 1);

    auto conflicting = Execute(
        "resource",
        ECompanionResourceCommand::Init,
        BuildInitArgument(spec, BuildDynamicResourceSpec("conflict"), incarnationId, 2));
    EXPECT_EQ(conflicting.Status, ECompanionResourceExecuteStatus::Error);
    EXPECT_EQ(TUnittestReconfigurableResource::ReconfigureCount, 1);
    EXPECT_EQ(2u, GetReference("resource").ConfigurationGeneration);
}

TEST_F(TResourceStoreTest, ResourceRevisionsAreFencedAndDelivered)
{
    auto incarnationId = MakeIncarnationId();
    auto spec = BuildResourceSpec(TypeName<TUnittestReconfigurableResource>());
    auto revision1 = BuildResourceRevision(10, "/prepared/v1");
    auto initialArgument = BuildInitArgument(
        spec,
        BuildDynamicResourceSpec("v1"),
        incarnationId,
        1,
        0,
        {},
        revision1);

    EXPECT_EQ(
        Execute("resource", ECompanionResourceCommand::Init, initialArgument).Status,
        ECompanionResourceExecuteStatus::Ok);
    EXPECT_EQ(10, TUnittestReconfigurableResource::ConstructionRevisionId);

    // An exact duplicate is idempotent.
    EXPECT_EQ(
        Execute("resource", ECompanionResourceCommand::Init, initialArgument).Status,
        ECompanionResourceExecuteStatus::Ok);
    EXPECT_EQ(1, TUnittestReconfigurableResource::ConstructionCount);
    EXPECT_EQ(0, TUnittestReconfigurableResource::ReconfigureCount);

    auto conflicting = Execute(
        "resource",
        ECompanionResourceCommand::Init,
        BuildInitArgument(
            spec,
            BuildDynamicResourceSpec("v1"),
            incarnationId,
            1,
            0,
            {},
            BuildResourceRevision(11, "/prepared/conflict")));
    EXPECT_EQ(ECompanionResourceExecuteStatus::Error, conflicting.Status);
    EXPECT_EQ(0, TUnittestReconfigurableResource::ReconfigureCount);

    EXPECT_EQ(
        Execute(
            "resource",
            ECompanionResourceCommand::Init,
            BuildInitArgument(
                spec,
                BuildDynamicResourceSpec("v2"),
                incarnationId,
                2,
                0,
                {},
                BuildResourceRevision(12, "/prepared/v2")))
            .Status,
        ECompanionResourceExecuteStatus::Ok);
    EXPECT_EQ(1, TUnittestReconfigurableResource::ReconfigureCount);
    EXPECT_EQ(12, TUnittestReconfigurableResource::ReconfigureRevisionId);
}

TEST_F(TResourceStoreTest, ReconfigureWaitsForTheAppliedTargetRevision)
{
    auto incarnationId = MakeIncarnationId();
    auto spec = BuildResourceSpec(TypeName<TUnittestSlowRevisionResource>());
    EXPECT_EQ(
        Execute(
            "resource",
            ECompanionResourceCommand::Init,
            BuildInitArgument(
                spec,
                BuildDynamicResourceSpec("v1"),
                incarnationId,
                1,
                0,
                {},
                BuildResourceRevision(10, "/prepared/v1")))
            .Status,
        ECompanionResourceExecuteStatus::Ok);
    auto resource = Find("resource");
    ASSERT_TRUE(resource);

    // Reconfigure only hands the target over; the switch is still in flight.
    auto reconfigureArgument = BuildInitArgument(
        spec,
        BuildDynamicResourceSpec("v2"),
        incarnationId,
        2,
        0,
        {},
        BuildResourceRevision(11, "/prepared/v2"));
    auto outcome = Execute("resource", ECompanionResourceCommand::Init, reconfigureArgument);
    EXPECT_EQ(outcome.Status, ECompanionResourceExecuteStatus::ResourceNotInitialized);
    EXPECT_THAT(outcome.Error.GetMessage(), testing::HasSubstr("has not applied target revision"));
    EXPECT_EQ(1, TUnittestSlowRevisionResource::ReconfigureCount);
    // The new generation must not run batches while user code still sees the old revision.
    EXPECT_FALSE(Store_->FindInitializedResource(MakeReference("resource", incarnationId, 2)));

    // A retry of the same init converges in place once the switch lands.
    TUnittestSlowRevisionResource::AppliedRevisionId = 11;
    EXPECT_EQ(
        Execute("resource", ECompanionResourceCommand::Init, reconfigureArgument).Status,
        ECompanionResourceExecuteStatus::Ok);
    EXPECT_EQ(1, TUnittestSlowRevisionResource::ReconfigureCount);
    EXPECT_EQ(1, TUnittestSlowRevisionResource::LoadCount);
    EXPECT_EQ(Find("resource"), resource);
    EXPECT_TRUE(Store_->FindInitializedResource(MakeReference("resource", incarnationId, 2)));
}

TEST_F(TResourceStoreTest, PendingReconfigureIsSupersededByNewerGeneration)
{
    auto incarnationId = MakeIncarnationId();
    auto spec = BuildResourceSpec(TypeName<TUnittestSlowRevisionResource>());
    EXPECT_EQ(
        Execute(
            "resource",
            ECompanionResourceCommand::Init,
            BuildInitArgument(
                spec,
                BuildDynamicResourceSpec("v1"),
                incarnationId,
                1,
                0,
                {},
                BuildResourceRevision(10, "/prepared/v1")))
            .Status,
        ECompanionResourceExecuteStatus::Ok);

    EXPECT_EQ(
        Execute(
            "resource",
            ECompanionResourceCommand::Init,
            BuildInitArgument(
                spec,
                BuildDynamicResourceSpec("v2"),
                incarnationId,
                2,
                0,
                {},
                BuildResourceRevision(11, "/prepared/v2")))
            .Status,
        ECompanionResourceExecuteStatus::ResourceNotInitialized);

    // The stalled generation is abandoned: a newer one rebuilds a clean
    // instance that serves its revision from the completed load.
    EXPECT_EQ(
        Execute(
            "resource",
            ECompanionResourceCommand::Init,
            BuildInitArgument(
                spec,
                BuildDynamicResourceSpec("v3"),
                incarnationId,
                3,
                0,
                {},
                BuildResourceRevision(12, "/prepared/v3")))
            .Status,
        ECompanionResourceExecuteStatus::Ok);
    EXPECT_EQ(2, TUnittestSlowRevisionResource::LoadCount);
    EXPECT_TRUE(Store_->FindInitializedResource(MakeReference("resource", incarnationId, 3)));
}

TEST_F(TResourceStoreTest, FailedReconfigureQuarantinesAndRecreatesResource)
{
    auto incarnationId = MakeIncarnationId();
    auto spec = BuildResourceSpec(TypeName<TUnittestReconfigurableResource>());
    EXPECT_EQ(
        Execute(
            "resource",
            ECompanionResourceCommand::Init,
            BuildInitArgument(spec, BuildDynamicResourceSpec("v1"), incarnationId, 1))
            .Status,
        ECompanionResourceExecuteStatus::Ok);
    auto oldResource = Find("resource");

    TUnittestReconfigurableResource::FailReconfigure = true;
    auto outcome = Execute(
        "resource",
        ECompanionResourceCommand::Init,
        BuildInitArgument(spec, BuildDynamicResourceSpec("v2"), incarnationId, 2));
    EXPECT_EQ(outcome.Status, ECompanionResourceExecuteStatus::Error);
    EXPECT_THAT(outcome.Error.GetMessage(), testing::HasSubstr("Reconfigure failure"));
    EXPECT_FALSE(Store_->FindInitializedResource(
        MakeReference("resource", incarnationId, 2)));

    TUnittestReconfigurableResource::FailReconfigure = false;
    EXPECT_EQ(
        Execute(
            "resource",
            ECompanionResourceCommand::Init,
            BuildInitArgument(spec, BuildDynamicResourceSpec("v2"), incarnationId, 2))
            .Status,
        ECompanionResourceExecuteStatus::Ok);
    EXPECT_NE(Find("resource"), oldResource);
    EXPECT_EQ(TUnittestReconfigurableResource::ConstructionCount, 2);
    EXPECT_EQ(TUnittestReconfigurableResource::LoadCount, 2);
}

TEST_F(TResourceStoreTest, FailedReconfigureCanHealLastAppliedGeneration)
{
    auto incarnationId = MakeIncarnationId();
    auto spec = BuildResourceSpec(TypeName<TUnittestReconfigurableResource>());
    auto appliedArgument = BuildInitArgument(
        spec,
        BuildDynamicResourceSpec("v1"),
        incarnationId,
        1);
    EXPECT_EQ(
        Execute("resource", ECompanionResourceCommand::Init, appliedArgument).Status,
        ECompanionResourceExecuteStatus::Ok);

    TUnittestReconfigurableResource::FailReconfigure = true;
    EXPECT_EQ(
        Execute(
            "resource",
            ECompanionResourceCommand::Init,
            BuildInitArgument(spec, BuildDynamicResourceSpec("v2"), incarnationId, 2))
            .Status,
        ECompanionResourceExecuteStatus::Error);
    EXPECT_FALSE(Find("resource"));

    TUnittestReconfigurableResource::FailReconfigure = false;
    EXPECT_EQ(
        Execute("resource", ECompanionResourceCommand::Init, appliedArgument).Status,
        ECompanionResourceExecuteStatus::Ok);
    EXPECT_TRUE(Store_->FindInitializedResource(
        MakeReference("resource", incarnationId, 1)));
}

TEST_F(TResourceStoreTest, FailedFirstInitWithRevisionCanHeal)
{
    auto incarnationId = MakeIncarnationId();
    auto spec = BuildResourceSpec(TypeName<TUnittestFlakyResource>());
    // The worker's initial load publishes at configuration generation 0, which
    // matches the entry's reset generation, so the retry takes the
    // same-generation path.
    auto argument = BuildInitArgument(
        spec,
        BuildDynamicResourceSpec(),
        incarnationId,
        /*configurationGeneration*/ 0,
        /*incarnationGeneration*/ 0,
        /*dependencies*/ {},
        BuildResourceRevision(7, "//rev"));

    TUnittestFlakyResource::FailLoad = true;
    EXPECT_EQ(
        Execute("resource", ECompanionResourceCommand::Init, argument).Status,
        ECompanionResourceExecuteStatus::Error);

    // A retry of the identical argument must rebuild: after a failed init
    // there are no applied specs to conflict with.
    TUnittestFlakyResource::FailLoad = false;
    EXPECT_EQ(
        Execute("resource", ECompanionResourceCommand::Init, argument).Status,
        ECompanionResourceExecuteStatus::Ok);
    EXPECT_TRUE(Store_->FindInitializedResource(
        MakeReference("resource", incarnationId, 0)));
}

TEST_F(TResourceStoreTest, UnknownClass)
{
    auto outcome = Execute(
        "dict",
        ECompanionResourceCommand::Init,
        BuildInitArgument(
            BuildResourceSpec("NYT::NFlow::NCompanionServer::TUnknownResource"),
            BuildDynamicResourceSpec()));
    EXPECT_EQ(outcome.Status, ECompanionResourceExecuteStatus::ResourceNotFound);
    EXPECT_THAT(outcome.Error.GetMessage(), testing::HasSubstr("no factory"));
}

TEST_F(TResourceStoreTest, UnknownCommand)
{
    auto outcome = Execute(
        "dict",
        static_cast<ECompanionResourceCommand>(123));
    EXPECT_EQ(outcome.Status, ECompanionResourceExecuteStatus::Unsupported);
    EXPECT_THAT(outcome.Error.GetMessage(), testing::HasSubstr("Unsupported"));
}

TEST_F(TResourceStoreTest, UnloadBeforeInitCreatesTombstone)
{
    auto retiredIncarnationId = MakeIncarnationId();
    auto successorIncarnationId = MakeIncarnationId();
    auto spec = BuildResourceSpec(TypeName<TUnittestDictionaryResource>());

    EXPECT_EQ(
        Execute(
            "ghost",
            ECompanionResourceCommand::Unload,
            BuildUnloadArgument(retiredIncarnationId))
            .Status,
        ECompanionResourceExecuteStatus::Ok);
    EXPECT_EQ(
        Execute(
            "ghost",
            ECompanionResourceCommand::Init,
            BuildInitArgument(spec, BuildDynamicResourceSpec(), retiredIncarnationId))
            .Status,
        ECompanionResourceExecuteStatus::StaleResourceIncarnation);
    EXPECT_EQ(
        Execute(
            "ghost",
            ECompanionResourceCommand::Init,
            BuildInitArgument(
                spec,
                BuildDynamicResourceSpec(),
                successorIncarnationId,
                1,
                1))
            .Status,
        ECompanionResourceExecuteStatus::Ok);
}

TEST_F(TResourceStoreTest, MalformedArgument)
{
    EXPECT_EQ(
        Execute("dict", ECompanionResourceCommand::Init).Status,
        ECompanionResourceExecuteStatus::Error);
    EXPECT_EQ(
        Execute("dict", ECompanionResourceCommand::Init, NYson::TYsonString(TStringBuf("{unknown_key=1}")))
            .Status,
        ECompanionResourceExecuteStatus::Error);
}

TEST_F(TResourceStoreTest, FailedLoadIsRetryable)
{
    auto argument = BuildInitArgument(
        BuildResourceSpec(TypeName<TUnittestFlakyResource>()),
        BuildDynamicResourceSpec());

    TUnittestFlakyResource::FailLoad = true;
    auto outcome = Execute("flaky", ECompanionResourceCommand::Init, argument);
    EXPECT_EQ(outcome.Status, ECompanionResourceExecuteStatus::Error);
    EXPECT_THAT(outcome.Error.GetMessage(), testing::HasSubstr("Flaky load failure"));
    EXPECT_FALSE(Find("flaky"));

    // The idempotent init retried by the worker heals the resource.
    TUnittestFlakyResource::FailLoad = false;
    EXPECT_EQ(Execute("flaky", ECompanionResourceCommand::Init, argument).Status, ECompanionResourceExecuteStatus::Ok);
    EXPECT_TRUE(Find("flaky"));
}

TEST_F(TResourceStoreTest, DependencyAliasResolution)
{
    EXPECT_EQ(
        Execute(
            "dict_a",
            ECompanionResourceCommand::Init,
            BuildInitArgument(
                BuildResourceSpec(TypeName<TUnittestDictionaryResource>()),
                BuildDynamicResourceSpec()))
            .Status,
        ECompanionResourceExecuteStatus::Ok);

    auto spec = BuildResourceSpec(
        TypeName<TUnittestDependentResource>(),
        "//dependent",
        {
            {TResourceId("dict_a"), TResourceId("the_dict")},
            // Worker-only dependency unknown to the store; skipped.
            {TResourceId("CompanionManager"), std::nullopt},
        });
    EXPECT_EQ(
        Execute(
            "dependent",
            ECompanionResourceCommand::Init,
            BuildInitArgument(
                spec,
                BuildDynamicResourceSpec(),
                DefaultIncarnationId,
                1,
                0,
                {MakeReference(
                    "dict_a",
                    DefaultIncarnationId,
                    1,
                    TResourceId("the_dict"))}))
            .Status,
        ECompanionResourceExecuteStatus::Ok);

    const auto& dependencies = TUnittestDependentResource::LastDependencies;
    ASSERT_EQ(std::ssize(dependencies), 1);
    EXPECT_EQ(GetOrCrash(dependencies, TResourceId("the_dict")), Find("dict_a"));
}

TEST_F(TResourceStoreTest, MissingAndStaleDependenciesAreRejected)
{
    auto dependencyIncarnationId = MakeIncarnationId();
    auto dependentIncarnationId = MakeIncarnationId();
    auto dependentSpec = BuildResourceSpec(TypeName<TUnittestDependentResource>());

    auto missing = Execute(
        "dependent",
        ECompanionResourceCommand::Init,
        BuildInitArgument(
            dependentSpec,
            BuildDynamicResourceSpec(),
            dependentIncarnationId,
            1,
            0,
            {MakeReference("dict", dependencyIncarnationId, 1)}));
    EXPECT_EQ(missing.Status, ECompanionResourceExecuteStatus::ResourceNotInitialized);

    EXPECT_EQ(
        Execute(
            "dict",
            ECompanionResourceCommand::Init,
            BuildInitArgument(
                BuildResourceSpec(TypeName<TUnittestDictionaryResource>()),
                BuildDynamicResourceSpec(),
                dependencyIncarnationId,
                1))
            .Status,
        ECompanionResourceExecuteStatus::Ok);

    auto stale = Execute(
        "dependent",
        ECompanionResourceCommand::Init,
        BuildInitArgument(
            dependentSpec,
            BuildDynamicResourceSpec(),
            dependentIncarnationId,
            1,
            0,
            {MakeReference("dict", dependencyIncarnationId, 2)}));
    EXPECT_EQ(stale.Status, ECompanionResourceExecuteStatus::ResourceNotInitialized);
}

TEST_F(TResourceStoreTest, MissingReplacementDependencyUnpublishesOldDependent)
{
    auto firstDependencyIncarnationId = MakeIncarnationId();
    auto missingDependencyIncarnationId = MakeIncarnationId();
    auto dependentIncarnationId = MakeIncarnationId();
    auto dictionarySpec = BuildResourceSpec(TypeName<TUnittestDictionaryResource>());
    auto dependentSpec = BuildResourceSpec(TypeName<TUnittestDependentResource>());

    EXPECT_EQ(
        Execute(
            "dict",
            ECompanionResourceCommand::Init,
            BuildInitArgument(
                dictionarySpec,
                BuildDynamicResourceSpec(),
                firstDependencyIncarnationId))
            .Status,
        ECompanionResourceExecuteStatus::Ok);
    EXPECT_EQ(
        Execute(
            "dependent",
            ECompanionResourceCommand::Init,
            BuildInitArgument(
                dependentSpec,
                BuildDynamicResourceSpec("v1"),
                dependentIncarnationId,
                1,
                0,
                {MakeReference("dict", firstDependencyIncarnationId, 1)}))
            .Status,
        ECompanionResourceExecuteStatus::Ok);
    auto oldDependent = Find("dependent");
    ASSERT_TRUE(oldDependent);

    auto replacementArgument = BuildInitArgument(
        dependentSpec,
        BuildDynamicResourceSpec("v2"),
        dependentIncarnationId,
        2,
        0,
        {MakeReference("dict", missingDependencyIncarnationId, 1)});
    EXPECT_EQ(
        Execute("dependent", ECompanionResourceCommand::Init, replacementArgument).Status,
        ECompanionResourceExecuteStatus::ResourceNotInitialized);
    EXPECT_FALSE(Find("dependent"));
    EXPECT_FALSE(Store_->FindInitializedResource(
        MakeReference("dependent", dependentIncarnationId, 1)));
    EXPECT_FALSE(Store_->FindInitializedResource(
        MakeReference("dependent", dependentIncarnationId, 2)));

    EXPECT_EQ(
        Execute(
            "dict",
            ECompanionResourceCommand::Init,
            BuildInitArgument(
                dictionarySpec,
                BuildDynamicResourceSpec(),
                missingDependencyIncarnationId,
                1,
                1))
            .Status,
        ECompanionResourceExecuteStatus::Ok);
    EXPECT_EQ(
        Execute("dependent", ECompanionResourceCommand::Init, replacementArgument).Status,
        ECompanionResourceExecuteStatus::Ok);
    EXPECT_NE(Find("dependent"), oldDependent);
}

TEST_F(TResourceStoreTest, DependencyReferenceChangeRecreatesDependent)
{
    auto firstDependencyIncarnationId = MakeIncarnationId();
    auto successorDependencyIncarnationId = MakeIncarnationId();
    auto dependentIncarnationId = MakeIncarnationId();
    auto dictionarySpec = BuildResourceSpec(TypeName<TUnittestDictionaryResource>());
    auto dependentSpec = BuildResourceSpec(TypeName<TUnittestDependentResource>());

    EXPECT_EQ(
        Execute(
            "dict",
            ECompanionResourceCommand::Init,
            BuildInitArgument(
                dictionarySpec,
                BuildDynamicResourceSpec(),
                firstDependencyIncarnationId))
            .Status,
        ECompanionResourceExecuteStatus::Ok);
    EXPECT_EQ(
        Execute(
            "dependent",
            ECompanionResourceCommand::Init,
            BuildInitArgument(
                dependentSpec,
                BuildDynamicResourceSpec(),
                dependentIncarnationId,
                1,
                0,
                {MakeReference(
                    "dict",
                    firstDependencyIncarnationId,
                    1,
                    TResourceId("dictionary"))}))
            .Status,
        ECompanionResourceExecuteStatus::Ok);
    auto oldDependent = Find("dependent");
    auto oldDependency = GetOrCrash(
        TUnittestDependentResource::LastDependencies,
        TResourceId("dictionary"));

    EXPECT_EQ(
        Execute(
            "dict",
            ECompanionResourceCommand::Init,
            BuildInitArgument(
                dictionarySpec,
                BuildDynamicResourceSpec(),
                successorDependencyIncarnationId,
                1,
                1))
            .Status,
        ECompanionResourceExecuteStatus::Ok);
    EXPECT_EQ(
        Execute(
            "dependent",
            ECompanionResourceCommand::Init,
            BuildInitArgument(
                dependentSpec,
                BuildDynamicResourceSpec(),
                dependentIncarnationId,
                1,
                0,
                {MakeReference(
                    "dict",
                    successorDependencyIncarnationId,
                    1,
                    TResourceId("dictionary"))}))
            .Status,
        ECompanionResourceExecuteStatus::Ok);

    EXPECT_NE(Find("dependent"), oldDependent);
    EXPECT_NE(
        GetOrCrash(TUnittestDependentResource::LastDependencies, TResourceId("dictionary")),
        oldDependency);
}

TEST_F(TResourceStoreTest, ConcurrentInitLoadsOnce)
{
    auto gate = NewPromise<void>();
    auto loadStarted = NewPromise<void>();
    TUnittestGatedResource::Gate = gate.ToFuture();
    TUnittestGatedResource::LoadStarted = loadStarted;

    auto argument = BuildInitArgument(
        BuildResourceSpec(TypeName<TUnittestGatedResource>()),
        BuildDynamicResourceSpec());
    auto first = Store_->Execute(TResourceId("gated"), ECompanionResourceCommand::Init, argument);
    auto second = Store_->Execute(TResourceId("gated"), ECompanionResourceCommand::Init, argument);

    // The load is in progress: readers must observe the uninitialized state,
    // and the concurrent init must be queued, not started.
    loadStarted.ToFuture().BlockingGet().ThrowOnError();
    EXPECT_FALSE(Find("gated"));
    EXPECT_FALSE(first.IsSet());
    EXPECT_FALSE(second.IsSet());
    EXPECT_EQ(TUnittestGatedResource::LoadCount, 1);

    gate.Set();
    EXPECT_EQ(first.BlockingGet().ValueOrThrow().Status, ECompanionResourceExecuteStatus::Ok);
    // The queued second init observes the initialized state and no-ops.
    EXPECT_EQ(second.BlockingGet().ValueOrThrow().Status, ECompanionResourceExecuteStatus::Ok);
    EXPECT_EQ(TUnittestGatedResource::LoadCount, 1);
    EXPECT_TRUE(Store_->FindInitializedResource(
        MakeReference("gated", DefaultIncarnationId, 1)));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TCompanionRuntimeInitContextResourcesTest, WithPrefixPreservesResourceLookup)
{
    auto store = New<TCompanionStateStore>(
        THashSet<std::string>{},
        THashSet<std::string>{},
        THashSet<std::string>{},
        NTesting::DefaultTestKeySchema());

    class TBareResource
        : public IResource
    {
    public:
        TFuture<void> Load(const THashMap<TResourceId, IResourcePtr>& /*dependencies*/) override
        {
            return OKFuture;
        }

        void Reconfigure(const TDynamicResourceContextPtr& /*dynamicContext*/) override
        { }

        TResourceRevisionState GetRevisionState() const override
        {
            return {};
        }

        TParametersPtr GetParametersBase() const override
        {
            return nullptr;
        }

        TDynamicParametersPtr GetDynamicParametersBase() const override
        {
            return nullptr;
        }
    };

    auto resource = New<TBareResource>();

    auto initContext = New<TCompanionRuntimeInitContext>(
        store,
        /*parametersNode*/ nullptr,
        THashMap<TResourceId, IResourcePtr>{{TResourceId("the_dict"), resource}});

    EXPECT_EQ(initContext->GetStaticResource(TResourceId("the_dict")), resource);
    EXPECT_EQ(
        initContext->WithPrefix("sub")->GetStaticResource(TResourceId("the_dict")),
        resource);
    EXPECT_THROW_WITH_SUBSTRING(
        Y_UNUSED(initContext->GetStaticResource(TResourceId("ghost"))),
        "required_resource_ids");
}

////////////////////////////////////////////////////////////////////////////////

class TResourceServiceTest
    : public ::testing::Test
{
protected:
    ::NTesting::TPortHolder Port_;
    TCompanionServerPtr Server_;
    std::optional<NCompanion::TCompanionProxy> Proxy_;

    NTableClient::TTableSchemaPtr Schema_ = NTesting::DefaultTestKeySchema();
    //! A distinct schema object: TStreamSpecs requires unique schema pointers per stream.
    NTableClient::TTableSchemaPtr OutputSchema_ = New<NTableClient::TTableSchema>(Schema_->Columns());
    TStreamSpecsPtr StreamSpecs_;
    TJobId JobId_ = TJobId(TGuid::Create());
    TResourceInstanceId ResourceIncarnationId_ = MakeIncarnationId();
    ui64 ResourceConfigurationGeneration_ = 1;

    void SetUp() override
    {
        Port_ = ::NTesting::GetFreePort();

        auto config = New<NCompanion::TCompanionExecutionConfig>();
        config->Port = Port_;

        TPipeline pipeline;
        pipeline.AddTransform<TUnittestResourceConsumerFunction>("my_computation");
        pipeline.AddResource<TUnittestDictionaryResource>();

        Server_ = New<TCompanionServer>(config, pipeline);
        Server_->Start();
        Proxy_.emplace(NCompanion::CreateCompanionProxy(
            Format("localhost:%v", static_cast<int>(Port_))));

        THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> streamSpecMap;
        for (const auto& [streamId, specId, schema] : {
                std::tuple{TStreamId("input"), TStreamSpecId(1), Schema_},
                std::tuple{TStreamId("output"), TStreamSpecId(2), OutputSchema_}})
        {
            auto streamSpec = New<TStreamSpec>();
            streamSpec->Schema = schema;
            streamSpecMap[streamId][specId] = std::move(streamSpec);
        }
        StreamSpecs_ = New<TStreamSpecs>(streamSpecMap);

        TUnittestDictionaryResource::LoadCount = 0;
        TUnittestResourceConsumerFunction::ProcessCalled = false;
        TUnittestResourceConsumerFunction::ProcessCount = 0;
        TUnittestResourceConsumerFunction::ResourceIdWasVisible = false;
        TUnittestResourceConsumerFunction::ObservedPath.clear();
        TUnittestResourceConsumerFunction::ProcessStarted = {};
        TUnittestResourceConsumerFunction::ProcessGate = {};
    }

    void TearDown() override
    {
        Server_->Stop();
    }

    auto BuildProcessBatchRequest()
    {
        auto req = Proxy_->ProcessBatch();
        ToProto(req->mutable_request_id(), TGuid::Create());
        ToProto(req->mutable_job_id(), JobId_);
        req->set_computation_id("my_computation");

        auto* jobInfo = req->mutable_job_info();
        jobInfo->set_spec(Format(R"({
                computation_class_name = "NYT::NFlow::NCompanion::TTransformCompanionComputation";
                processing_function = %Qv;
                group_by_schema = [{name = "key"; type = "uint64"}];
                input_stream_ids = ["input"];
                output_stream_ids = ["output"];
                required_resource_ids = {
                    my_dictionary = {worker = %%true; alias = dictionary_alias};
                };
            })",
            TypeName<TUnittestResourceConsumerFunction>()));
        jobInfo->set_dynamic_spec("{}");
        for (const auto& [streamId, specId, schema] : {
                std::tuple{TStreamId("input"), TStreamSpecId(1), Schema_},
                std::tuple{TStreamId("output"), TStreamSpecId(2), OutputSchema_}})
        {
            auto* stream = jobInfo->add_streams();
            stream->set_stream_id(ToProto<TProtobufString>(streamId));
            stream->set_stream_spec_id(specId.Underlying());
            stream->set_schema(ToProto(NYson::ConvertToYsonString(schema)));
        }
        auto* reference = jobInfo->add_companion_resources();
        reference->set_resource_id("my_dictionary");
        ToProto(reference->mutable_incarnation_id(), ResourceIncarnationId_.Underlying());
        reference->set_configuration_generation(ResourceConfigurationGeneration_);
        reference->set_alias("dictionary_alias");

        auto message = NTesting::MakeTestMessage(
            TStreamId("input"),
            MakeKey(ui64{7}),
            Schema_,
            [&] (TMessageBuilder& builder) {
                builder.SetMessageId(TMessageId("m1"));
                builder.Payload().Set(ui64{7}, "key");
            });
        auto* protoMessage = req->add_messages();
        ToProto(protoMessage->mutable_message(), *message, StreamSpecs_);
        ToProto(protoMessage->mutable_key(), message->Key);
        return req;
    }

    auto ResourceExecute(
        ECompanionResourceCommand command,
        const NYson::TYsonString& argument = {})
    {
        auto req = Proxy_->ResourceExecute();
        ToProto(req->mutable_request_id(), TGuid::Create());
        req->set_resource_id("my_dictionary");
        req->set_command(static_cast<NProto::NCompanion::EResourceCommand>(command));
        if (argument) {
            req->set_argument(argument.ToString());
        }
        return req->Invoke().BlockingGet().ValueOrThrow();
    }

    NCompanion::TCompanionInfoPtr FetchCompanionInfo()
    {
        auto rsp = Proxy_->CompanionInfo()->Invoke().BlockingGet().ValueOrThrow();
        return ConvertTo<NCompanion::TCompanionInfoPtr>(NYson::TYsonString(TString(rsp->payload())));
    }
};

TEST_F(TResourceServiceTest, ProcessBatchPreCheckAndHealing)
{
    {
        // The pre-check rejects the batch before any user code runs.
        auto rsp = BuildProcessBatchRequest()->Invoke().BlockingGet().ValueOrThrow();
        EXPECT_EQ(rsp->status(), NProto::NCompanion::RS_RESOURCE_NOT_INITIALIZED);
        EXPECT_FALSE(TUnittestResourceConsumerFunction::ProcessCalled);
        EXPECT_TRUE(TUnittestResourceConsumerFunction::ObservedPath.empty());
    }

    {
        auto rsp = ResourceExecute(
            ECompanionResourceCommand::Init,
            BuildInitArgument(
                BuildResourceSpec(TypeName<TUnittestDictionaryResource>(), "//home/dicts/geo"),
                BuildDynamicResourceSpec(),
                ResourceIncarnationId_));
        EXPECT_EQ(rsp->status(), NProto::NCompanion::RES_OK);
        EXPECT_FALSE(rsp->has_error());
    }

    {
        auto rsp = BuildProcessBatchRequest()->Invoke().BlockingGet().ValueOrThrow();
        ASSERT_EQ(rsp->status(), NProto::NCompanion::RS_OK);
        EXPECT_TRUE(TUnittestResourceConsumerFunction::ProcessCalled);
        // GetStaticResource resolved the companion-hosted instance with the
        // parameters parsed against the companion-side class.
        EXPECT_EQ(TUnittestResourceConsumerFunction::ObservedPath, "//home/dicts/geo");
        EXPECT_FALSE(TUnittestResourceConsumerFunction::ResourceIdWasVisible);
        EXPECT_EQ(rsp->data().output_size(), 1);
    }
}

TEST_F(TResourceServiceTest, StaleIncarnationStatusIsSerialized)
{
    auto predecessorIncarnationId = MakeIncarnationId();
    EXPECT_EQ(
        ResourceExecute(
            ECompanionResourceCommand::Init,
            BuildInitArgument(
                BuildResourceSpec(TypeName<TUnittestDictionaryResource>()),
                BuildDynamicResourceSpec(),
                ResourceIncarnationId_,
                1,
                1))
            ->status(),
        NProto::NCompanion::RES_OK);

    auto response = ResourceExecute(
        ECompanionResourceCommand::Init,
        BuildInitArgument(
            BuildResourceSpec(TypeName<TUnittestDictionaryResource>()),
            BuildDynamicResourceSpec(),
            predecessorIncarnationId));
    EXPECT_EQ(response->status(), NProto::NCompanion::RES_STALE_RESOURCE_INCARNATION);
    ASSERT_TRUE(response->has_error());
    EXPECT_THAT(FromProto<TError>(response->error()).GetMessage(), testing::HasSubstr("stale"));
}

TEST_F(TResourceServiceTest, JobIsReboundAfterConfigurationGenerationChanges)
{
    auto spec = BuildResourceSpec(TypeName<TUnittestDictionaryResource>());
    EXPECT_EQ(
        ResourceExecute(
            ECompanionResourceCommand::Init,
            BuildInitArgument(
                spec,
                BuildDynamicResourceSpec("v1"),
                ResourceIncarnationId_,
                1))
            ->status(),
        NProto::NCompanion::RES_OK);
    EXPECT_EQ(
        BuildProcessBatchRequest()->Invoke().BlockingGet().ValueOrThrow()->status(),
        NProto::NCompanion::RS_OK);

    EXPECT_EQ(
        ResourceExecute(
            ECompanionResourceCommand::Init,
            BuildInitArgument(
                spec,
                BuildDynamicResourceSpec("v2"),
                ResourceIncarnationId_,
                2))
            ->status(),
        NProto::NCompanion::RES_OK);

    EXPECT_EQ(
        BuildProcessBatchRequest()->Invoke().BlockingGet().ValueOrThrow()->status(),
        NProto::NCompanion::RS_RESOURCE_NOT_INITIALIZED);

    ResourceConfigurationGeneration_ = 2;
    EXPECT_EQ(
        BuildProcessBatchRequest()->Invoke().BlockingGet().ValueOrThrow()->status(),
        NProto::NCompanion::RS_OK);
}

TEST_F(TResourceServiceTest, QueuedBatchRevalidatesResourceGeneration)
{
    EXPECT_EQ(
        ResourceExecute(
            ECompanionResourceCommand::Init,
            BuildInitArgument(
                BuildResourceSpec(TypeName<TUnittestDictionaryResource>()),
                BuildDynamicResourceSpec("v1"),
                ResourceIncarnationId_,
                1))
            ->status(),
        NProto::NCompanion::RES_OK);

    auto processStarted = NewPromise<void>();
    auto processGate = NewPromise<void>();
    TUnittestResourceConsumerFunction::ProcessStarted = processStarted;
    TUnittestResourceConsumerFunction::ProcessGate = processGate.ToFuture();

    auto firstResponse = BuildProcessBatchRequest()->Invoke();
    processStarted.ToFuture().BlockingGet().ThrowOnError();

    auto secondRequest = BuildProcessBatchRequest();
    secondRequest->clear_job_info();
    auto secondResponse = secondRequest->Invoke();
    NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(10));
    EXPECT_FALSE(secondResponse.IsSet());

    EXPECT_EQ(
        ResourceExecute(
            ECompanionResourceCommand::Init,
            BuildInitArgument(
                BuildResourceSpec(TypeName<TUnittestDictionaryResource>()),
                BuildDynamicResourceSpec("v2"),
                ResourceIncarnationId_,
                2))
            ->status(),
        NProto::NCompanion::RES_OK);

    processGate.Set();
    EXPECT_EQ(
        firstResponse.BlockingGet().ValueOrThrow()->status(),
        NProto::NCompanion::RS_OK);
    EXPECT_EQ(
        secondResponse.BlockingGet().ValueOrThrow()->status(),
        NProto::NCompanion::RS_RESOURCE_NOT_INITIALIZED);
    EXPECT_EQ(1, TUnittestResourceConsumerFunction::ProcessCount);
}

TEST_F(TResourceServiceTest, JobIsReboundAfterIncarnationChanges)
{
    auto firstIncarnationId = ResourceIncarnationId_;
    auto successorIncarnationId = MakeIncarnationId();
    EXPECT_EQ(
        ResourceExecute(
            ECompanionResourceCommand::Init,
            BuildInitArgument(
                BuildResourceSpec(TypeName<TUnittestDictionaryResource>(), "//first"),
                BuildDynamicResourceSpec(),
                firstIncarnationId))
            ->status(),
        NProto::NCompanion::RES_OK);
    EXPECT_EQ(
        BuildProcessBatchRequest()->Invoke().BlockingGet().ValueOrThrow()->status(),
        NProto::NCompanion::RS_OK);
    EXPECT_EQ(TUnittestResourceConsumerFunction::ObservedPath, "//first");

    EXPECT_EQ(
        ResourceExecute(
            ECompanionResourceCommand::Init,
            BuildInitArgument(
                BuildResourceSpec(TypeName<TUnittestDictionaryResource>(), "//successor"),
                BuildDynamicResourceSpec(),
                successorIncarnationId,
                1,
                1))
            ->status(),
        NProto::NCompanion::RES_OK);
    EXPECT_EQ(
        BuildProcessBatchRequest()->Invoke().BlockingGet().ValueOrThrow()->status(),
        NProto::NCompanion::RS_RESOURCE_NOT_INITIALIZED);

    ResourceIncarnationId_ = successorIncarnationId;
    EXPECT_EQ(
        BuildProcessBatchRequest()->Invoke().BlockingGet().ValueOrThrow()->status(),
        NProto::NCompanion::RS_OK);
    EXPECT_EQ(TUnittestResourceConsumerFunction::ObservedPath, "//successor");
}

TEST_F(TResourceServiceTest, CompanionInfoExposesStableMetadata)
{
    auto info = FetchCompanionInfo();
    ASSERT_TRUE(info->ProcessId);
    EXPECT_GT(*info->ProcessId, 0);
    EXPECT_TRUE(info->Computations.contains(TComputationId("my_computation")));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanionServer
