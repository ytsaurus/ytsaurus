#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/companion/server/pipeline.h>

#include <yt/yt/flow/library/cpp/common/process_function.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/system/getpid.h>
#include <util/system/type_name.h>

namespace NYT::NFlow::NCompanionServer {
namespace {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TPipelineUnittestFunction
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& /*message*/,
        const IOutputCollectorPtr& /*output*/,
        const IRuntimeContextPtr& /*context*/) override
    { }
};

// Used only by DuplicateFunctionRegistrationThrows, so the conflicting
// registration below cannot affect the registry entry other tests rely on.
class TDuplicateParamsFunction
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& /*message*/,
        const IOutputCollectorPtr& /*output*/,
        const IRuntimeContextPtr& /*context*/) override
    { }
};

struct TAlternativeParameters
    : public NYTree::TYsonStruct
{
    REGISTER_YSON_STRUCT(TAlternativeParameters);

    static void Register(TRegistrar /*registrar*/)
    { }
};

TEST(TPipelineTest, RegisterAndBuildInfo)
{
    TPipeline pipeline;
    pipeline.AddTransform<TPipelineUnittestFunction>("my_transform");
    pipeline.AddSource<TPipelineUnittestFunction>("my_source");

    EXPECT_TRUE(pipeline.HasComputation("my_transform"));
    EXPECT_TRUE(pipeline.HasComputation("my_source"));
    EXPECT_FALSE(pipeline.HasComputation("unknown"));

    auto info = pipeline.BuildCompanionInfo();
    ASSERT_EQ(std::ssize(info->Computations), 2);
    EXPECT_EQ(
        info->Computations["my_transform"]->CompanionComputationType,
        ECompanionComputationType::Transform);
    EXPECT_EQ(
        info->Computations["my_source"]->CompanionComputationType,
        ECompanionComputationType::Source);
}

TEST(TPipelineTest, TypedDeclarationRegistersFunction)
{
    // Declaring through the typed API registers the function; repeated
    // declarations (across computations and pipeline instances) register once.
    TPipeline first;
    first.AddTransform<TPipelineUnittestFunction>("t1");
    first.AddSource<TPipelineUnittestFunction>("s1");
    TPipeline second;
    second.AddTransform<TPipelineUnittestFunction>("t1");

    auto function = TRegistry::Get()->CreateProcessFunction(
        TypeName<TPipelineUnittestFunction>());
    EXPECT_TRUE(function);
}

TEST(TPipelineTest, DuplicateFunctionRegistrationThrows)
{
    TPipeline pipeline;
    pipeline.AddTransform<TDuplicateParamsFunction>("first");
    // Same function with different parameter types is a different template
    // instantiation, so the once-only guard does not apply; the registry must
    // report the conflict with a catchable error.
    EXPECT_THROW_WITH_SUBSTRING(
        (pipeline.AddTransform<TDuplicateParamsFunction, TAlternativeParameters>("second")),
        "already registered");
}

TEST(TPipelineTest, DuplicateIdThrows)
{
    TPipeline pipeline;
    pipeline.AddTransform<TPipelineUnittestFunction>("computation");
    EXPECT_THROW_WITH_SUBSTRING(
        pipeline.AddSource<TPipelineUnittestFunction>("computation"),
        "already registered");
}

TEST(TPipelineTest, PayloadRoundTripsThroughCompanionInfoParser)
{
    TPipeline pipeline;
    pipeline.AddTransform<TPipelineUnittestFunction>("my_transform");

    auto payload = pipeline.BuildCompanionInfoPayload();

    // The worker-side client parses the payload with ConvertTo<TCompanionInfoPtr>;
    // an extra top-level key must not break it.
    auto parsed = ConvertTo<NCompanion::TCompanionInfoPtr>(payload);
    ASSERT_EQ(std::ssize(parsed->Computations), 1);
    EXPECT_EQ(
        parsed->Computations["my_transform"]->CompanionComputationType,
        ECompanionComputationType::Transform);

    auto node = ConvertTo<IMapNodePtr>(payload);
    EXPECT_EQ(node->GetChildOrThrow("pid")->AsInt64()->GetValue(), static_cast<i64>(GetPID()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanionServer
