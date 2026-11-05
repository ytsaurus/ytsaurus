#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/companion/companion_computation_base.h>
#include <yt/yt/flow/library/cpp/companion/transform_companion_computation.h>

#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow::NCompanion {
namespace {

////////////////////////////////////////////////////////////////////////////////

TComputationSpecPtr MakeSpecWithExternalManager(bool autoPreload)
{
    auto managerSpec = New<TExternalStateManagerSpec>();
    managerSpec->ExternalStateManagerClassName = "NYT::NFlow::TSimpleExternalStateManager";
    managerSpec->AutoPreload = autoPreload;

    auto spec = New<TComputationSpec>();
    spec->ComputationClassName = "NYT::NFlow::NCompanion::TTransformCompanionComputation";
    // The base TTransformComputation validator wants an input stream.
    spec->InputStreamIds = {TStreamId("in")};
    spec->ExternalStateManagers["/ext"] = std::move(managerSpec);
    return spec;
}

////////////////////////////////////////////////////////////////////////////////

// Companion states travel with the batch, so a manager the framework does not preload cannot work.
TEST(TCompanionSpecValidationTest, RejectsManualPreloadExternalManager)
{
    auto spec = MakeSpecWithExternalManager(/*autoPreload*/ false);

    EXPECT_THROW_WITH_SUBSTRING(
        TTransformCompanionComputation::TValidator::Validate(*spec),
        "\"/ext\" has auto_preload disabled");
}

TEST(TCompanionSpecValidationTest, AcceptsAutoPreloadExternalManager)
{
    auto spec = MakeSpecWithExternalManager(/*autoPreload*/ true);

    EXPECT_NO_THROW(TTransformCompanionComputation::TValidator::Validate(*spec));
}

////////////////////////////////////////////////////////////////////////////////

TCompanionComputationInfoPtr ParseComputationInfo(TStringBuf info)
{
    return NYTree::ConvertTo<TCompanionComputationInfoPtr>(NYson::TYsonStringBuf(info));
}

// An SDK that does not advertise the capability would silently ignore the parameter.
TEST(TCompanionSpecValidationTest, RejectsFunctionIdsUnlessCompanionResolvesThem)
{
    auto parameters = New<TCompanionFunctionParameters>();
    parameters->FunctionIds = std::vector<std::string>{"Count", "Format"};

    EXPECT_THROW_WITH_SUBSTRING(
        ValidateCompanionFunctionIds(*parameters, *ParseComputationInfo("{computation_id=Count;}")),
        "Companion does not support \"function_ids\"");
    EXPECT_NO_THROW(ValidateCompanionFunctionIds(
        *parameters,
        *ParseComputationInfo("{computation_id=Count; supports_function_ids=%true;}")));
}

TEST(TCompanionSpecValidationTest, AcceptsAbsentFunctionIdsWithAnyCompanion)
{
    auto parameters = New<TCompanionFunctionParameters>();

    EXPECT_NO_THROW(ValidateCompanionFunctionIds(*parameters, *ParseComputationInfo("{computation_id=Count;}")));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanion
