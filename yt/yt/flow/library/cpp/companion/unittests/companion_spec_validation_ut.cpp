#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/companion/transform_companion_computation.h>

#include <yt/yt/flow/library/cpp/common/spec.h>

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

} // namespace
} // namespace NYT::NFlow::NCompanion
