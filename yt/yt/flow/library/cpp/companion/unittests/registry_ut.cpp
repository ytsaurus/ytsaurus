#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/companion/companion_computation_base.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow::NCompanion {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TCompanionRegistryTest, ProductionTypesAreRegistered)
{
    const auto computationTypeNames = TRegistry::Get()->GetComputationTypeNames();
    EXPECT_THAT(computationTypeNames, testing::Contains("NYT::NFlow::NCompanion::TSwiftMapCompanionComputation"));
    EXPECT_THAT(computationTypeNames, testing::Contains("NYT::NFlow::NCompanion::TSwiftOrderedSourceCompanionComputation"));
    EXPECT_THAT(computationTypeNames, testing::Contains("NYT::NFlow::NCompanion::TTransformCompanionComputation"));
    EXPECT_THAT(computationTypeNames, testing::Contains("NYT::NFlow::NCompanion::TTransformOrderedSourceCompanionComputation"));

    const auto resourceTypeNames = TRegistry::Get()->GetResourceTypeNames();
    EXPECT_THAT(resourceTypeNames, testing::Contains("NYT::NFlow::NCompanion::TCompanionManager"));
    EXPECT_THAT(resourceTypeNames, testing::Contains("NYT::NFlow::NCompanion::TCompanionResource"));
    EXPECT_THAT(resourceTypeNames, testing::Contains("NYT::NFlow::NCompanion::TJavaCompanionManager"));
}

////////////////////////////////////////////////////////////////////////////////

const std::vector<std::string> CompanionComputationClasses{
    "NYT::NFlow::NCompanion::TTransformCompanionComputation",
    "NYT::NFlow::NCompanion::TSwiftMapCompanionComputation",
    "NYT::NFlow::NCompanion::TSwiftOrderedSourceCompanionComputation",
    "NYT::NFlow::NCompanion::TTransformOrderedSourceCompanionComputation",
};

NYTree::TYsonStructPtr ParseCompanionParameters(TStringBuf computationClass, TStringBuf parameters)
{
    auto spec = New<TComputationSpec>();
    spec->ComputationClassName = computationClass;
    spec->Parameters = NYTree::ConvertTo<NYTree::IMapNodePtr>(NYson::TYsonStringBuf(parameters));
    return TRegistry::Get()->ParseComputationParameters(spec);
}

TEST(TCompanionRegistryTest, FunctionIdsAreRecognized)
{
    for (const auto& computationClass : CompanionComputationClasses) {
        SCOPED_TRACE(computationClass);
        auto parameters = DynamicPointerCast<TCompanionFunctionParameters>(
            ParseCompanionParameters(computationClass, "{function_ids=[Count; Format];}"));
        ASSERT_TRUE(parameters);
        ASSERT_TRUE(parameters->FunctionIds);
        EXPECT_THAT(*parameters->FunctionIds, testing::ElementsAre("Count", "Format"));

        auto defaults = DynamicPointerCast<TCompanionFunctionParameters>(
            ParseCompanionParameters(computationClass, "{}"));
        ASSERT_TRUE(defaults);
        EXPECT_FALSE(defaults->FunctionIds);
    }
}

TEST(TCompanionRegistryTest, RejectsEmptyOrDuplicatedFunctionIds)
{
    for (const auto& computationClass : CompanionComputationClasses) {
        SCOPED_TRACE(computationClass);
        EXPECT_THROW_WITH_SUBSTRING(
            ParseCompanionParameters(computationClass, "{function_ids=[];}"),
            "\"function_ids\" cannot be empty");
        EXPECT_THROW_WITH_SUBSTRING(
            ParseCompanionParameters(computationClass, "{function_ids=[Count; Format; Count];}"),
            "lists function \"Count\" more than once");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanion
