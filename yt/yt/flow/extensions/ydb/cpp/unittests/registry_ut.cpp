#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TYdbRegistryTest, PublicTypesAreRegistered)
{
    EXPECT_THAT(
        TRegistry::Get()->GetSourceTypeNames(),
        testing::IsSupersetOf({
            "NYT::NFlow::TYdbSource",
            "NYT::NFlow::TYdbMultiLineSource",
            "NYT::NFlow::TYdbFramingSource",
        }));
    EXPECT_THAT(
        TRegistry::Get()->GetSinkTypeNames(),
        testing::IsSupersetOf({
            "NYT::NFlow::TYdbSink",
            "NYT::NFlow::TYdbFramingSink",
            "NYT::NFlow::TYdbTwoPhaseCommitSink",
            "NYT::NFlow::TYdbAtLeastOnceSink",
            "NYT::NFlow::TYdbAtLeastOnceFramingSink",
        }));
    EXPECT_THAT(TRegistry::Get()->GetResourceTypeNames(), testing::Contains("NYT::NFlow::TYdbTopicDriver"));
}

TEST(TYdbRegistryTest, PublicSourcesAreReplayStable)
{
    for (const auto* className : {
            "NYT::NFlow::TYdbSource",
            "NYT::NFlow::TYdbFramingSource",
            "NYT::NFlow::TYdbMultiLineSource",
         })
    {
        EXPECT_TRUE(TRegistry::Get()->IsSourceReplayStable(className)) << className;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
