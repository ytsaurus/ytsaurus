#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

const std::vector<std::string> PublicSourceTypeNames = {
    "NYT::NFlow::TYdbSource",
    "NYT::NFlow::TYdbMultiLineSource",
    "NYT::NFlow::TYdbFramingSource",
};

const std::vector<std::string> PublicSinkTypeNames = {
    "NYT::NFlow::TYdbSink",
    "NYT::NFlow::TYdbFramingSink",
    "NYT::NFlow::TYdbTwoPhaseCommitSink",
    "NYT::NFlow::TYdbAtLeastOnceSink",
    "NYT::NFlow::TYdbAtLeastOnceFramingSink",
};

TEST(TExtensionRegistryTest, PublicYdbFamilyIsRegisteredInEveryEdition)
{
    EXPECT_THAT(TRegistry::Get()->GetSourceTypeNames(), testing::IsSupersetOf(PublicSourceTypeNames));
    EXPECT_THAT(TRegistry::Get()->GetSinkTypeNames(), testing::IsSupersetOf(PublicSinkTypeNames));
    EXPECT_THAT(TRegistry::Get()->GetResourceTypeNames(), testing::Contains("NYT::NFlow::TYdbTopicDriver"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
