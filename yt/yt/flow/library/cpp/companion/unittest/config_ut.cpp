#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/flow/library/cpp/companion/config.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow::NCompanion {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TCompanionConfigTest, CompanionProcessCountDefaultsToAuto)
{
    auto config = New<TCompanionConfig>();
    EXPECT_EQ(0, config->CompanionProcessCount);
}

TEST(TCompanionConfigTest, CompanionProcessCountParses)
{
    auto yson = NYson::TYsonString(TStringBuf(R"({
        "companion_process_count" = 4;
    })"));
    auto config = NYTree::ConvertTo<TCompanionConfigPtr>(yson);
    EXPECT_EQ(4, config->CompanionProcessCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanion
