#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/companion/manager/companion_manager.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow::NCompanion {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TCompanionManagerParametersTest, JobReconciliationPeriodDefaultsAndRejectsNonPositive)
{
    auto params = New<TCompanionManagerParameters>();
    EXPECT_EQ(params->JobReconciliationPeriod, TDuration::Seconds(15));

    for (const auto* yson : {R"({"job_reconciliation_period" = 0;})", R"({"job_reconciliation_period" = -1;})"}) {
        EXPECT_ANY_THROW(NYTree::ConvertTo<TCompanionManagerParametersPtr>(NYson::TYsonString(TStringBuf(yson))));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanion
