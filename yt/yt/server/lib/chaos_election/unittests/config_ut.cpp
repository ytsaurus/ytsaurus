#include <yt/yt/server/lib/chaos_election/config.h>

#include <yt/yt/library/cypress_election/config.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/polymorphic_yson_struct.h>

namespace NYT::NChaosElection {
namespace {

////////////////////////////////////////////////////////////////////////////////

DEFINE_POLYMORPHIC_YSON_STRUCT_WITH_DEFAULT(
    ElectionConfig,
    Cypress,
    NLockElection::TLockElectionManagerConfig,
    ((Cypress)(NCypressElection::TCypressElectionManagerConfig))
    ((Chaos)(TChaosElectionManagerConfig)));

TEST(TElectionConfigTest, PolymorphicRoundTrip)
{
    for (const auto* yson : {
        "{type=cypress;lock_path=\"//leader\";lock_acquisition_period=2000;}",
        "{type=chaos;lock_table_path=\"//locks\";chaos_cell_bundle=bundle;lock_acquisition_period=2000;}",
    }) {
        SCOPED_TRACE(yson);
        auto config = NYTree::ConvertTo<TElectionConfig>(NYson::TYsonStringBuf(yson));
        auto restored = NYTree::ConvertTo<TElectionConfig>(NYson::ConvertToYsonString(config));
        EXPECT_EQ(restored.GetType(), config.GetType());
        EXPECT_EQ(restored->LockAcquisitionPeriod, TDuration::Seconds(2));
        EXPECT_EQ(NYson::ConvertToYsonString(restored), NYson::ConvertToYsonString(config));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChaosElection
