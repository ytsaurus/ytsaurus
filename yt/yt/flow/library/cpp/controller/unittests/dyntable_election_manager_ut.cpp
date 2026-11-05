#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/controller/dyntable_election_manager.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <library/cpp/yt/misc/guid.h>

namespace NYT::NFlow::NController {
namespace {

////////////////////////////////////////////////////////////////////////////////

IDyntableElectionManagerPtr CreateManager()
{
    // The manager is never started here, so no request ever reaches the client; only the
    // leadership bookkeeping is under test.
    return CreateDyntableElectionManager(
        /*client*/ nullptr,
        GetSyncInvoker(),
        TDyntableElectionManagerOptions{
            .FlowControlTablePath = "//tmp/flow_control",
            .LeasesTablePath = "//tmp/leases",
            .IncarnationId = TIncarnationId(TGuid::Create()),
            .Address = "localhost:1",
            .LeaseTtl = TDuration::Seconds(30),
            .CapturePeriod = TDuration::Seconds(1),
            .DetachTimeout = TDuration::Seconds(30),
        });
}

////////////////////////////////////////////////////////////////////////////////

// The controller reports the end of recovery from its scheduler, asynchronously: such a report
// can be delivered after its own leadership is gone. It must not disarm the renewal of whatever
// leadership is running by then, which is what the epoch is for.

TEST(TDyntableElectionManagerTest, LeadershipEpochStartsAtZero)
{
    EXPECT_EQ(CreateManager()->GetLeadershipEpoch(), 0u);
}

TEST(TDyntableElectionManagerTest, RecoveryRenewalSwitchIgnoresForeignEpochs)
{
    auto manager = CreateManager();
    auto epoch = manager->GetLeadershipEpoch();

    manager->SetRecoveryRenewalEnabled(true, epoch);
    EXPECT_TRUE(manager->IsRecoveryRenewalEnabled());

    // A report from another leadership leaves the switch alone.
    manager->SetRecoveryRenewalEnabled(false, epoch + 1);
    EXPECT_TRUE(manager->IsRecoveryRenewalEnabled());
    manager->SetRecoveryRenewalEnabled(false, epoch + 100);
    EXPECT_TRUE(manager->IsRecoveryRenewalEnabled());

    // The report of the running leadership does switch it off.
    manager->SetRecoveryRenewalEnabled(false, epoch);
    EXPECT_FALSE(manager->IsRecoveryRenewalEnabled());

    // Only an acquisition advances the epoch; none of the calls above did.
    EXPECT_EQ(manager->GetLeadershipEpoch(), epoch);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NController
