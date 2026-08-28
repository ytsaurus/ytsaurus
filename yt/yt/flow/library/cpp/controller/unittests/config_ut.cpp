#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/controller/config.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow::NController {
namespace {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TControllerConfigPtr LoadControllerConfig(TStringBuf yson)
{
    return ConvertTo<TControllerConfigPtr>(TYsonString(yson));
}

////////////////////////////////////////////////////////////////////////////////

// The election backend config is polymorphic, and a polymorphic struct holds nothing until it is
// either loaded or explicitly default-constructed. Every one of these cases must end up with a
// usable concrete config, since the connector dereferences it unconditionally.

TEST(TElectionManagerConfigTest, DefaultsToCypressWhenAbsent)
{
    auto config = LoadControllerConfig("{}");

    EXPECT_EQ(config->ElectionManager.GetType(), EElectionBackend::Cypress);
    auto backendConfig = config->ElectionManager.GetConcrete<TCypressElectionBackendConfig>();
    EXPECT_EQ(backendConfig->TransactionTimeout, TDuration::Seconds(5));
    EXPECT_EQ(backendConfig->LockAcquisitionPeriod, TDuration::Seconds(1));
}

// Deployed configs predate the "backend" parameter and name the Cypress settings directly.
TEST(TElectionManagerConfigTest, DefaultsToCypressWhenBackendIsOmitted)
{
    auto config = LoadControllerConfig(R"({election_manager={transaction_timeout="7s"}})");

    EXPECT_EQ(config->ElectionManager.GetType(), EElectionBackend::Cypress);
    EXPECT_EQ(
        config->ElectionManager.GetConcrete<TCypressElectionBackendConfig>()->TransactionTimeout,
        TDuration::Seconds(7));
}

TEST(TElectionManagerConfigTest, DyntableBackend)
{
    auto config = LoadControllerConfig(
        R"({election_manager={backend=dyntable;leader_lease_ttl="15s";detach_timeout="20s";lock_acquisition_period="2s"}})");

    EXPECT_EQ(config->ElectionManager.GetType(), EElectionBackend::Dyntable);
    auto backendConfig = config->ElectionManager.GetConcrete<TDyntableElectionBackendConfig>();
    EXPECT_EQ(backendConfig->LeaderLeaseTtl, TDuration::Seconds(15));
    EXPECT_EQ(backendConfig->DetachTimeout, TDuration::Seconds(20));
    // The shared parameters are registered by the base struct and apply to both backends.
    EXPECT_EQ(backendConfig->LockAcquisitionPeriod, TDuration::Seconds(2));
}

// A parameter of the other backend is simply unrecognized, as anywhere else in a yson struct:
// switching the backend silently drops the settings that no longer apply.
TEST(TElectionManagerConfigTest, IgnoresForeignBackendParameters)
{
    auto config = LoadControllerConfig(R"({election_manager={backend=dyntable;transaction_timeout="7s"}})");

    EXPECT_EQ(config->ElectionManager.GetType(), EElectionBackend::Dyntable);
}

////////////////////////////////////////////////////////////////////////////////

// Nothing refreshes the leader row between two fenced commits, so a ttl that does not cover the
// scheduling cadence demotes the leader in the middle of healthy work. The config must not load.

TEST(TElectionManagerConfigTest, RejectsLeaderLeaseTtlBelowTheCadence)
{
    EXPECT_THROW_WITH_SUBSTRING(
        LoadControllerConfig(R"({scheduler_period="5s";election_manager={backend=dyntable;leader_lease_ttl="500ms"}})"),
        "leader_lease_ttl");
}

TEST(TElectionManagerConfigTest, AcceptsLeaderLeaseTtlCoveringTheCadence)
{
    auto config = LoadControllerConfig(
        R"({scheduler_period="1s";election_manager={backend=dyntable;leader_lease_ttl="10s"}})");

    EXPECT_EQ(
        config->ElectionManager.GetConcrete<TDyntableElectionBackendConfig>()->LeaderLeaseTtl,
        TDuration::Seconds(10));
}

////////////////////////////////////////////////////////////////////////////////

// The pipeline-wide deadline has to outlast a change of leader: the replica waits out the leader
// lease before it can take over, and only then refreshes the deadline it inherited.

TEST(TElectionManagerConfigTest, RejectsLeaseTimeoutBelowTheLeaderLeaseTtl)
{
    EXPECT_THROW_WITH_SUBSTRING(
        LoadControllerConfig(
            R"({scheduler_period="1s";lease_manager={lease_timeout="20s"};)"
            R"(election_manager={backend=dyntable;leader_lease_ttl="10s"}})"),
        "lease_timeout");
}

TEST(TElectionManagerConfigTest, AcceptsLeaseTimeoutCoveringTheLeaderLeaseTtl)
{
    auto config = LoadControllerConfig(
        R"({scheduler_period="1s";lease_manager={lease_timeout="60s"};)"
        R"(election_manager={backend=dyntable;leader_lease_ttl="10s"}})");

    EXPECT_EQ(config->LeaseManager->LeaseTimeout, TDuration::Seconds(60));
}

////////////////////////////////////////////////////////////////////////////////

// The check is specific to the dyntable backend: a Cypress leader is fenced by its lock
// transaction, which its own pings keep alive regardless of the scheduling cadence.
TEST(TElectionManagerConfigTest, DoesNotConstrainTheCypressBackend)
{
    auto config = LoadControllerConfig(R"({scheduler_period="5s";election_manager={backend=cypress}})");

    EXPECT_EQ(config->ElectionManager.GetType(), EElectionBackend::Cypress);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NController
