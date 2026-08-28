#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/dyntable_lease.h>

#include <yt/yt/library/lock_election/election_manager.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

struct TDyntableElectionManagerOptions
{
    //! Path to the pipeline's `flow_control` dynamic table where the leader lease row lives.
    //! The table itself is provisioned by yt_sync like every other pipeline table.
    NYPath::TYPath FlowControlTablePath;
    //! Path to the pipeline's `leases` dynamic table for partition leases.
    NYPath::TYPath LeasesTablePath;
    //! The identity to compete with.
    TIncarnationId IncarnationId;
    std::string Address;
    //! How long a written leader lease stays fresh; every fenced transaction and every renewal
    //! prolongs it.
    TDuration LeaseTtl;
    //! How often to run the capture/renew iteration.
    TDuration CapturePeriod;
    //! Self-demote when no renewal has succeeded for this long (e.g. the dynamic table is
    //! unreachable). Committing state is impossible anyway (fencing), this only makes the
    //! controller's view of itself honest.
    TDuration DetachTimeout;
};

DECLARE_REFCOUNTED_STRUCT(IDyntableElectionManager)

//! The dyntable election manager: NLockElection::ILockElectionManager plus the recovery-time
//! renewal switch.
struct IDyntableElectionManager
    : public NLockElection::ILockElectionManager
{
    //! While enabled, the election loop renews the leader lease in the background — needed
    //! during leader recovery, whose long read-only phases create no fenced transactions.
    //! The switch turns on automatically on every leadership acquisition; the controller turns
    //! it off after the first successful scheduling iteration, and from then on the lease is fed
    //! by fenced transactions alone (a stalled work cycle loses the lease by design).
    //!
    //! |leadershipEpoch| is the epoch the caller acted on behalf of: a call carrying any other
    //! epoch is ignored, so a callback delayed across a demotion and a re-acquisition cannot
    //! disarm the renewal of the leadership that is running now.
    virtual void SetRecoveryRenewalEnabled(bool enabled, ui64 leadershipEpoch) = 0;

    //! Incremented on every leadership acquisition; zero until the first one.
    virtual ui64 GetLeadershipEpoch() const = 0;

    //! Whether the election loop is currently renewing the lease on its own.
    virtual bool IsRecoveryRenewalEnabled() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IDyntableElectionManager)

//! Leader election over a plain dynamic table row (see TDyntableLeases). No YT leases, no
//! Cypress locks, no prerequisites: safety comes from the leader-row write conflicts, so it works
//! identically for regular and chaos replicated tables.
IDyntableElectionManagerPtr CreateDyntableElectionManager(
    NApi::IClientPtr client,
    IInvokerPtr invoker,
    TDyntableElectionManagerOptions options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
