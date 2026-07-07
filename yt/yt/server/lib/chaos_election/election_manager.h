#pragma once

#include "public.h"

#include <yt/yt/library/lock_election/election_manager.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/table_client/public.h>

namespace NYT::NChaosElection {

////////////////////////////////////////////////////////////////////////////////

struct TChaosElectionManagerOptions
    : public TRefCounted
{
    std::string GroupName;
    std::string MemberName;
};

DEFINE_REFCOUNTED_TYPE(TChaosElectionManagerOptions)

////////////////////////////////////////////////////////////////////////////////

//! Chaos lease-based election manager.
//!
//! Implements leader election using a shared dynamic table
//! (the "lock table") as the coordination point and chaos leases
//! as the liveness mechanism. The lock table has one row per
//! election group, keyed by
//! #TChaosElectionManagerOptions::GroupName; multiple independent
//! elections can coexist in the same table.
//!
//! Lock acquisition protocol:
//!
//! Each participant periodically attempts the following within a
//! single tablet transaction (providing snapshot isolation):
//! 1. Read the current leader row for this group.
//! 2. If a row exists, try to attach to the recorded chaos lease.
//!    If the lease responds to a ping, the existing leader is
//!    alive; abort the transaction and back off. If the lease is
//!    dead (ResolveError), proceed to take over.
//! 3. Create a new chaos lease.
//! 4. Overwrite the lock table row with our lease ID, member
//!    name, and timeout.
//! 5. Commit the tablet transaction.
//!
//! The tablet transaction prevents participant races during lock
//! table updates: if two participants try to update the row with
//! their lease, exactly one commit succeeds (the other gets a
//! conflict error).
//! https://ytsaurus.tech/docs/user-guide/dynamic-tables/transactions#timestamps
//!
//! Example timeline for two competing participants A and B:
//!
//!   A: begin tx -> read row -> create lease La -> write -> commit OK
//!   B:   begin tx -> read row -> create lease Lb -> write -> commit FAIL
//!
//! After election, the leader periodically pings the chaos lease
//! to keep it alive and updates last ping time in the lock table
//! row.
NLockElection::ILockElectionManagerPtr CreateChaosElectionManager(
    NApi::IClientPtr client,
    IInvokerPtr invoker,
    TChaosElectionManagerConfigPtr config,
    TChaosElectionManagerOptionsPtr options);

//! Returns the schema required for the lock table used by the chaos election manager.
NTableClient::TTableSchemaPtr GetChaosElectionLockTableSchema();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosElection
