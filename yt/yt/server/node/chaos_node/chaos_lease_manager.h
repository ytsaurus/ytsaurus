#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/hydra/entity_map.h>

#include <yt/yt/server/lib/tablet_server/replicated_table_tracker.h>

#include <yt/yt/server/lib/transaction_supervisor/transaction_lease_tracker.h>

#include <yt/yt/ytlib/chaos_client/proto/chaos_node_service.pb.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EChaosLeaseManagerState,
    ((Disabled)      (0))
    ((Enabling)      (1))
    ((Enabled)       (2))
    ((Disabling)     (3))
);

struct TCreateChaosLeaseOptions
{
    TChaosLeaseId ParentId;
    TDuration Timeout;
};

////////////////////////////////////////////////////////////////////////////////

struct IChaosLeaseManager
    : public virtual TRefCounted
{
    using TCtxRemoveChaosLeasePtr = TIntrusivePtr<NRpc::TTypedServiceContext<
        NChaosClient::NProto::TReqRemoveChaosLease,
        NChaosClient::NProto::TRspRemoveChaosLease
    >>;
    using TCtxCreateChaosLeasePtr = TIntrusivePtr<NRpc::TTypedServiceContext<
        NChaosClient::NProto::TReqCreateChaosLease,
        NChaosClient::NProto::TRspCreateChaosLease
    >>;

    virtual void Initialize() = 0;

    virtual NYTree::IYPathServicePtr GetOrchidService() const = 0;

    virtual EChaosLeaseManagerState GetState() const = 0;

    virtual void CreateChaosLease(const TCtxCreateChaosLeasePtr& context) = 0;
    virtual void RemoveChaosLease(const TCtxRemoveChaosLeasePtr& context) = 0;

    virtual TFuture<void> PingChaosLease(TChaosLeaseId chaosLeaseId, bool pingAncestors) = 0;

    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(ChaosLease, TChaosLease);
    virtual TChaosLease* GetChaosLeaseOrThrow(TChaosLeaseId chaosLeaseId) const = 0;

    virtual void InsertChaosLease(std::unique_ptr<TChaosLease> chaosLease) = 0;

    virtual NTransactionSupervisor::ITransactionLeaseTrackerPtr GetChaosLeaseTracker() const = 0;

    virtual void HandleChaosLeaseStateTransition(TChaosLease* chaosLease) = 0;

    //! ChaosLeaseManager migration protocol for chaos leases is different from
    //! ChaosManager protocol for migrating replication cards.
    //! The main motivation for this difference is the requirement that user must
    //! never be in a situation, where all chaos cells lose track of a lease,
    //! even for a short period of time. For replication cards such situation is
    //! tolerable, because it will cause a simple retry, but leases are used for
    //! leader election and the requirements are more strict.
    //! 1. User must always know, where to search for a chaos lease. Replication card
    //!    might migrate around the cluster cell's freely, but chaos lease is always
    //!    on one of the two sibling cells.
    //! 2. Each sibling cell, responsible for a chaos lease, must maintain its
    //!    current state, to determine which cell is responsible for this lease at
    //!    any given moment, so that after requesting lease from one cell, client can
    //!    be sure that the lease is present on that cell or it is managed by its
    //!    sibling right now.
    //!    Without such informations the following situations could occur:
    //!       a. Client asks both cells about the lease but receives response only
    //!         from one, which does not hold the lease. Should the client wait for
    //!         the other cell to response? What if it is currently offline under
    //!         maintainence?
    //!       b. Both cells are online, but the lease is migrating. It could happen
    //!          that the first cell sent the lease via hive message to its sibling
    //!          and forgot about it, and the second cell haven't yet received this
    //!          hive message so it also does not know about the lease (as for
    //!          2025/10/21 replication card migration is implemented this way and
    //!          it allows replication card to "disappear" from the cluster for a
    //!          brief moment during its migration).
    //! So to support chaos lease migration, chaos lease is only allowed to migrate
    //! from the cell of its creation to its sibling and back.
    //! During the chaos lease lifetime, pair of chaos cells (origin and sibling)
    //! maintain their current state, which can be one of the following:
    //! [Enabled, Disabled, Enabling, Disabling].
    //! Only the following configurations of states and transitions are allowed:
    //! [Enabling, Disabling] -> [Enabled, Disabled] -> [Disabling, Enabling] ->
    //! [Disabled, Enabled]
    //! Only cell in Enabled state can respond to requests about the lease.
    //!
    //! Migration happens during [Enabling, Disabling] configuration, and during this
    //! process cells should respond with a retryable error, so that user waits for
    //! its completion. The migration protocol itself is several hive messages sent
    //! to a sibling cell:
    //! *For clarity (A) is the current Enabled cell and (B) is its Disabled sibling*
    //! 1. (A) Change state to Disabling (from Enabled), send hive message to (B) to
    //!    change its state to Enabling (from Disabled).
    //! 2. (A) Send all the managed chaos leases to (B). Chaos leases are sent in
    //!    batches to avoid huge hive messages.
    //! 3. (A) Change state to Disabled (from Disabling), send hive message to (B) to
    //!    change its state to Enabled (from Enabling).
    //! After cells just got created, initial states are assigned according to the
    //! parity of their cell tags to establish determined initialization order.
    virtual void MakeStateTransition(EChaosLeaseManagerState expectedState, EChaosLeaseManagerState newState) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChaosLeaseManager)

IChaosLeaseManagerPtr CreateChaosLeaseManager(
    TChaosLeaseManagerConfigPtr config,
    IChaosSlotPtr slot,
    IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
