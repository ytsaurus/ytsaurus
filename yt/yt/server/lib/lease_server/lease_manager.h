#pragma once

#include "public.h"

#include <yt/yt/server/lib/hive/public.h>

#include <yt/yt/server/lib/hydra/serialize.h>

#include <yt/yt/core/actions/signal.h>

#include <library/cpp/yt/yson/consumer.h>

namespace NYT::NLeaseServer {

////////////////////////////////////////////////////////////////////////////////

struct ILease
{
    virtual ~ILease() = default;

    virtual TLeaseId GetId() const = 0;

    virtual ELeaseState GetState() const = 0;

    virtual int RefPersistently(bool force) = 0;
    virtual int UnrefPersistently() = 0;
    virtual int GetPersistentRefCounter() const = 0;

    virtual int RefTransiently(bool force) = 0;
    virtual int UnrefTransiently() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ILeaseManager
    : public virtual TRefCounted
{
    // Fired on the lessor side (i.e. on the cell that has issued the lease) once every lessee confirms revocation.
    DECLARE_INTERFACE_SIGNAL(void(TLeaseId leaseId, NHydra::TCellId cellId), LeaseRevoked);

    // Fired on the lessee side (i.e. on the cell that has requested the issuing of the lease)
    // right before the lease is destroyed locally.
    // NB: Holding a reference to a lease (either transient or persistent) is not enough to prevent
    // its removal because RevokeLease may be called with force == true.
    DECLARE_INTERFACE_SIGNAL(void(ILease* lease), LeaseRemoved);

    virtual ILease* FindLease(TLeaseId leaseId) const = 0;
    virtual ILease* GetLease(TLeaseId leaseId) const = 0;
    virtual ILease* GetLeaseOrThrow(TLeaseId leaseId) const = 0;

    virtual void SetDecommission(bool decommission) = 0;
    virtual bool IsFullyDecommissioned() const = 0;

    virtual NRpc::IServicePtr GetRpcService() = 0;
    virtual NYTree::IYPathServicePtr GetOrchidService() = 0;
};

DEFINE_REFCOUNTED_TYPE(ILeaseManager)

////////////////////////////////////////////////////////////////////////////////

ILeaseManagerPtr CreateLeaseManager(
    TLeaseManagerConfigPtr config,
    NHydra::IHydraManagerPtr hydraManager,
    NHydra::TCompositeAutomatonPtr automaton,
    NHiveServer::IHiveManagerPtr hiveManager,
    IInvokerPtr automatonInvoker,
    NElection::TCellId selfCellId,
    NHydra::IUpstreamSynchronizerPtr upstreamSynchronizer,
    NRpc::IAuthenticatorPtr authenticator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLeaseServer
