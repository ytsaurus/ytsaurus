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

    virtual int RefTransiently(bool force) = 0;
    virtual int UnrefTransiently() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ILeaseManager
    : public virtual TRefCounted
{
    DECLARE_INTERFACE_SIGNAL(void(TLeaseId leaseId, NHydra::TCellId cellId), LeaseRevoked);

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
