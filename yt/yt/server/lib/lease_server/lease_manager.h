#pragma once

#include "public.h"

#include <yt/yt/server/lib/hive/public.h>

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/core/actions/signal.h>

#include <library/cpp/yt/yson/consumer.h>

namespace NYT::NLeaseServer {

////////////////////////////////////////////////////////////////////////////////

struct ILeaseGuard
    : public TRefCounted
{
    virtual TLeaseId GetLeaseId() const = 0;

    virtual bool IsPersistent() const = 0;
    virtual bool IsValid() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ILeaseGuard)

////////////////////////////////////////////////////////////////////////////////

struct TLeaseGuardSerializer
{
    static void Save(NHydra::TSaveContext& context, const ILeaseGuardPtr& guard);
    static void Load(NHydra::TLoadContext& context, ILeaseGuardPtr& guard);
};

struct TLeaseGuardComparer
{
    bool operator()(const ILeaseGuardPtr& lhs, const ILeaseGuardPtr& rhs) const;

    static bool Compare(const ILeaseGuardPtr& lhs, const ILeaseGuardPtr& rhs);
};

////////////////////////////////////////////////////////////////////////////////

struct ILease
{
    virtual ~ILease() = default;

    virtual TLeaseId GetId() const = 0;

    virtual ELeaseState GetState() const = 0;

    virtual int RefPersistently(bool force = false) = 0;
    virtual int UnrefPersistently() = 0;
    virtual ILeaseGuardPtr GetPersistentLeaseGuard(bool force = false) = 0;
    virtual ILeaseGuardPtr GetPersistentLeaseGuardOnLoad() = 0;

    virtual int RefTransiently(bool force = false) = 0;
    virtual int UnrefTransiently(int leaderAutomatonTerm) = 0;
    virtual ILeaseGuardPtr GetTransientLeaseGuard(bool force = false) = 0;
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

    virtual std::optional<int> GetLeaderAutomatonTerm() const = 0;

    virtual NRpc::IServicePtr GetRpcService() = 0;

    virtual void BuildOrchid(NYson::IYsonConsumer* consumer) const = 0;
};

DEFINE_REFCOUNTED_TYPE(ILeaseManager)

////////////////////////////////////////////////////////////////////////////////

//! TLS helpers for lease manager.
void SetLeaseManager(ILeaseManagerPtr leaseManager);
ILeaseManagerPtr GetLeaseManager();

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

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class C>
struct TSerializerTraits<NLeaseServer::ILeaseGuardPtr, C, void>
{
    using TSerializer = NLeaseServer::TLeaseGuardSerializer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLeaseServer
