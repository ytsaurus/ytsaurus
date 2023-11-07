#pragma once

#include "public.h"

#include <yt/yt/ytlib/query_client/session_coordinator.h>

namespace NYT::NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

class IDistributedSession
    : public TRefCounted
{
public:
    virtual void RenewLease() const = 0;

    virtual void PropagateToNode(TString address) = 0;

    virtual std::vector<TString> CopyPropagationRequests() const = 0;

    virtual void FulfillPropagationRequests(const std::vector<TString>& addresses) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDistributedSession)

////////////////////////////////////////////////////////////////////////////////

class ISessionManager
    : public TRefCounted
{
public:
    virtual TWeakPtr<IDistributedSession> GetOrCreate(NQueryClient::TSessionId id, TDuration retentionTime) = 0;

    virtual TWeakPtr<IDistributedSession> GetOrThrow(NQueryClient::TSessionId id) = 0;

    virtual bool InvalidateIfExists(NQueryClient::TSessionId id) = 0;

    virtual void UponSessionLeaseExpiration(NQueryClient::TSessionId id) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISessionManager)

////////////////////////////////////////////////////////////////////////////////

ISessionManagerPtr CreateNewSessionManager(
    IInvokerPtr controlInvoker,
    const NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent
