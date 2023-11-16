#pragma once

#include "public.h"

#include <yt/yt/ytlib/query_client/public.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

struct IDistributedSession
    : public TRefCounted
{
    virtual void RenewLease() const = 0;

    virtual void PropagateToNode(TString address) = 0;

    virtual std::vector<TString> GetPropagationAddresses() const = 0;

    virtual void ErasePropagationAddresses(const std::vector<TString>& addresses) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDistributedSession)

////////////////////////////////////////////////////////////////////////////////

IDistributedSessionPtr CreateDistributedSession(
    NQueryClient::TDistributedSessionId sessionId,
    NConcurrency::TLease lease);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent
