#pragma once

#include "public.h"

#include "commands.h"

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NZookeeper {

////////////////////////////////////////////////////////////////////////////////

struct ISession
    : public TRefCounted
{
    virtual TSessionId GetId() const = 0;

    virtual TDuration GetTimeout() const = 0;

    virtual const NConcurrency::TLease& GetLease() const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISession)

////////////////////////////////////////////////////////////////////////////////

ISessionPtr CreateSession(
    const TReqStartSessionPtr& req,
    NConcurrency::TLease lease);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeper
