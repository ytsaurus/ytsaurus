#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

struct ITvmService
    : public virtual TRefCounted
{
    // Our TVM id.
    virtual ui32 GetSelfTvmId() = 0;

    // Get TVM service ticket from us to serviceId. Service mapping must be in config.
    // Throws on failure.
    virtual TString GetServiceTicket(const TString& serviceId) = 0;

    // Decode user ticket contents. Throws on failure.
    virtual TParsedTicket ParseUserTicket(const TString& ticket) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITvmService)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
