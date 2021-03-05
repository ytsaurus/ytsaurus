#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

struct ITvmService
    : public virtual TRefCounted
{
    virtual TFuture<TString> GetTicket(const TString& serviceId) = 0;
    virtual TErrorOr<TParsedTicket> ParseUserTicket(const TString& ticket) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITvmService)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
