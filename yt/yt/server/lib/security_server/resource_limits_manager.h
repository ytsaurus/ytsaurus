#pragma once

#include "public.h"

#include <yt/client/tablet_client/public.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

struct IResourceLimitsManager
    : public virtual TRefCounted
{
    virtual void ValidateResourceLimits(
        const TString& account,
        const TString& mediumName,
        NTabletClient::EInMemoryMode inMemoryMode) = 0;
};

DEFINE_REFCOUNTED_TYPE(IResourceLimitsManager)

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
