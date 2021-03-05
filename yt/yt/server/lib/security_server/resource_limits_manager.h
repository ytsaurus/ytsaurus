#pragma once

#include "public.h"

#include <yt/yt/client/tablet_client/public.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

struct IResourceLimitsManager
    : public virtual TRefCounted
{
    virtual void ValidateResourceLimits(
        const TString& account,
        const TString& mediumName,
        const std::optional<TString>& tabletCellBundle = std::nullopt,
        NTabletClient::EInMemoryMode inMemoryMode = NTabletClient::EInMemoryMode::None) = 0;
};

DEFINE_REFCOUNTED_TYPE(IResourceLimitsManager)

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
