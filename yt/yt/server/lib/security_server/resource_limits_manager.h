#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/client/tablet_client/public.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

struct IResourceLimitsManager
    : public virtual TRefCounted
{
    virtual void ValidateResourceLimits(
        const std::string& account,
        const std::string& mediumName,
        const std::optional<std::string>& tabletCellBundle = std::nullopt,
        NTabletClient::EInMemoryMode inMemoryMode = NTabletClient::EInMemoryMode::None) = 0;

    virtual void Reconfigure(const NTabletNode::TSecurityManagerDynamicConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IResourceLimitsManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
