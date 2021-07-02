#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/node/cellar_node/bootstrap.h>

#include <yt/yt/server/lib/security_server/resource_limits_manager.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/optional.h>

#include <yt/yt/core/ytree/permission.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TSecurityManager
    : public NSecurityServer::IResourceLimitsManager
{
public:
    TSecurityManager(
        TSecurityManagerConfigPtr config,
        NCellarNode::IBootstrap* bootstrap);
    ~TSecurityManager();

    TFuture<void> CheckResourceLimits(
        const TString& account,
        const TString& mediumName,
        const std::optional<TString>& tabletCellBundle = std::nullopt,
        NTabletClient::EInMemoryMode inMemoryMode = NTabletClient::EInMemoryMode::None);

    virtual void ValidateResourceLimits(
        const TString& account,
        const TString& mediumName,
        const std::optional<TString>& tabletCellBundle = std::nullopt,
        NTabletClient::EInMemoryMode inMemoryMode = NTabletClient::EInMemoryMode::None);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TSecurityManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
