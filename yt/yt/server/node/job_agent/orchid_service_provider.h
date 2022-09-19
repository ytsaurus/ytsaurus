#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

class IOrchidServiceProvider
    : public TRefCounted
{
public:
    virtual void Initialize() = 0;

    virtual NYTree::IYPathServicePtr GetOrchidService() = 0;
};

DEFINE_REFCOUNTED_TYPE(IOrchidServiceProvider)

IOrchidServiceProviderPtr CreateOrchidServiceProvider(NClusterNode::IBootstrapBase* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
