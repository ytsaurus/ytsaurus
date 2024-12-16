#pragma once

#include "public.h"

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  Thread affinity: any.
 */
struct INodeTrackerCache
    : public TRefCounted
{
    virtual void ResetNodeDefaultAddresses() = 0;

    virtual void UpdateNodeDefaultAddress(TNodeId nodeId, std::optional<TStringBuf> defaultAddress) = 0;

    virtual std::string GetNodeDefaultAddressOrThrow(TNodeId nodeId) = 0;
};

////////////////////////////////////////////////////////////////////////////////

INodeTrackerCachePtr CreateNodeTrackerCache();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
