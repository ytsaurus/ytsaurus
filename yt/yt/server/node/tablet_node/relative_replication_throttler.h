#pragma once

#include "public.h"

#include <yt/yt/server/lib/tablet_node/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IRelativeReplicationThrottler
    : public TRefCounted
{
    virtual void OnReplicationBatchProcessed(NTransactionClient::TTimestamp recordTimestamp) = 0;

    virtual TFuture<void> Throttle() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IRelativeReplicationThrottler)

////////////////////////////////////////////////////////////////////////////////

IRelativeReplicationThrottlerPtr CreateRelativeReplicationThrottler(
    TRelativeReplicationThrottlerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
