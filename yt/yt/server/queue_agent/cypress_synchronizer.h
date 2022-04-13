#pragma once

#include "private.h"
#include "dynamic_state.h"

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

struct ICypressSynchronizer
    : public TRefCounted
{
    virtual NYTree::IYPathServicePtr GetOrchidService() const = 0;

    virtual void Start() = 0;

    virtual void Stop() = 0;

    virtual void OnDynamicConfigChanged(
        const TCypressSynchronizerDynamicConfigPtr& oldConfig,
        const TCypressSynchronizerDynamicConfigPtr& newConfig) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICypressSynchronizer)

ICypressSynchronizerPtr CreateCypressSynchronizer(
    TCypressSynchronizerConfigPtr config,
    IInvokerPtr controlInvoker,
    TDynamicStatePtr dynamicState,
    NHiveClient::TClientDirectoryPtr clientDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
