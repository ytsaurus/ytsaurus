#pragma once

#include "private.h"

#include <yt/yt/ytlib/queue_client/dynamic_state.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

//! Periodically removes rows of the multi_consumer_names state table referring
//! to multi consumers that are no longer present in the consumers state table.
struct IMultiConsumerNamesGarbageCollector
    : public TRefCounted
{
    virtual NYTree::IYPathServicePtr GetOrchidService() const = 0;

    virtual void Start() = 0;

    virtual void Stop() = 0;

    virtual void OnDynamicConfigChanged(
        const TMultiConsumerNamesGarbageCollectorDynamicConfigPtr& oldConfig,
        const TMultiConsumerNamesGarbageCollectorDynamicConfigPtr& newConfig) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMultiConsumerNamesGarbageCollector)

IMultiConsumerNamesGarbageCollectorPtr CreateMultiConsumerNamesGarbageCollector(
    IInvokerPtr invoker,
    NQueueClient::TDynamicStatePtr dynamicState,
    TCallback<NAlertManager::IAlertCollectorPtr()> createAlertCollectorCallback);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
