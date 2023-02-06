#pragma once

#include "private.h"

#include <yt/yt/core/yson/public.h>

#include <any>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

//! A host interface for accessing object store containing queues, consumers and registrations.
/*!
 *  \note Thread affinity: any.
 */
struct IObjectStore
    : public TRefCounted
{
    //! Returns null if requested object not found.
    virtual TRefCountedPtr FindSnapshot(NQueueClient::TCrossClusterReference objectRef) const = 0;

    //! Returns empty vector if requested object not found.
    virtual std::vector<NQueueClient::TConsumerRegistrationTableRow> GetRegistrations(
        NQueueClient::TCrossClusterReference objectRef,
        EObjectKind objectKind) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IObjectStore)

////////////////////////////////////////////////////////////////////////////////

//! A common interface for queue controllers and consumer controllers.
/*!
 *  \note Thread affinity: any.
 */
struct IObjectController
    : public TRefCounted
{
    virtual void OnDynamicConfigChanged(
        const TQueueControllerDynamicConfigPtr& oldConfig,
        const TQueueControllerDynamicConfigPtr& newConfig) = 0;

    virtual void OnRowUpdated(std::any row) = 0;

    // Always returns non-null.
    virtual TRefCountedPtr GetLatestSnapshot() const = 0;

    virtual void BuildOrchid(NYson::IYsonConsumer* consumer) const = 0;

    virtual bool IsLeading() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IObjectController)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
