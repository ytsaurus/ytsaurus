#pragma once

#include "public.h"
#include "dynamic_state.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

struct IQueueConsumerRegistrationManager
    : public TRefCounted
{
    virtual void StartSync() const = 0;
    virtual void StopSync() const = 0;

    //! Contains path resolution context for better error messages.
    struct TGetRegistrationResult
    {
        std::optional<TConsumerRegistrationTableRow> Registration;
        NYPath::TRichYPath ResolvedQueue;
        NYPath::TRichYPath ResolvedConsumer;
    };

    // NB: May return stale results in regard to the other methods in this class.
    // NB: If cache bypass is enabled, this call will always refresh the cache itself.
    virtual TGetRegistrationResult GetRegistrationOrThrow(
        NYPath::TRichYPath queue,
        NYPath::TRichYPath consumer) = 0;

    // NB: May return stale results in regard to the other methods in this class.
    // NB: If cache bypass is enabled, this call will always refresh the cache itself.
    virtual std::vector<TConsumerRegistrationTableRow> ListRegistrations(
        std::optional<NYPath::TRichYPath> queue,
        std::optional<NYPath::TRichYPath> consumer) = 0;

    // NB: Using the registration cache immediately after this call may return stale results.
    virtual void RegisterQueueConsumer(
        NYPath::TRichYPath queue,
        NYPath::TRichYPath consumer,
        bool vital,
        const std::optional<std::vector<int>>& partitions = {}) = 0;

    // NB: Using the registration cache immediately after this call may return stale results.
    virtual void UnregisterQueueConsumer(
        NYPath::TRichYPath queue,
        NYPath::TRichYPath consumer) = 0;

    virtual void Clear() = 0;

    //! Exports information about current applied config and cached registrations.
    // NB: If cache bypass is enabled, this call will always refresh the cache itself.
    virtual void BuildOrchid(NYTree::TFluentAny fluent) = 0;
};

DEFINE_REFCOUNTED_TYPE(IQueueConsumerRegistrationManager)

////////////////////////////////////////////////////////////////////////////////

IQueueConsumerRegistrationManagerPtr CreateQueueConsumerRegistrationManager(
    TQueueConsumerRegistrationManagerConfigPtr config,
    NApi::NNative::IConnection* connection,
    IInvokerPtr invoker,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
