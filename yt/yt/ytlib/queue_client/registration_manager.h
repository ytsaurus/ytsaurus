#pragma once

#include "public.h"
#include "dynamic_state.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

class TQueueConsumerRegistrationManager
    : public TRefCounted
{
public:
    TQueueConsumerRegistrationManager(
        TQueueConsumerRegistrationManagerConfigPtr config,
        NApi::NNative::IConnection* connection,
        IInvokerPtr invoker,
        const NLogging::TLogger& logger);

    void StartSync() const;
    void StopSync() const;

    // NB: May return stale results in regard to the other methods in this class.
    // NB: If cache bypass is enabled, this call will always refresh the cache itself.
    std::optional<TConsumerRegistrationTableRow> GetRegistration(
        const NYPath::TRichYPath& queue,
        const NYPath::TRichYPath& consumer);

    // NB: May return stale results in regard to the other methods in this class.
    // NB: If cache bypass is enabled, this call will always refresh the cache itself.
    std::vector<TConsumerRegistrationTableRow> ListRegistrations(
        std::optional<NYPath::TRichYPath> queue,
        std::optional<NYPath::TRichYPath> consumer);

    // NB: Using the registration cache immediately after this call may return stale results.
    void RegisterQueueConsumer(
        const NYPath::TRichYPath& queue,
        const NYPath::TRichYPath& consumer,
        bool vital);

    // NB: Using the registration cache immediately after this call may return stale results.
    void UnregisterQueueConsumer(
        const NYPath::TRichYPath& queue,
        const NYPath::TRichYPath& consumer);

    void Clear();

    //! Exports information about current applied config and cached registrations.
    // NB: If cache bypass is enabled, this call will always refresh the cache itself.
    void BuildOrchid(NYTree::TFluentAny fluent);

private:
    const TQueueConsumerRegistrationManagerConfigPtr Config_;
    // The connection holds a strong reference to this object.
    const TWeakPtr<NApi::NNative::IConnection> Connection_;
    const IInvokerPtr Invoker_;
    const std::optional<TString> ClusterName_;
    const NConcurrency::TPeriodicExecutorPtr ConfigurationRefreshExecutor_;
    const NConcurrency::TPeriodicExecutorPtr CacheRefreshExecutor_;
    const NLogging::TLogger Logger;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ConfigurationSpinLock_);
    TQueueConsumerRegistrationManagerConfigPtr DynamicConfig_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, CacheSpinLock_);
    THashMap<std::pair<NYPath::TRichYPath, NYPath::TRichYPath>, TConsumerRegistrationTableRow> Registrations_;

    //! Polls the registration dynamic table and fills the cached map.
    void RefreshCache();
    void GuardedRefreshCache();

    //! Retrieves the dynamic config from cluster directory, applies it and stores a local copy.
    void RefreshConfiguration();
    void GuardedRefreshConfiguration();

    //! Attempts to establish a connection to the registration table's (potentially remote) cluster
    //! and returns a client to be used for modifying the table.
    TConsumerRegistrationTablePtr CreateRegistrationTableWriteClientOrThrow() const;
    //! Collects registrations from the clusters specified in state read path.
    //! The first successful response is returned.
    std::vector<TConsumerRegistrationTableRow> FetchRegistrationsOrThrow() const;

    //! Returns the stored dynamic config.
    TQueueConsumerRegistrationManagerConfigPtr GetDynamicConfig() const;
};

DEFINE_REFCOUNTED_TYPE(TQueueConsumerRegistrationManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
