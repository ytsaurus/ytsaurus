#pragma once

#include "public.h"
#include "dynamic_state.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/ypath/rich.h>

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
    std::optional<TConsumerRegistrationTableRow> GetRegistration(
        const NYPath::TRichYPath& queue,
        const NYPath::TRichYPath& consumer) const;

    // NB: Calling GetRegistration immediately after this call may return stale results.
    void RegisterQueueConsumer(
        const NYPath::TRichYPath& queue,
        const NYPath::TRichYPath& consumer,
        bool vital);

    // NB: Calling GetRegistration immediately after this call may return stale results.
    void UnregisterQueueConsumer(
        const NYPath::TRichYPath& queue,
        const NYPath::TRichYPath& consumer);

    void Clear();

    //! Exports information about current applied config and cached registrations.
    NYTree::IYPathServicePtr GetOrchidService() const;

private:
    const TQueueConsumerRegistrationManagerConfigPtr Config_;
    // The connection holds a strong reference to this object.
    const TWeakPtr<NApi::NNative::IConnection> Connection_;
    const IInvokerPtr Invoker_;
    const std::optional<TString> ClusterName_;
    const NConcurrency::TPeriodicExecutorPtr RefreshExecutor_;
    const NYTree::IYPathServicePtr OrchidService_;
    const NLogging::TLogger Logger;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ConfigurationSpinLock_);
    TQueueConsumerRegistrationManagerConfigPtr DynamicConfig_;
    TConsumerRegistrationTablePtr RegistrationTable_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, CacheSpinLock_);
    THashMap<std::pair<NYPath::TRichYPath, NYPath::TRichYPath>, TConsumerRegistrationTableRow> Registrations_;

    //! Polls the registration dynamic table and fills the cached map.
    void Refresh();
    void GuardedRefresh();

    //! Returns the `RegistrationTable_` if not null.
    //! Otherwise, attempts to establish a connection to the registration table's (potentially remote) cluster and
    //! initializes `RegistrationTable_` if successful.
    TConsumerRegistrationTablePtr GetOrInitRegistrationTableOrThrow();

    //! Retrieves, applies and stores a local version of the dynamic config.
    TQueueConsumerRegistrationManagerConfigPtr RefreshDynamicConfig();

    //! Produces information about cached registrations.
    void BuildOrchid(NYson::IYsonConsumer* consumer) const;
};

DEFINE_REFCOUNTED_TYPE(TQueueConsumerRegistrationManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
