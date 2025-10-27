#pragma once

#include "public.h"

#include "registration_manager.h"

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

class TQueueConsumerRegistrationManagerBase
    : public IQueueConsumerRegistrationManager
{
public:
    TQueueConsumerRegistrationManagerBase(
        TQueueConsumerRegistrationManagerConfigPtr config,
        NApi::NNative::IConnection* connection,
        IInvokerPtr invoker,
        const NProfiling::TProfiler& profiler,
        const NLogging::TLogger& logger);

    void StartSync() const override;
    void StopSync() const override;

    TGetRegistrationResult GetRegistrationOrThrow(
        NYPath::TRichYPath queue,
        NYPath::TRichYPath consumer) override final;

    std::vector<TConsumerRegistrationTableRow> ListRegistrations(
        std::optional<NYPath::TRichYPath> queue,
        std::optional<NYPath::TRichYPath> consumer) override final;

    void RegisterQueueConsumer(
        NYPath::TRichYPath queue,
        NYPath::TRichYPath consumer,
        bool vital,
        const std::optional<std::vector<int>>& partitions = {}) override final;

    void UnregisterQueueConsumer(
        NYPath::TRichYPath queue,
        NYPath::TRichYPath consumer) override final;

protected:
    virtual std::optional<TConsumerRegistrationTableRow> DoFindRegistration(
        NYPath::TRichYPath resolvedQueue,
        NYPath::TRichYPath resolvedConsumer) = 0;

    virtual std::vector<TConsumerRegistrationTableRow> DoListRegistrations(
        std::optional<NYPath::TRichYPath> resolvedQueue,
        std::optional<NYPath::TRichYPath> resolvedConsumer) = 0;

    //! Resolves the corresponding chaos_replicated_table/replicated_table path for chaos/replicated table replicas
    //! via the replicated table mapping cache.
    virtual NYPath::TRichYPath ResolveReplica(
        const NYPath::TRichYPath& objectPath,
        const NTabletClient::TTableMountInfoPtr& tableMountInfo,
        bool throwOnFailure) const = 0;

    //! Method for handling changes in derived classes. It must only handle config changes
    //! specific to the derived class, e.g. cache_refresh_period for full-scan implementation
    //! of registration manager.
    virtual void OnDynamicConfigChanged(
        const TQueueConsumerRegistrationManagerConfigPtr oldConfig,
        const TQueueConsumerRegistrationManagerConfigPtr newConfig) = 0;

    virtual void RefreshCache() = 0;

protected:
    //! Returns the stored dynamic config.
    TQueueConsumerRegistrationManagerConfigPtr GetDynamicConfig() const;

    //! Attempts to establish a connection to the registration table's (potentially remote) cluster
    //! and returns a client to be used for modifying the table.
    TConsumerRegistrationTablePtr CreateRegistrationTableWriteClientOrThrow() const;

    //! Performs all necessary resolutions for a pair of queue and consumer paths in accordance with the provided config.
    //! Modifies the passed pointers, if not null.
    //! Effectively performs the following:
    //!     1. If cluster is not set, sets it to the statically configured cluster.
    //!     2. Resolves physical paths for potential symlinks.
    //!     3. Resolves [chaos] replicated table path for replicas.
    //! If throwOnFailure is false, errors during symlink resolution are written to the log only.
    void Resolve(
        const TQueueConsumerRegistrationManagerConfigPtr& config,
        NYPath::TRichYPath* queuePath,
        NYPath::TRichYPath* consumerPath,
        bool throwOnFailure);

    //! Resolves physical path for object.
    //! If throwOnFailure is false, errors during resolution are written to the log only and the path is returned unchanged.
    NYPath::TRichYPath ResolveObjectPhysicalPath(
        const NYPath::TRichYPath& objectPath,
        const NTabletClient::TTableMountInfoPtr& tableMountInfo) const;

    //! Updates configuration from cluster connection and calls #Reconfigure
    //! to process the changes after a successful update.
    void RefreshConfiguration();
    void GuardedRefreshConfiguration();

    //! Validates that a statically configured cluster name is set.
    void ValidateClusterNameConfigured() const;

protected:
    const TQueueConsumerRegistrationManagerConfigPtr Config_;
    // The connection holds a strong reference to this object.
    const TWeakPtr<NApi::NNative::IConnection> Connection_;
    const IInvokerPtr Invoker_;
    const std::optional<std::string> ClusterName_;
    const NConcurrency::TPeriodicExecutorPtr ConfigurationRefreshExecutor_;
    const NLogging::TLogger Logger;

    TQueueConsumerRegistrationManagerProfilingCounters ProfilingCounters_;

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ConfigurationSpinLock_);
    TQueueConsumerRegistrationManagerConfigPtr DynamicConfig_;
};

DEFINE_REFCOUNTED_TYPE(TQueueConsumerRegistrationManagerBase)

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYT::NQueueClient
