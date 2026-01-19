#pragma once

#include "public.h"

#include "config.h"
#include "registration_manager.h"

namespace NYT::NQueueClient {

////////////////////////////////////////////////////////////////////////////////

class TQueueConsumerRegistrationManagerBase
    : public IQueueConsumerRegistrationManager
{
public:
    virtual void Initialize();

    void StartSync() override;
    void StopSync() override;

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

    virtual EQueueConsumerRegistrationManagerImplementation GetImplementationType() const = 0;

    virtual void Reconfigure(
        const TQueueConsumerRegistrationManagerConfigPtr& oldConfig,
        const TQueueConsumerRegistrationManagerConfigPtr& newConfig);

protected:
    // Virtual methods.

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

    virtual void RefreshCache() = 0;

    // Helper methods.

    TQueueConsumerRegistrationManagerBase(
        TQueueConsumerRegistrationManagerConfigPtr config,
        TWeakPtr<NApi::NNative::IConnection> connection,
        std::optional<std::string> clusterName,
        IInvokerPtr invoker,
        NProfiling::TProfiler profiler,
        NLogging::TLogger logger);

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

    //! Validates that a statically configured cluster name is set.
    void ValidateClusterNameConfigured() const;

    const TQueueConsumerRegistrationManagerConfigPtr Config_;
    // The connection holds a strong reference to this object.
    const TWeakPtr<NApi::NNative::IConnection> Connection_;
    const IInvokerPtr Invoker_;
    const std::optional<std::string> ClusterName_;
    const NLogging::TLogger Logger;

    TQueueConsumerRegistrationManagerProfilingCounters ProfilingCounters_;

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, ConfigurationSpinLock_);
    TQueueConsumerRegistrationManagerConfigPtr DynamicConfig_;

    //! Verifies that config->Implementation is equal to #GetImplementationType().
    //! Crashes if not.
    void VerifyConfigImplementation(const TQueueConsumerRegistrationManagerConfigPtr& config);
};

DEFINE_REFCOUNTED_TYPE(TQueueConsumerRegistrationManagerBase)

////////////////////////////////////////////////////////////////////////////////

}  // namespace NYT::NQueueClient
