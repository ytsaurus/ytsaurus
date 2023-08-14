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

    //! Contains path resolution context for better error messages.
    struct TGetRegistrationResult
    {
        std::optional<TConsumerRegistrationTableRow> Registration;
        NYPath::TRichYPath ResolvedQueue;
        NYPath::TRichYPath ResolvedConsumer;
    };

    // NB: May return stale results in regard to the other methods in this class.
    // NB: If cache bypass is enabled, this call will always refresh the cache itself.
    TGetRegistrationResult GetRegistration(
        NYPath::TRichYPath queue,
        NYPath::TRichYPath consumer);

    // NB: May return stale results in regard to the other methods in this class.
    // NB: If cache bypass is enabled, this call will always refresh the cache itself.
    std::vector<TConsumerRegistrationTableRow> ListRegistrations(
        std::optional<NYPath::TRichYPath> queue,
        std::optional<NYPath::TRichYPath> consumer);

    // NB: Using the registration cache immediately after this call may return stale results.
    void RegisterQueueConsumer(
        NYPath::TRichYPath queue,
        NYPath::TRichYPath consumer,
        bool vital,
        const std::optional<std::vector<int>>& partitions = {});

    // NB: Using the registration cache immediately after this call may return stale results.
    void UnregisterQueueConsumer(
        NYPath::TRichYPath queue,
        NYPath::TRichYPath consumer);

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
    THashMap<NYPath::TRichYPath, NYPath::TRichYPath> ReplicaToReplicatedTable_;

    //! Polls the registration dynamic table and fills the cached map.
    void RefreshCache();
    void GuardedRefreshCache();
    void GuardedRefreshReplicationTableMappingCache();

    //! Retrieves the dynamic config from cluster directory, applies it and stores a local copy.
    void RefreshConfiguration();
    void GuardedRefreshConfiguration();

    //! Attempts to establish a connection to the registration table's (potentially remote) cluster
    //! and returns a client to be used for modifying the table.
    TConsumerRegistrationTablePtr CreateRegistrationTableWriteClientOrThrow() const;
    //! Collects state rows from the clusters specified in state read path.
    //! The first successful response is returned.
    template <class TTable>
    std::vector<typename TTable::TRowType> FetchStateRowsOrThrow(
        const NYPath::TRichYPath& stateReadPath,
        const TString& user) const;

    //! Returns the stored dynamic config.
    TQueueConsumerRegistrationManagerConfigPtr GetDynamicConfig() const;

    //! Resolves physical path for object.
    //! If throwOnFailure is false, errors during resolution are written to the log only and the path is returned unchanged.
    NYPath::TRichYPath ResolveObjectPhysicalPath(
        const NYPath::TRichYPath& objectPath,
        const NTabletClient::TTableMountInfoPtr& tableMountInfo) const;
    //! Resolves the corresponding chaos_replicated_table/replicated_table path for chaos/replicated table replicas
    //! via the replicated table mapping cache.
    NYPath::TRichYPath ResolveReplica(
        const NYPath::TRichYPath& objectPath,
        const NTabletClient::TTableMountInfoPtr& tableMountInfo,
        bool throwOnFailure) const;

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

    //! Validates that a statically configured cluster name is set.
    void ValidateClusterNameConfigured() const;
};

DEFINE_REFCOUNTED_TYPE(TQueueConsumerRegistrationManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient
