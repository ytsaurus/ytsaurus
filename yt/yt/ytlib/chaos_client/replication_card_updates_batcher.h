#pragma once

#include "public.h"

#include <yt/yt/client/chaos_client/public.h>
#include <yt/yt/client/chaos_client/replication_card.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

struct TReplicaProgressUpdate
{
    TReplicaId ReplicaId;
    TReplicationProgress ReplicationProgressUpdate;
};

struct TReplicationCardProgressUpdate
{
    TReplicationCardId ReplicationCardId;
    std::vector<TReplicaProgressUpdate> ReplicaProgressUpdates;
    std::optional<TReplicationCardFetchOptions> FetchOptions;
};

struct TReplicationCardProgressUpdatesBatch
{
    std::vector<TReplicationCardProgressUpdate> ReplicationCardProgressUpdates;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TReplicaProgressUpdate& replicaProgressUpdates,
    TStringBuf spec);

void FormatValue(
    TStringBuilderBase* builder,
    const TReplicationCardProgressUpdate& replicationCardProgressUpdate,
    TStringBuf spec);

void FormatValue(
    TStringBuilderBase* builder,
    const TReplicationCardProgressUpdatesBatch& replicationCardProgressUpdatesBatch,
    TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

struct IReplicationCardUpdatesBatcher
    : public virtual TRefCounted
{
    using TBulkUpdateResult = std::vector<std::pair<TReplicationCardId, TFuture<TReplicationCardPtr>>>;

    virtual TBulkUpdateResult AddBulkReplicationCardProgressesUpdate(
        TReplicationCardProgressUpdatesBatch replicationCardProgressUpdateBatch) = 0;

    virtual TFuture<TReplicationCardPtr> AddReplicationCardProgressesUpdate(
        TReplicationCardProgressUpdate replicationCardProgressUpdate) = 0;

    virtual TFuture<TReplicationCardPtr> AddTabletProgressUpdate(
        TReplicationCardId replicationCardId,
        TReplicaId replicaId,
        const TReplicationProgress& tabletReplicationProgress) = 0;

    virtual void Start() = 0;
    virtual void Stop() = 0;

    virtual bool Enabled() const = 0;
    virtual void Reconfigure(const TChaosReplicationCardUpdatesBatcherConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IReplicationCardUpdatesBatcher)

IReplicationCardUpdatesBatcherPtr CreateMasterCacheReplicationCardUpdatesBatcher(
    const NChaosClient::TChaosReplicationCardUpdatesBatcherConfigPtr& config,
    NApi::NNative::IConnectionPtr connection,
    const IInvokerPtr& invoker,
    NLogging::TLogger logger);

IReplicationCardUpdatesBatcherPtr CreateClientReplicationCardUpdatesBatcher(
    const NChaosClient::TChaosReplicationCardUpdatesBatcherConfigPtr& config,
    NApi::NNative::IConnectionPtr connection,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
