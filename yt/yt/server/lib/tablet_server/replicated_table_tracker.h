#pragma once

#include "public.h"

#include <yt/yt/ytlib/tablet_client/table_replica_ypath.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/tablet_client/public.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

struct TChangeReplicaModeCommand
{
    NTabletClient::TTableReplicaId ReplicaId;
    NTabletClient::ETableReplicaMode TargetMode;
};

void FormatValue(
    TStringBuilderBase* builder,
    const TChangeReplicaModeCommand& command,
    TStringBuf spec);

TString ToString(const TChangeReplicaModeCommand& command);

////////////////////////////////////////////////////////////////////////////////

struct TReplicatedTableData
{
    NTableClient::TTableId Id;
    NTabletClient::TReplicatedTableOptionsPtr Options;
    NProfiling::TCounter ReplicaModeSwitchCounter;
};

struct TReplicaData
{
    NTableClient::TTableId TableId;
    NTabletClient::TTableReplicaId Id;
    NTabletClient::ETableReplicaMode Mode;
    bool Enabled;
    TString ClusterName;
    NYPath::TYPath TablePath;
    bool TrackingEnabled;
    // NB: RTT treats replicas with different ContentType independently.
    // Queue replicas are used in chaos. Sync queue replica count cannot be set to zero.
    NTabletClient::ETableReplicaContentType ContentType;
};

struct TTableCollocationData
{
    NTableClient::TTableCollocationId Id;
    std::vector<NTableClient::TTableId> TableIds;
};

struct TReplicatedTableTrackerSnapshot
{
    std::vector<TReplicatedTableData> ReplicatedTables;
    std::vector<TReplicaData> Replicas;
    std::vector<TTableCollocationData> Collocations;
};

using TReplicaLagTimes = std::vector<
    std::pair<NTabletClient::TTableReplicaId, std::optional<TDuration>>>;

using TApplyChangeReplicaCommandResults = std::vector<TError>;

////////////////////////////////////////////////////////////////////////////////

struct IReplicatedTableTrackerHost
    : public TRefCounted
{
    // COMPAT(akozhikhov): Drop with old RTT.
    virtual bool AlwaysUseNewReplicatedTableTracker() const = 0;

    virtual TFuture<TReplicatedTableTrackerSnapshot> GetSnapshot() = 0;

    virtual bool LoadingFromSnapshotRequested() const = 0;

    virtual void RequestLoadingFromSnapshot() = 0;

    virtual TFuture<TReplicaLagTimes> ComputeReplicaLagTimes(
        std::vector<NTabletClient::TTableReplicaId> replicaIds) = 0;

    virtual NApi::IClientPtr CreateClusterClient(const TString& clusterName) = 0;

    virtual TFuture<TApplyChangeReplicaCommandResults> ApplyChangeReplicaModeCommands(
        std::vector<TChangeReplicaModeCommand> commands) = 0;

    virtual void SubscribeReplicatedTableCreated(TCallback<void(TReplicatedTableData)> callback) = 0;
    virtual void SubscribeReplicatedTableDestroyed(TCallback<void(NTableClient::TTableId)> callback) = 0;
    virtual void SubscribeReplicatedTableOptionsUpdated(
        TCallback<void(NTableClient::TTableId, NTabletClient::TReplicatedTableOptionsPtr)> callback) = 0;

    virtual void SubscribeReplicaCreated(TCallback<void(TReplicaData)> callback) = 0;
    virtual void SubscribeReplicaDestroyed(
        TCallback<void(NTabletClient::TTableReplicaId)> callback) = 0;
    virtual void SubscribeReplicaModeUpdated(
        TCallback<void(NTabletClient::TTableReplicaId, NTabletClient::ETableReplicaMode)> callback) = 0;
    virtual void SubscribeReplicaEnablementUpdated(
        TCallback<void(NTabletClient::TTableReplicaId, bool)> callback) = 0;
    virtual void SubscribeReplicaTrackingPolicyUpdated(
        TCallback<void(NTabletClient::TTableReplicaId, bool)> callback) = 0;

    virtual void SubscribeReplicationCollocationUpdated(
        TCallback<void(TTableCollocationData)> callback) = 0;
    virtual void SubscribeReplicationCollocationDestroyed(
        TCallback<void(NTableClient::TTableCollocationId)> callback) = 0;

    virtual void SubscribeConfigChanged(
        TCallback<void(TDynamicReplicatedTableTrackerConfigPtr)> callback) = 0;
};

DEFINE_REFCOUNTED_TYPE(IReplicatedTableTrackerHost)

////////////////////////////////////////////////////////////////////////////////

struct IReplicatedTableTracker
    : public TRefCounted
{
    // When tracker is enabled it performs replica mode updates.
    // Only a single instance per cell should be enabled.
    virtual void EnableTracking() = 0;
    virtual void DisableTracking() = 0;

    // Invokes state loading from snapshot.
    virtual void Initialize() = 0;

    // Performs state loading from snapshot (used primarily for testing purposes).
    virtual void RequestLoadingFromSnapshot() = 0;
};

DEFINE_REFCOUNTED_TYPE(IReplicatedTableTracker)

////////////////////////////////////////////////////////////////////////////////

IReplicatedTableTrackerPtr CreateReplicatedTableTracker(
    IReplicatedTableTrackerHostPtr host,
    TDynamicReplicatedTableTrackerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
