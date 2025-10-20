#pragma once

#include "client_common.h"
#include "dynamic_table_client.h"

#include <yt/yt/client/table_client/chunk_stripe_statistics.h>
#include <yt/yt/client/table_client/columnar_statistics.h>
#include <yt/yt/client/table_client/external_source_spec.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/chaos_client/replication_card.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TDetailedProfilingInfo final
{
    bool EnableDetailedTableProfiling = false;
    NYPath::TYPath TablePath;
    TDuration MountCacheWaitTime;
    TDuration PermissionCacheWaitTime;

    int WastedSubrequestCount = 0;

    std::vector<TErrorCode> RetryReasons;
};

DEFINE_REFCOUNTED_TYPE(TDetailedProfilingInfo)

struct TTableReaderOptions
    : public TTransactionalOptions
    , public TSuppressableAccessTrackingOptions
{
    bool Unordered = false;
    bool OmitInaccessibleColumns = false;
    bool EnableTableIndex = false;
    bool EnableRowIndex = false;
    bool EnableRangeIndex = false;
    bool EnableTabletIndex = false;
    NTableClient::TTableReaderConfigPtr Config;
};

struct TTableWriterOptions
    : public TTransactionalOptions
{
    bool ValidateAnyIsValidYson = false;

    NTableClient::TTableWriterConfigPtr Config;
};

DEFINE_ENUM(EAttachTableMode,
    (Sequential)
    (Parallel)
);

DEFINE_ENUM(EAttachTableSourceOrder,
    // Source URIs are processed in the order they are produced from the source spec.
    (None)
    // Source URIs are processed in lexicographical order.
    (LexAsc)
    // Source URIs are processed in reverse lexicographical order.
    (LexDesc)
);

struct TAttachTableOptions
    : public TTransactionalOptions
    , public TTimeoutOptions
{
    //! If true, allows attaching tables even if their source schemas are incompatible.
    //! If the destination table does not exist, it will be created with an empty weak schema.
    bool AllowIncompatibleSourceSchemas = false;

    //! If the destination table does not exist, it will be created with this medium as primary.
    //! Otherwise, the medium of the existing table is used and must match the value of this field, if specified.
    std::optional<TString> Medium;

    //! If set, overrides the source format deduced from source URI file extensions.
    //! This override is applied to *all* source URIs, so there is no way to attach
    //! a table with mixed source formats via this option.
    //! This option is useful if your source URIs do not have standard file extensions.
    std::optional<NChunkClient::EExternalSourceFormat> SourceFormat;

    //! Controls how source URIs produced from the specified source spec are processed.
    //! In sequential mode, sources are attached to the table one by one in the order specified by `source_order`.
    //! In parallel mode (default), multiple sources can be attached concurrently, so there are no ordering guarantees.
    //! NB: It makes no sense to use sequential mode if the ordering of source URIs is not guaranteed either by
    //! the source spec itself (e.g. it is an explicit list of source URIs) or by the source order option below.
    EAttachTableMode AttachMode = EAttachTableMode::Parallel;

    //! Controls the order in which source URIs are fed into the attach process.
    //! In *sequential* attach mode, this guarantees that data from sources will appear in the table in the specified order.
    //! By default sources are processed in the order they are produced from the source spec, which may be unspecified.
    EAttachTableSourceOrder SourceOrder = EAttachTableSourceOrder::None;
};

struct TAttachedChunkInfo
{
    NChunkClient::TChunkId ChunkId;
    i64 RowCount = 0;
    i64 UncompressedDataSize = 0;
    std::string SourceUri;
    NChunkClient::EExternalSourceFormat SourceFormat;
    NChunkClient::EChunkFormat ChunkFormat;
};

struct TAttachTableResult
{
    i64 TotalChunkCount = 0;
    i64 TotalRowCount = 0;
    i64 TotalUncompressedDataSize = 0;
    std::vector<TAttachedChunkInfo> ChunkInfos;
};

struct TTabletRangeOptions
{
    std::optional<int> FirstTabletIndex;
    std::optional<int> LastTabletIndex;
};

struct TMountTableOptions
    : public TTimeoutOptions
    , public TMutatingOptions
    , public TTabletRangeOptions
{
    NTabletClient::TTabletCellId CellId = NTabletClient::NullTabletCellId;
    std::vector<NTabletClient::TTabletCellId> TargetCellIds;
    bool Freeze = false;
};

struct TUnmountTableOptions
    : public TTimeoutOptions
    , public TMutatingOptions
    , public TTabletRangeOptions
{
    bool Force = false;
};

struct TRemountTableOptions
    : public TTimeoutOptions
    , public TMutatingOptions
    , public TTabletRangeOptions
{ };

struct TFreezeTableOptions
    : public TTimeoutOptions
    , public TMutatingOptions
    , public TTabletRangeOptions
{ };

struct TUnfreezeTableOptions
    : public TTimeoutOptions
    , public TMutatingOptions
    , public TTabletRangeOptions
{ };

struct TReshardTableOptions
    : public TTimeoutOptions
    , public TMutatingOptions
    , public TTabletRangeOptions
{
    std::optional<bool> Uniform;
    std::optional<bool> EnableSlicing;
    std::optional<double> SlicingAccuracy;
    std::vector<i64> TrimmedRowCounts;
};

struct TReshardTableAutomaticOptions
    : public TTimeoutOptions
    , public TMutatingOptions
    , public TTabletRangeOptions
{
    bool KeepActions = false;
};

struct TAlterTableOptions
    : public TTimeoutOptions
    , public TMutatingOptions
    , public TTransactionalOptions
{
    std::optional<NTableClient::TTableSchema> Schema;
    std::optional<NTableClient::TMasterTableSchemaId> SchemaId;
    std::optional<bool> Dynamic;
    std::optional<NTabletClient::TTableReplicaId> UpstreamReplicaId;
    std::optional<NTableClient::ETableSchemaModification> SchemaModification;
    std::optional<NChaosClient::TReplicationProgress> ReplicationProgress;
};

struct TTrimTableOptions
    : public TTimeoutOptions
{ };

struct TAlterTableReplicaOptions
    : public TTimeoutOptions
    , public TMutatingOptions
{
    std::optional<bool> Enabled;
    std::optional<NTabletClient::ETableReplicaMode> Mode;
    std::optional<bool> PreserveTimestamps;
    std::optional<NTransactionClient::EAtomicity> Atomicity;
    std::optional<bool> EnableReplicatedTableTracker;
    std::optional<NYPath::TYPath> ReplicaPath;
};

struct TGetTablePivotKeysOptions
    : public TTimeoutOptions
{
    bool RepresentKeyAsList = false;
};

struct TGetInSyncReplicasOptions
    : public TTimeoutOptions
    , public TSyncReplicaCacheOptions
{
    NTransactionClient::TTimestamp Timestamp = NTransactionClient::NullTimestamp;
};

struct TGetTabletInfosOptions
    : public TTimeoutOptions
{
    bool RequestErrors = false;
};

struct TTabletInfo
{
    struct TTableReplicaInfo
    {
        NTabletClient::TTableReplicaId ReplicaId;
        NTransactionClient::TTimestamp LastReplicationTimestamp;
        NTabletClient::ETableReplicaMode Mode;
        i64 CurrentReplicationRowIndex;
        i64 CommittedReplicationRowIndex;
        TError ReplicationError;
    };

    //! Currently only provided for ordered tablets.
    //! Indicates the total number of rows added to the tablet (including trimmed ones).
    // TODO(babenko): implement for sorted tablets
    i64 TotalRowCount = 0;

    //! Only makes sense for ordered tablet.
    //! Contains the number of front rows that are trimmed and are not guaranteed to be accessible.
    i64 TrimmedRowCount = 0;

    //! Only makes sense for replicated tablets.
    //! Contains the number of rows that are yet to be committed.
    i64 DelayedLocklessRowCount = 0;

    //! Mostly makes sense for ordered tablets.
    //! Contains the barrier timestamp of the tablet cell containing the tablet, which lags behind the current timestamp.
    //! It is guaranteed that all transactions with commit timestamp not exceeding the barrier
    //! are fully committed; e.g. all their added rows are visible (and are included in TTabletInfo::TotalRowCount).
    NTransactionClient::TTimestamp BarrierTimestamp;

    //! Contains maximum timestamp of committed transactions.
    NTransactionClient::TTimestamp LastWriteTimestamp;

    //! Only makes sense for replicated tablets.
    std::optional<std::vector<TTableReplicaInfo>> TableReplicaInfos;

    //! Set if `RequestErrors` is present in command parameters.
    std::vector<TError> TabletErrors;
};

struct TGetTabletErrorsOptions
    : public TTimeoutOptions
{
    std::optional<i64> Limit;
};

struct TGetTabletErrorsResult
{
    bool Incomplete = false;
    THashMap<NTabletClient::TTabletId, std::vector<TError>> TabletErrors;
    THashMap<NTabletClient::TTableReplicaId, std::vector<TError>> ReplicationErrors;
};

struct TBalanceTabletCellsOptions
    : public TTimeoutOptions
    , public TMutatingOptions
{
    bool KeepActions = false;
};

struct TTableBackupManifest
    : public NYTree::TYsonStruct
{
    NYTree::TYPath SourcePath;
    NYTree::TYPath DestinationPath;
    NTabletClient::EOrderedTableBackupMode OrderedMode;

    REGISTER_YSON_STRUCT(TTableBackupManifest);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTableBackupManifest)

struct TBackupManifest
    : public NYTree::TYsonStruct
{
    THashMap<TString, std::vector<TTableBackupManifestPtr>> Clusters;

    REGISTER_YSON_STRUCT(TBackupManifest);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBackupManifest)

struct TCreateTableBackupOptions
    : public TTimeoutOptions
{
    TDuration CheckpointTimestampDelay = TDuration::Zero();
    TDuration CheckpointCheckPeriod = TDuration::Zero();
    TDuration CheckpointCheckTimeout = TDuration::Zero();

    bool Force = false;
    bool PreserveAccount = false;
};

struct TRestoreTableBackupOptions
    : public TTimeoutOptions
{
    bool Force = false;
    bool Mount = false;
    bool EnableReplicas = false;
    bool PreserveAccount = false;
};

struct TUpdateChaosTableReplicaProgressOptions
    : public TTimeoutOptions
    , public TMutatingOptions
{
    NChaosClient::TReplicationProgress Progress;
    bool Force;
};

struct TAlterReplicationCardOptions
    : public TTimeoutOptions
    , public TMutatingOptions
{
    NTabletClient::TReplicatedTableOptionsPtr ReplicatedTableOptions;
    std::optional<bool> EnableReplicatedTableTracker;
    std::optional<NChaosClient::TReplicationCardCollocationId> ReplicationCardCollocationId;
    NTabletClient::TReplicationCollocationOptionsPtr CollocationOptions;
};

struct TGetReplicationCardOptions
    : public TTimeoutOptions
    , public NChaosClient::TReplicationCardFetchOptions
{
    bool BypassCache = false;
};

struct TGetColumnarStatisticsOptions
    : public TTransactionalOptions
    , public TTimeoutOptions
{
    NChunkClient::TFetchChunkSpecConfigPtr FetchChunkSpecConfig;
    NChunkClient::TFetcherConfigPtr FetcherConfig;
    NTableClient::EColumnarStatisticsFetcherMode FetcherMode = NTableClient::EColumnarStatisticsFetcherMode::FromNodes;
    bool EnableEarlyFinish = true;
};

struct TPartitionTablesOptions
    : public TTransactionalOptions
    , public TTimeoutOptions
{
    NChunkClient::TFetchChunkSpecConfigPtr FetchChunkSpecConfig;
    NChunkClient::TFetcherConfigPtr FetcherConfig;
    NChunkClient::TChunkSliceFetcherConfigPtr ChunkSliceFetcherConfig;
    NTableClient::ETablePartitionMode PartitionMode = NTableClient::ETablePartitionMode::Unordered;
    i64 DataWeightPerPartition;
    std::optional<int> MaxPartitionCount;
    bool AdjustDataWeightPerPartition = true;
    bool EnableKeyGuarantee = false;
};

struct TMultiTablePartition
{
    //! Table ranges are indexed by table index.
    std::vector<NYPath::TRichYPath> TableRanges;

    //! Aggregate statistics of all the table ranges in the partition.
    NTableClient::TChunkStripeStatistics AggregateStatistics;
};

void Serialize(const TMultiTablePartition& partitions, NYson::IYsonConsumer* consumer);

struct TMultiTablePartitions
{
    std::vector<TMultiTablePartition> Partitions;
};

void Serialize(const TMultiTablePartitions& partitions, NYson::IYsonConsumer* consumer);

struct TLocateSkynetShareOptions
    : public TTimeoutOptions
{
    NChunkClient::TFetchChunkSpecConfigPtr Config;
};

////////////////////////////////////////////////////////////////////////////////

struct ITableClientBase
    : public IDynamicTableClientBase
{
    virtual TFuture<ITableReaderPtr> CreateTableReader(
        const NYPath::TRichYPath& path,
        const TTableReaderOptions& options = {}) = 0;

    virtual TFuture<ITableWriterPtr> CreateTableWriter(
        const NYPath::TRichYPath& path,
        const TTableWriterOptions& options = {}) = 0;

    virtual TFuture<TAttachTableResult> AttachTable(
        const NYPath::TRichYPath& path,
        const NTableClient::TExternalSourceSpec& sourceSpec,
        const TAttachTableOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ITableClient
{
    virtual ~ITableClient() = default;

    virtual TFuture<void> MountTable(
        const NYPath::TYPath& path,
        const TMountTableOptions& options = {}) = 0;

    virtual TFuture<void> UnmountTable(
        const NYPath::TYPath& path,
        const TUnmountTableOptions& options = {}) = 0;

    virtual TFuture<void> RemountTable(
        const NYPath::TYPath& path,
        const TRemountTableOptions& options = {}) = 0;

    virtual TFuture<void> FreezeTable(
        const NYPath::TYPath& path,
        const TFreezeTableOptions& options = {}) = 0;

    virtual TFuture<void> UnfreezeTable(
        const NYPath::TYPath& path,
        const TUnfreezeTableOptions& options = {}) = 0;

    virtual TFuture<void> ReshardTable(
        const NYPath::TYPath& path,
        const std::vector<NTableClient::TLegacyOwningKey>& pivotKeys,
        const TReshardTableOptions& options = {}) = 0;

    virtual TFuture<void> ReshardTable(
        const NYPath::TYPath& path,
        int tabletCount,
        const TReshardTableOptions& options = {}) = 0;

    virtual TFuture<std::vector<NTabletClient::TTabletActionId>> ReshardTableAutomatic(
        const NYPath::TYPath& path,
        const TReshardTableAutomaticOptions& options = {}) = 0;

    virtual TFuture<void> TrimTable(
        const NYPath::TYPath& path,
        int tabletIndex,
        i64 trimmedRowCount,
        const TTrimTableOptions& options = {}) = 0;

    virtual TFuture<void> AlterTable(
        const NYPath::TYPath& path,
        const TAlterTableOptions& options = {}) = 0;

    virtual TFuture<void> AlterTableReplica(
        NTabletClient::TTableReplicaId replicaId,
        const TAlterTableReplicaOptions& options = {}) = 0;

    virtual TFuture<NYson::TYsonString> GetTablePivotKeys(
        const NYPath::TYPath& path,
        const TGetTablePivotKeysOptions& options = {}) = 0;

    virtual TFuture<void> CreateTableBackup(
        const TBackupManifestPtr& manifest,
        const TCreateTableBackupOptions& options = {}) = 0;

    virtual TFuture<void> RestoreTableBackup(
        const TBackupManifestPtr& manifest,
        const TRestoreTableBackupOptions& options = {}) = 0;

    virtual TFuture<std::vector<NTabletClient::TTableReplicaId>> GetInSyncReplicas(
        const NYPath::TYPath& path,
        const NTableClient::TNameTablePtr& nameTable,
        const TSharedRange<NTableClient::TLegacyKey>& keys,
        const TGetInSyncReplicasOptions& options = {}) = 0;

    //! Same as above but returns the list of replicas that are in sync w.r.t. all of the table keys.
    virtual TFuture<std::vector<NTabletClient::TTableReplicaId>> GetInSyncReplicas(
        const NYPath::TYPath& path,
        const TGetInSyncReplicasOptions& options = {}) = 0;

    virtual TFuture<std::vector<TTabletInfo>> GetTabletInfos(
        const NYPath::TYPath& path,
        const std::vector<int>& tabletIndexes,
        const TGetTabletInfosOptions& options = {}) = 0;

    virtual TFuture<TGetTabletErrorsResult> GetTabletErrors(
        const NYPath::TYPath& path,
        const TGetTabletErrorsOptions& options = {}) = 0;

    virtual TFuture<std::vector<NTabletClient::TTabletActionId>> BalanceTabletCells(
        const TString& tabletCellBundle,
        const std::vector<NYPath::TYPath>& movableTables,
        const TBalanceTabletCellsOptions& options = {}) = 0;

    virtual TFuture<NChaosClient::TReplicationCardPtr> GetReplicationCard(
        const NChaosClient::TReplicationCardId replicationCardId,
        const TGetReplicationCardOptions& options = {}) = 0;

    virtual TFuture<void> UpdateChaosTableReplicaProgress(
        NChaosClient::TReplicaId replicaId,
        const TUpdateChaosTableReplicaProgressOptions& options = {}) = 0;

    virtual TFuture<void> AlterReplicationCard(
        NChaosClient::TReplicationCardId replicationCardId,
        const TAlterReplicationCardOptions& options = {}) = 0;

    virtual TFuture<TSkynetSharePartsLocationsPtr> LocateSkynetShare(
        const NYPath::TRichYPath& path,
        const TLocateSkynetShareOptions& options = {}) = 0;

    virtual TFuture<std::vector<NTableClient::TColumnarStatistics>> GetColumnarStatistics(
        const std::vector<NYPath::TRichYPath>& path,
        const TGetColumnarStatisticsOptions& options = {}) = 0;

    virtual TFuture<TMultiTablePartitions> PartitionTables(
        const std::vector<NYPath::TRichYPath>& paths,
        const TPartitionTablesOptions& options) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
