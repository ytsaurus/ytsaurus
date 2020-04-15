#pragma once

#include "public.h"
#include "connection.h"

#include <yt/client/cypress_client/public.h>

#include <yt/client/job_tracker_client/public.h>

#include <yt/client/object_client/public.h>

#include <yt/client/query_client/query_statistics.h>

#include <yt/client/scheduler/operation_id_or_alias.h>

#include <yt/client/security_client/public.h>

#include <yt/client/node_tracker_client/public.h>
#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/client/table_client/config.h>
#include <yt/client/table_client/row_base.h>
#include <yt/client/table_client/schema.h>

#include <yt/client/table_client/columnar_statistics.h>

#include <yt/client/tablet_client/public.h>

#include <yt/client/chunk_client/config.h>

#include <yt/client/transaction_client/public.h>

#include <yt/client/driver/private.h>

#include <yt/client/ypath/public.h>

#include <yt/client/hive/timestamp_map.h>

#include <yt/core/actions/future.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/optional.h>

#include <yt/core/ytree/yson_serializable.h>

#include <yt/core/rpc/public.h>

#include <yt/core/ytree/permission.h>

#include <yt/core/yson/string.h>

#include <yt/core/profiling/profiler.h>

#include <yt/library/erasure/public.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TUserWorkloadDescriptor
{
    EUserWorkloadCategory Category = EUserWorkloadCategory::Interactive;
    int Band = 0;

    operator TWorkloadDescriptor() const;
};

void Serialize(const TUserWorkloadDescriptor& workloadDescriptor, NYson::IYsonConsumer* consumer);
void Deserialize(TUserWorkloadDescriptor& workloadDescriptor, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

struct TTimeoutOptions
{
    std::optional<TDuration> Timeout;
};

struct TTabletRangeOptions
{
    std::optional<int> FirstTabletIndex;
    std::optional<int> LastTabletIndex;
};

struct TTransactionalOptions
{
    //! Ignored when queried via transaction.
    NObjectClient::TTransactionId TransactionId;
    bool Ping = false;
    bool PingAncestors = false;
    // COMPAT(kiselyovp) remove Sticky (YT-10654)
    bool Sticky = false;
};

struct TSuppressableAccessTrackingOptions
{
    bool SuppressAccessTracking = false;
    bool SuppressModificationTracking = false;
};

struct TMutatingOptions
{
    NRpc::TMutationId MutationId;
    bool Retry = false;

    NRpc::TMutationId GetOrGenerateMutationId() const;
};

struct TMasterReadOptions
{
    EMasterChannelKind ReadFrom = EMasterChannelKind::Follower;
    TDuration ExpireAfterSuccessfulUpdateTime = TDuration::Seconds(15);
    TDuration ExpireAfterFailedUpdateTime = TDuration::Seconds(15);
    int CacheStickyGroupSize = 1;
};

struct TPrerequisiteRevisionConfig
    : public NYTree::TYsonSerializable
{
    NYTree::TYPath Path;
    NTransactionClient::TTransactionId TransactionId;
    NHydra::TRevision Revision;

    TPrerequisiteRevisionConfig()
    {
        RegisterParameter("path", Path);
        RegisterParameter("transaction_id", TransactionId);
        RegisterParameter("revision", Revision);
    }
};

DEFINE_REFCOUNTED_TYPE(TPrerequisiteRevisionConfig)

struct TPrerequisiteOptions
{
    std::vector<NTransactionClient::TTransactionId> PrerequisiteTransactionIds;
    std::vector<TPrerequisiteRevisionConfigPtr> PrerequisiteRevisions;
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
{ };

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
    std::optional<bool> Dynamic;
    std::optional<NTabletClient::TTableReplicaId> UpstreamReplicaId;
    std::optional<NTableClient::ETableSchemaModification> SchemaModification;
};

struct TTrimTableOptions
    : public TTimeoutOptions
{ };

struct TAlterTableReplicaOptions
    : public TTimeoutOptions
{
    std::optional<bool> Enabled;
    std::optional<NTabletClient::ETableReplicaMode> Mode;
    std::optional<bool> PreserveTimestamps;
    std::optional<NTransactionClient::EAtomicity> Atomicity;
};

struct TGetTablePivotKeysOptions
    : public TTimeoutOptions
{ };

struct TGetInSyncReplicasOptions
    : public TTimeoutOptions
{
    NTransactionClient::TTimestamp Timestamp = NTransactionClient::NullTimestamp;
};

struct TGetTabletsInfoOptions
    : public TTimeoutOptions
{ };

struct TTabletInfo
{
    struct TTableReplicaInfo
    {
        NTabletClient::TTableReplicaId ReplicaId;
        NTransactionClient::TTimestamp LastReplicationTimestamp;
        NTabletClient::ETableReplicaMode Mode;
        i64 CurrentReplicationRowIndex;
    };

    //! Currently only provided for ordered tablets.
    //! Indicates the total number of rows added to the tablet (including trimmed ones).
    // TODO(babenko): implement for sorted tablets
    i64 TotalRowCount = 0;

    //! Only makes sense for ordered tablet.
    //! Contains the number of front rows that are trimmed and are not guaranteed to be accessible.
    i64 TrimmedRowCount = 0;

    //! Mostly makes sense for ordered tablets.
    //! Contains the barrier timestamp of the tablet cell containing the tablet, which lags behind the current timestamp.
    //! It is guaranteed that all transactions with commit timestamp not exceeding the barrier
    //! are fully committed; e.g. all their addes rows are visible (and are included in TTabletInfo::TotalRowCount).
    NTransactionClient::TTimestamp BarrierTimestamp;

    //! Contains maximum timestamp of committed transactions.
    NTransactionClient::TTimestamp LastWriteTimestamp;

    //! Only makes sense for replicated tablets.
    std::optional<std::vector<TTableReplicaInfo>> TableReplicaInfos;
};

struct TBalanceTabletCellsOptions
    : public TTimeoutOptions
    , public TMutatingOptions
{
    bool KeepActions = false;
};

struct TAddMemberOptions
    : public TTimeoutOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{ };

struct TRemoveMemberOptions
    : public TTimeoutOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{ };

struct TCheckPermissionOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
    , public TTransactionalOptions
    , public TPrerequisiteOptions
{
    std::optional<std::vector<TString>> Columns;
};

struct TCheckPermissionResult
{
    TError ToError(
        const TString& user,
        NYTree::EPermission permission,
        const std::optional<TString>& columns = {}) const;

    NSecurityClient::ESecurityAction Action;
    NObjectClient::TObjectId ObjectId;
    std::optional<TString> ObjectName;
    NSecurityClient::TSubjectId SubjectId;
    std::optional<TString> SubjectName;
};

struct TCheckPermissionResponse
    : public TCheckPermissionResult
{
    std::optional<std::vector<TCheckPermissionResult>> Columns;
};

struct TCheckPermissionByAclOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
    , public TPrerequisiteOptions
{
    bool IgnoreMissingSubjects = false;
};

struct TCheckPermissionByAclResult
{
    TError ToError(const TString& user, NYTree::EPermission permission) const;

    NSecurityClient::ESecurityAction Action;
    NSecurityClient::TSubjectId SubjectId;
    std::optional<TString> SubjectName;
    std::vector<TString> MissingSubjects;
};

// TODO(lukyan): Use TTransactionalOptions as base class
struct TTransactionStartOptions
    : public TMutatingOptions
{
    std::optional<TDuration> Timeout;

    //! If not null then the transaction must use this externally provided id.
    //! Only applicable to tablet transactions.
    NTransactionClient::TTransactionId Id;

    NTransactionClient::TTransactionId ParentId;
    std::vector<NTransactionClient::TTransactionId> PrerequisiteTransactionIds;

    std::optional<TInstant> Deadline;

    bool AutoAbort = true;
    bool Sticky = false;

    std::optional<TDuration> PingPeriod;
    bool Ping = true;
    bool PingAncestors = true;

    std::shared_ptr<const NYTree::IAttributeDictionary> Attributes;

    NTransactionClient::EAtomicity Atomicity = NTransactionClient::EAtomicity::Full;
    NTransactionClient::EDurability Durability = NTransactionClient::EDurability::Sync;

    //! If not null then the transaction must use this externally provided start timestamp.
    //! Only applicable to tablet transactions.
    NTransactionClient::TTimestamp StartTimestamp = NTransactionClient::NullTimestamp;

    //! Only for master transactions.
    //! Indicates the master cell the transaction will be initially started at and controlled by
    //! (primary cell by default).
    NObjectClient::TCellTag CoordinatorMasterCellTag = NObjectClient::InvalidCellTag;

    //! Only for master transactions.
    //! Indicates the cells the transaction will be replicated to (all by default).
    std::optional<NObjectClient::TCellTagList> ReplicateToMasterCellTags;
};

struct TTransactionAttachOptions
{
    bool AutoAbort = false;
    // COMPAT(kiselyovp) remove Sticky (YT-10654)
    bool Sticky = false;
    std::optional<TDuration> PingPeriod;
    bool Ping = true;
    bool PingAncestors = false;
};

struct TTransactionCommitOptions
    : public TMutatingOptions
    , public TPrerequisiteOptions
    , public TTransactionalOptions
{
    //! If not null, then this particular cell will be the coordinator.
    NObjectClient::TCellId CoordinatorCellId;

    //! If |true| then two-phase-commit protocol is executed regardless of the number of participants.
    bool Force2PC = false;

    //! For 2PC, controls is success is reported eagerly or lazyly.
    ETransactionCoordinatorCommitMode CoordinatorCommitMode = ETransactionCoordinatorCommitMode::Eager;

    //! If |true| then all participants will use the commit timestamp provided by the coordinator.
    //! If |false| then the participants will use individual commit timestamps based on their cell tag.
    bool InheritCommitTimestamp = true;

    //! If |true| then the coordinator will generate a non-null prepare timestamp (which is a lower bound for
    //! the upcoming commit timestamp) and send it to all the participants.
    //! If |false| then no prepare timestamp is generated and null value is provided to the participants.
    //! The latter is useful for async replication that does not involve any local write operations
    //! and also relies on ETransactionCoordinatorCommitMode::Lazy transactions whose commit may be delayed
    //! for an arbitrary period of time in case of replica failure.
    bool GeneratePrepareTimestamp = true;

    //! Cell ids of additional 2PC participants.
    //! Used to implement cross-cluster commit via RPC proxy.
    std::vector<NObjectClient::TCellId> AdditionalParticipantCellIds;
};

struct TTransactionPingOptions
{
    bool EnableRetries = true;
};

struct TTransactionCommitResult
{
    //! Empty for non-atomic transactions (timestamps are fake).
    //! Empty for empty tablet transactions (since the commit is essentially no-op).
    //! May contain multiple items for cross-cluster commit.
    NHiveClient::TTimestampMap CommitTimestamps;
};

struct TTransactionAbortOptions
    : public TMutatingOptions
    , public TPrerequisiteOptions
    , public TTransactionalOptions
{
    bool Force = false;
};

struct TTabletReadOptions
    : public TTimeoutOptions
{
    NHydra::EPeerKind ReadFrom = NHydra::EPeerKind::Leader;
    std::optional<TDuration> BackupRequestDelay;
    //! Ignored when queried via transaction.
    NTransactionClient::TTimestamp Timestamp = NTransactionClient::SyncLastCommittedTimestamp;
};

struct TLookupRowsOptionsBase
    : public TTabletReadOptions
{
    NTableClient::TColumnFilter ColumnFilter;
    bool KeepMissingRows = false;
    bool EnablePartialResult = false;
    NRpc::EMultiplexingBand MultiplexingBand = NRpc::EMultiplexingBand::Default;
};

struct TLookupRowsOptions
    : public TLookupRowsOptionsBase
{ };

struct TVersionedLookupRowsOptions
    : public TLookupRowsOptionsBase
{
    NTableClient::TRetentionConfigPtr RetentionConfig;
};

struct TSelectRowsOptionsBase
    : public TTabletReadOptions
    , public TSuppressableAccessTrackingOptions
{
    //! Path in Cypress with UDFs.
    std::optional<TString> UdfRegistryPath;
};

struct TSelectRowsOptions
    : public TSelectRowsOptionsBase
{
    //! If null then connection defaults are used.
    std::optional<i64> InputRowLimit;
    //! If null then connection defaults are used.
    std::optional<i64> OutputRowLimit;
    //! Limits range expanding.
    ui64 RangeExpansionLimit = 200000;
    //! Limits maximum parallel subqueries.
    int MaxSubqueries = std::numeric_limits<int>::max();
    //! Combine independent joins in one.
    bool UseMultijoin = true;
    //! Allow queries without any condition on key columns.
    bool AllowFullScan = true;
    //! Allow queries with join condition which implies foreign query with IN operator.
    bool AllowJoinWithoutIndex = false;
    //! Execution pool
    std::optional<TString> ExecutionPool;
    //! If |true| then incomplete result would lead to a failure.
    bool FailOnIncompleteResult = true;
    //! If |true| then logging is more verbose.
    bool VerboseLogging = false;
    //! Enables generated code caching.
    bool EnableCodeCache = true;
    //! Used to prioritize requests.
    TUserWorkloadDescriptor WorkloadDescriptor;
    //! Memory limit per execution node
    size_t MemoryLimitPerNode = std::numeric_limits<size_t>::max();
};

struct TExplainQueryOptions
    : public TSelectRowsOptionsBase
{};

struct TGetNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMasterReadOptions
    , public TSuppressableAccessTrackingOptions
    , public TPrerequisiteOptions
{
    // XXX(sandello): This one is used only in ProfileManager to pass `from_time`.
    std::shared_ptr<const NYTree::IAttributeDictionary> Options;
    std::optional<std::vector<TString>> Attributes;
    std::optional<i64> MaxSize;
};

struct TSetNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
    , public TSuppressableAccessTrackingOptions
    , public TPrerequisiteOptions
{
    bool Recursive = false;
    bool Force = false;
};

struct TRemoveNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{
    bool Recursive = true;
    bool Force = false;
};

struct TListNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMasterReadOptions
    , public TSuppressableAccessTrackingOptions
    , public TPrerequisiteOptions
{
    std::optional<std::vector<TString>> Attributes;
    std::optional<i64> MaxSize;
};

struct TCreateObjectOptions
    : public TTimeoutOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{
    bool IgnoreExisting = false;
    std::shared_ptr<const NYTree::IAttributeDictionary> Attributes;
};

struct TCreateNodeOptions
    : public TCreateObjectOptions
    , public TTransactionalOptions
{
    bool Recursive = false;
    bool LockExisting = false;
    bool Force = false;
    bool IgnoreTypeMismatch = false;
};

struct TLockNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{
    bool Waitable = false;
    std::optional<TString> ChildKey;
    std::optional<TString> AttributeKey;
};

struct TLockNodeResult
{
    NCypressClient::TLockId LockId;
    NCypressClient::TNodeId NodeId;
    NHydra::TRevision Revision;
};

struct TUnlockNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{ };

struct TCopyNodeOptionsBase
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{
    bool Recursive = false;
    bool Force = false;
    bool PreserveAccount = false;
    bool PreserveCreationTime = false;
    bool PreserveModificationTime = false;
    bool PreserveOwner = false;
    bool PreserveExpirationTime = false;
    bool PreserveAcl = false;
    bool PessimisticQuotaCheck = true;
};

struct TCopyNodeOptions
    : public TCopyNodeOptionsBase
{
    bool IgnoreExisting = false;
    bool LockExisting = false;
};

struct TMoveNodeOptions
    : public TCopyNodeOptionsBase
{ };

struct TLinkNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{
    //! Attributes of a newly created link node.
    std::shared_ptr<const NYTree::IAttributeDictionary> Attributes;
    bool Recursive = false;
    bool IgnoreExisting = false;
    bool LockExisting = false;
    bool Force = false;
};

struct TConcatenateNodesOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
{
    NChunkClient::TFetcherConfigPtr ChunkMetaFetcherConfig;
};

struct TNodeExistsOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
    , public TTransactionalOptions
    , public TSuppressableAccessTrackingOptions
    , public TPrerequisiteOptions
{ };

struct TExternalizeNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
{ };

struct TInternalizeNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
{ };

struct TFileReaderOptions
    : public TTransactionalOptions
    , public TSuppressableAccessTrackingOptions
{
    std::optional<i64> Offset;
    std::optional<i64> Length;
    TFileReaderConfigPtr Config;
};

struct TFileWriterOptions
    : public TTransactionalOptions
    , public TPrerequisiteOptions
{
    bool ComputeMD5 = false;
    TFileWriterConfigPtr Config;
};

struct TGetFileFromCacheOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
    , public TTransactionalOptions
{
    NYPath::TYPath CachePath;
};

struct TPutFileToCacheOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
    , public TTransactionalOptions
{
    NYPath::TYPath CachePath;
    int RetryCount = 10;
};

struct TJournalReaderOptions
    : public TTransactionalOptions
    , public TSuppressableAccessTrackingOptions
{
    std::optional<i64> FirstRowIndex;
    std::optional<i64> RowCount;
    TJournalReaderConfigPtr Config;
};

struct TJournalWriterOptions
    : public TTransactionalOptions
    , public TPrerequisiteOptions
{
    TJournalWriterConfigPtr Config;
    bool EnableMultiplexing = true;
    bool WaitForAllReplicasUponOpen = false;
    NProfiling::TProfiler Profiler;
};

struct TTruncateJournalOptions
    : public TTimeoutOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{ };

struct TTableReaderOptions
    : public TTransactionalOptions
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
    NTableClient::TTableWriterConfigPtr Config;
};

struct TGetColumnarStatisticsOptions
    : public TTransactionalOptions
    , public TTimeoutOptions
{
    NChunkClient::TFetchChunkSpecConfigPtr FetchChunkSpecConfig;
    NChunkClient::TFetcherConfigPtr FetcherConfig;
};

struct TLocateSkynetShareOptions
    : public TTimeoutOptions
{
    NChunkClient::TFetchChunkSpecConfigPtr Config;
};

struct TStartOperationOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
{ };

struct TAbortOperationOptions
    : public TTimeoutOptions
{
    std::optional<TString> AbortMessage;
};

struct TSuspendOperationOptions
    : public TTimeoutOptions
{
    bool AbortRunningJobs = false;
};

struct TResumeOperationOptions
    : public TTimeoutOptions
{ };

struct TCompleteOperationOptions
    : public TTimeoutOptions
{ };

struct TUpdateOperationParametersOptions
    : public TTimeoutOptions
{ };

struct TDumpJobContextOptions
    : public TTimeoutOptions
{ };

struct TGetJobInputOptions
    : public TTimeoutOptions
{ };

struct TGetJobInputPathsOptions
    : public TTimeoutOptions
{ };


struct TGetJobStderrOptions
    : public TTimeoutOptions
{ };

struct TGetJobFailContextOptions
    : public TTimeoutOptions
{ };

DEFINE_ENUM(EOperationSortDirection,
    ((None)   (0))
    ((Past)   (1))
    ((Future) (2))
);

struct TListOperationsAccessFilter
    : NYTree::TYsonSerializable
{
    TString Subject;
    NYTree::EPermissionSet Permissions;

    // This parameter cannot be set from YSON, it must be computed.
    THashSet<TString> SubjectTransitiveClosure;

    TListOperationsAccessFilter()
    {
        RegisterParameter("subject", Subject);
        RegisterParameter("permissions", Permissions);
    }
};

DECLARE_REFCOUNTED_TYPE(TListOperationsAccessFilter);
DEFINE_REFCOUNTED_TYPE(TListOperationsAccessFilter);

struct TListOperationsOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
{
    std::optional<TInstant> FromTime;
    std::optional<TInstant> ToTime;
    std::optional<TInstant> CursorTime;
    EOperationSortDirection CursorDirection = EOperationSortDirection::Past;
    std::optional<TString> UserFilter;

    TListOperationsAccessFilterPtr AccessFilter;

    std::optional<NScheduler::EOperationState> StateFilter;
    std::optional<NScheduler::EOperationType> TypeFilter;
    std::optional<TString> SubstrFilter;
    std::optional<TString> Pool;
    std::optional<bool> WithFailedJobs;
    bool IncludeArchive = false;
    bool IncludeCounters = true;
    ui64 Limit = 100;

    std::optional<THashSet<TString>> Attributes;

    // TODO(ignat): Remove this mode when UI migrate to list_operations without enabled UI mode.
    // See st/YTFRONT-1360.
    bool EnableUIMode = false;

    TDuration ArchiveFetchingTimeout = TDuration::Seconds(3);

    TListOperationsOptions()
    {
        ReadFrom = EMasterChannelKind::Cache;
    }
};

DEFINE_ENUM(EJobSortField,
    ((None)       (0))
    ((Type)       (1))
    ((State)      (2))
    ((StartTime)  (3))
    ((FinishTime) (4))
    ((Address)    (5))
    ((Duration)   (6))
    ((Progress)   (7))
    ((Id)         (8))
);

DEFINE_ENUM(EJobSortDirection,
    ((Ascending)  (0))
    ((Descending) (1))
);

DEFINE_ENUM(EDataSource,
    ((Archive) (0))
    ((Runtime) (1))
    ((Auto)    (2))
    // Should be used only in tests.
    ((Manual)  (3))
);

struct TListJobsOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
{
    NJobTrackerClient::TJobId JobCompetitionId;
    std::optional<NJobTrackerClient::EJobType> Type;
    std::optional<NJobTrackerClient::EJobState> State;
    std::optional<TString> Address;
    std::optional<bool> WithStderr;
    std::optional<bool> WithFailContext;
    std::optional<bool> WithSpec;
    std::optional<bool> WithCompetitors;

    EJobSortField SortField = EJobSortField::None;
    EJobSortDirection SortOrder = EJobSortDirection::Ascending;

    i64 Limit = 1000;
    i64 Offset = 0;

    bool IncludeCypress = false;
    bool IncludeControllerAgent = false;
    bool IncludeArchive = false;

    EDataSource DataSource = EDataSource::Auto;

    TDuration RunningJobsLookbehindPeriod = TDuration::Max();
};

struct TStraceJobOptions
    : public TTimeoutOptions
{ };

struct TSignalJobOptions
    : public TTimeoutOptions
{ };

struct TAbandonJobOptions
    : public TTimeoutOptions
{ };

struct TPollJobShellOptions
    : public TTimeoutOptions
{ };

struct TAbortJobOptions
    : public TTimeoutOptions
{
    std::optional<TDuration> InterruptTimeout;
};

struct TGetOperationOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
{
    std::optional<THashSet<TString>> Attributes;
    TDuration ArchiveTimeout = TDuration::Seconds(3);
    bool IncludeRuntime = false;
};

struct TGetJobOptions
    : public TTimeoutOptions
{
    std::optional<THashSet<TString>> Attributes;
};

struct TSelectRowsResult
{
    IUnversionedRowsetPtr Rowset;
    NQueryClient::TQueryStatistics Statistics;
};

struct TGetClusterMetaOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
{
    bool PopulateNodeDirectory = false;
    bool PopulateClusterDirectory = false;
    bool PopulateMediumDirectory = false;
    bool PopulateCellDirectory = false;
    bool PopulateMasterCacheNodeAddresses = false;
    bool PopulateTimestampProviderAddresses = false;
};

struct TClusterMeta
{
    std::shared_ptr<NNodeTrackerClient::NProto::TNodeDirectory> NodeDirectory;
    std::shared_ptr<NHiveClient::NProto::TClusterDirectory> ClusterDirectory;
    std::shared_ptr<NChunkClient::NProto::TMediumDirectory> MediumDirectory;
    std::vector<TString> MasterCacheNodeAddresses;
    std::vector<TString> TimestampProviderAddresses;
};

struct TOperation
{
    std::optional<NScheduler::TOperationId> Id;

    std::optional<NScheduler::EOperationType> Type;
    std::optional<NScheduler::EOperationState> State;

    std::optional<TInstant> StartTime;
    std::optional<TInstant> FinishTime;

    std::optional<TString> AuthenticatedUser;
    NYTree::INodePtr Acl;

    NYson::TYsonString BriefSpec;
    NYson::TYsonString Spec;
    NYson::TYsonString FullSpec;
    NYson::TYsonString UnrecognizedSpec;

    NYson::TYsonString BriefProgress;
    NYson::TYsonString Progress;

    NYson::TYsonString RuntimeParameters;

    std::optional<bool> Suspended;

    NYson::TYsonString Events;
    NYson::TYsonString Result;

    NYson::TYsonString SlotIndexPerPoolTree;
    NYson::TYsonString Alerts;
};

struct TListOperationsResult
{
    std::vector<TOperation> Operations;
    std::optional<THashMap<TString, i64>> PoolCounts;
    std::optional<THashMap<TString, i64>> UserCounts;
    std::optional<TEnumIndexedVector<NScheduler::EOperationState, i64>> StateCounts;
    std::optional<TEnumIndexedVector<NScheduler::EOperationType, i64>> TypeCounts;
    std::optional<i64> FailedJobsCount;
    bool Incomplete = false;
};

struct TJob
{
    NJobTrackerClient::TJobId Id;
    NJobTrackerClient::TJobId OperationId;
    std::optional<NJobTrackerClient::EJobType> Type;
    std::optional<NJobTrackerClient::EJobState> State;
    std::optional<NJobTrackerClient::EJobState> ControllerAgentState;
    std::optional<NJobTrackerClient::EJobState> ArchiveState;
    std::optional<TInstant> StartTime;
    std::optional<TInstant> FinishTime;
    std::optional<TString> Address;
    std::optional<double> Progress;
    std::optional<ui64> StderrSize;
    std::optional<ui64> FailContextSize;
    std::optional<bool> HasSpec;
    std::optional<bool> HasCompetitors;
    NJobTrackerClient::TJobId JobCompetitionId;
    NYson::TYsonString Error;
    NYson::TYsonString BriefStatistics;
    NYson::TYsonString Statistics;
    NYson::TYsonString InputPaths;
    NYson::TYsonString CoreInfos;
    NYson::TYsonString Events;
    std::optional<bool> IsStale;
};

void Serialize(const TJob& job, NYson::IYsonConsumer* consumer, TStringBuf idKey);

struct TListJobsStatistics
{
    TEnumIndexedVector<NJobTrackerClient::EJobState, i64> StateCounts;
    TEnumIndexedVector<NJobTrackerClient::EJobType, i64> TypeCounts;
};

struct TListJobsResult
{
    std::vector<TJob> Jobs;
    std::optional<int> CypressJobCount;
    std::optional<int> ControllerAgentJobCount;
    std::optional<int> ArchiveJobCount;

    TListJobsStatistics Statistics;

    std::vector<TError> Errors;
};

struct TGetFileFromCacheResult
{
    NYPath::TYPath Path;
};

struct TPutFileToCacheResult
{
    NYPath::TYPath Path;
};

////////////////////////////////////////////////////////////////////////////////

//! Provides a basic set of functions that can be invoked
//! both standalone and inside transaction.
/*
 *  This interface contains methods shared by IClient and ITransaction.
 *
 *  Thread affinity: single
 */
struct IClientBase
    : public virtual TRefCounted
{
    virtual IConnectionPtr GetConnection() = 0;

    // Transactions
    virtual TFuture<ITransactionPtr> StartTransaction(
        NTransactionClient::ETransactionType type,
        const TTransactionStartOptions& options = {}) = 0;


    // Tables
    virtual TFuture<IUnversionedRowsetPtr> LookupRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const TLookupRowsOptions& options = {}) = 0;

    virtual TFuture<IVersionedRowsetPtr> VersionedLookupRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const TVersionedLookupRowsOptions& options = {}) = 0;

    virtual TFuture<TSelectRowsResult> SelectRows(
        const TString& query,
        const TSelectRowsOptions& options = {}) = 0;

    virtual TFuture<NYson::TYsonString> ExplainQuery(
        const TString& query,
        const TExplainQueryOptions& options = TExplainQueryOptions()) = 0;

    virtual TFuture<ITableReaderPtr> CreateTableReader(
        const NYPath::TRichYPath& path,
        const TTableReaderOptions& options = {}) = 0;

    virtual TFuture<ITableWriterPtr> CreateTableWriter(
        const NYPath::TRichYPath& path,
        const TTableWriterOptions& options = {}) = 0;

    // Cypress
    virtual TFuture<NYson::TYsonString> GetNode(
        const NYPath::TYPath& path,
        const TGetNodeOptions& options = {}) = 0;

    virtual TFuture<void> SetNode(
        const NYPath::TYPath& path,
        const NYson::TYsonString& value,
        const TSetNodeOptions& options = {}) = 0;

    virtual TFuture<void> RemoveNode(
        const NYPath::TYPath& path,
        const TRemoveNodeOptions& options = {}) = 0;

    virtual TFuture<NYson::TYsonString> ListNode(
        const NYPath::TYPath& path,
        const TListNodeOptions& options = {}) = 0;

    virtual TFuture<NCypressClient::TNodeId> CreateNode(
        const NYPath::TYPath& path,
        NObjectClient::EObjectType type,
        const TCreateNodeOptions& options = {}) = 0;

    virtual TFuture<TLockNodeResult> LockNode(
        const NYPath::TYPath& path,
        NCypressClient::ELockMode mode,
        const TLockNodeOptions& options = {}) = 0;

    virtual TFuture<void> UnlockNode(
        const NYPath::TYPath& path,
        const TUnlockNodeOptions& options = {}) = 0;

    virtual TFuture<NCypressClient::TNodeId> CopyNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TCopyNodeOptions& options = {}) = 0;

    virtual TFuture<NCypressClient::TNodeId> MoveNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TMoveNodeOptions& options = {}) = 0;

    virtual TFuture<NCypressClient::TNodeId> LinkNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TLinkNodeOptions& options = {}) = 0;

    virtual TFuture<void> ConcatenateNodes(
        const std::vector<NYPath::TRichYPath>& srcPaths,
        const NYPath::TRichYPath& dstPath,
        const TConcatenateNodesOptions& options = {}) = 0;

    virtual TFuture<bool> NodeExists(
        const NYPath::TYPath& path,
        const TNodeExistsOptions& options = {}) = 0;

    virtual TFuture<void> ExternalizeNode(
        const NYPath::TYPath& path,
        NObjectClient::TCellTag cellTag,
        const TExternalizeNodeOptions& options = {}) = 0;

    virtual TFuture<void> InternalizeNode(
        const NYPath::TYPath& path,
        const TInternalizeNodeOptions& options = {}) = 0;


    // Objects
    virtual TFuture<NObjectClient::TObjectId> CreateObject(
        NObjectClient::EObjectType type,
        const TCreateObjectOptions& options = {}) = 0;


    // Files
    virtual TFuture<IFileReaderPtr> CreateFileReader(
        const NYPath::TYPath& path,
        const TFileReaderOptions& options = {}) = 0;

    virtual IFileWriterPtr CreateFileWriter(
        const NYPath::TRichYPath& path,
        const TFileWriterOptions& options = {}) = 0;

    // Journals
    virtual IJournalReaderPtr CreateJournalReader(
        const NYPath::TYPath& path,
        const TJournalReaderOptions& options = {}) = 0;

    virtual IJournalWriterPtr CreateJournalWriter(
        const NYPath::TYPath& path,
        const TJournalWriterOptions& options = {}) = 0;

};

DEFINE_REFCOUNTED_TYPE(IClientBase)

////////////////////////////////////////////////////////////////////////////////

//! A central entry point for all interactions with the YT cluster.
/*!
 *  In contrast to IConnection, each IClient represents an authenticated entity.
 *  The needed username is passed to #IConnection::CreateClient via options.
 *  Note that YT API has no built-in authentication mechanisms so it must be wrapped
 *  with appropriate logic.
 *
 *  Most methods accept |TransactionId| as a part of their options.
 *  A similar effect can be achieved by issuing requests via ITransaction.
 */
struct IClient
    : public virtual IClientBase
{
    //! Terminates all channels.
    //! Aborts all pending uncommitted transactions.
    //! Returns a async flag indicating completion.
    virtual TFuture<void> Terminate() = 0;

    virtual const NTabletClient::ITableMountCachePtr& GetTableMountCache() = 0;
    virtual const NTransactionClient::ITimestampProviderPtr& GetTimestampProvider() = 0;

    // Transactions
    virtual ITransactionPtr AttachTransaction(
        NTransactionClient::TTransactionId transactionId,
        const TTransactionAttachOptions& options = {}) = 0;

    // Tables
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
        const std::vector<NTableClient::TOwningKey>& pivotKeys,
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

    virtual TFuture<std::vector<NTabletClient::TTableReplicaId>> GetInSyncReplicas(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const TGetInSyncReplicasOptions& options = {}) = 0;

    virtual TFuture<std::vector<TTabletInfo>> GetTabletInfos(
        const NYPath::TYPath& path,
        const std::vector<int>& tabletIndexes,
        const TGetTabletsInfoOptions& options = {}) = 0;

    virtual TFuture<std::vector<NTabletClient::TTabletActionId>> BalanceTabletCells(
        const TString& tabletCellBundle,
        const std::vector<NYPath::TYPath>& movableTables,
        const TBalanceTabletCellsOptions& options = {}) = 0;

    virtual TFuture<TSkynetSharePartsLocationsPtr> LocateSkynetShare(
        const NYPath::TRichYPath& path,
        const TLocateSkynetShareOptions& options = {}) = 0;

    virtual TFuture<std::vector<NTableClient::TColumnarStatistics>> GetColumnarStatistics(
        const std::vector<NYPath::TRichYPath>& path,
        const TGetColumnarStatisticsOptions& options = {}) = 0;

    // Journals
    virtual TFuture<void> TruncateJournal(
        const NYPath::TYPath& path,
        i64 rowCount,
        const TTruncateJournalOptions& options = {}) = 0;

    // Files
    virtual TFuture<TGetFileFromCacheResult> GetFileFromCache(
        const TString& md5,
        const TGetFileFromCacheOptions& options = {}) = 0;

    virtual TFuture<TPutFileToCacheResult> PutFileToCache(
        const NYPath::TYPath& path,
        const TString& expectedMD5,
        const TPutFileToCacheOptions& options = {}) = 0;

    // Security
    virtual TFuture<void> AddMember(
        const TString& group,
        const TString& member,
        const TAddMemberOptions& options = {}) = 0;

    virtual TFuture<void> RemoveMember(
        const TString& group,
        const TString& member,
        const TRemoveMemberOptions& options = {}) = 0;

    virtual TFuture<TCheckPermissionResponse> CheckPermission(
        const TString& user,
        const NYPath::TYPath& path,
        NYTree::EPermission permission,
        const TCheckPermissionOptions& options = {}) = 0;

    virtual TFuture<TCheckPermissionByAclResult> CheckPermissionByAcl(
        const std::optional<TString>& user,
        NYTree::EPermission permission,
        NYTree::INodePtr acl,
        const TCheckPermissionByAclOptions& options = {}) = 0;


    // Scheduler
    virtual TFuture<NScheduler::TOperationId> StartOperation(
        NScheduler::EOperationType type,
        const NYson::TYsonString& spec,
        const TStartOperationOptions& options = {}) = 0;

    virtual TFuture<void> AbortOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TAbortOperationOptions& options = {}) = 0;

    virtual TFuture<void> SuspendOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TSuspendOperationOptions& options = {}) = 0;

    virtual TFuture<void> ResumeOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TResumeOperationOptions& options = {}) = 0;

    virtual TFuture<void> CompleteOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TCompleteOperationOptions& options = {}) = 0;

    virtual TFuture<void> UpdateOperationParameters(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const NYson::TYsonString& parameters,
        const TUpdateOperationParametersOptions& options = {}) = 0;

    virtual TFuture<NYson::TYsonString> GetOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TGetOperationOptions& options = {}) = 0;

    virtual TFuture<void> DumpJobContext(
        NJobTrackerClient::TJobId jobId,
        const NYPath::TYPath& path,
        const TDumpJobContextOptions& options = {}) = 0;

    virtual TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr> GetJobInput(
        NJobTrackerClient::TJobId jobId,
        const TGetJobInputOptions& options = {}) = 0;

    virtual TFuture<NYson::TYsonString> GetJobInputPaths(
        NJobTrackerClient::TJobId jobId,
        const TGetJobInputPathsOptions& options = {}) = 0;

    virtual TFuture<TSharedRef> GetJobStderr(
        NJobTrackerClient::TOperationId operationId,
        NJobTrackerClient::TJobId jobId,
        const TGetJobStderrOptions& options = {}) = 0;

    virtual TFuture<TSharedRef> GetJobFailContext(
        NJobTrackerClient::TOperationId operationId,
        NJobTrackerClient::TJobId jobId,
        const TGetJobFailContextOptions& options = {}) = 0;

    virtual TFuture<TListOperationsResult> ListOperations(
        const TListOperationsOptions& options = {}) = 0;

    virtual TFuture<TListJobsResult> ListJobs(
        NJobTrackerClient::TOperationId operationId,
        const TListJobsOptions& options = {}) = 0;

    virtual TFuture<NYson::TYsonString> GetJob(
        NScheduler::TOperationId operationId,
        NJobTrackerClient::TJobId jobId,
        const TGetJobOptions& options = {}) = 0;

    virtual TFuture<NYson::TYsonString> StraceJob(
        NJobTrackerClient::TJobId jobId,
        const TStraceJobOptions& options = {}) = 0;

    virtual TFuture<void> SignalJob(
        NJobTrackerClient::TJobId jobId,
        const TString& signalName,
        const TSignalJobOptions& options = {}) = 0;

    virtual TFuture<void> AbandonJob(
        NJobTrackerClient::TJobId jobId,
        const TAbandonJobOptions& options = {}) = 0;

    virtual TFuture<NYson::TYsonString> PollJobShell(
        NJobTrackerClient::TJobId jobId,
        const NYson::TYsonString& parameters,
        const TPollJobShellOptions& options = {}) = 0;

    virtual TFuture<void> AbortJob(
        NJobTrackerClient::TJobId jobId,
        const TAbortJobOptions& options = {}) = 0;

    // Metadata
    virtual TFuture<TClusterMeta> GetClusterMeta(
        const TGetClusterMetaOptions& options = {}) = 0;
};

DEFINE_REFCOUNTED_TYPE(IClient)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

