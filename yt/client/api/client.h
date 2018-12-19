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

#include <yt/client/table_client/config.h>
#include <yt/client/table_client/row_base.h>
#include <yt/client/table_client/schema.h>

#include <yt/client/table_client/columnar_statistics.h>

#include <yt/client/tablet_client/public.h>

#include <yt/client/chunk_client/config.h>

#include <yt/client/transaction_client/public.h>

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

#include <yt/core/erasure/public.h>

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
    i64 Revision;

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

struct TAlterTableOptions
    : public TTimeoutOptions
    , public TMutatingOptions
    , public TTransactionalOptions
{
    std::optional<NTableClient::TTableSchema> Schema;
    std::optional<bool> Dynamic;
    std::optional<NTabletClient::TTableReplicaId> UpstreamReplicaId;
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
    i64 TotalRowCount = 0;
    i64 TrimmedRowCount = 0;
};

struct TAddMemberOptions
    : public TTimeoutOptions
    , public TMutatingOptions
{ };

struct TRemoveMemberOptions
    : public TTimeoutOptions
    , public TMutatingOptions
{ };

struct TCheckPermissionOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
    , public TTransactionalOptions
    , public TPrerequisiteOptions
{ };

struct TCheckPermissionResult
{
    TError ToError(const TString& user, NYTree::EPermission permission) const;

    NSecurityClient::ESecurityAction Action;
    NObjectClient::TObjectId ObjectId;
    std::optional<TString> ObjectName;
    NSecurityClient::TSubjectId SubjectId;
    std::optional<TString> SubjectName;
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

    //! Only for master transactions at secondary cell.
    NObjectClient::TCellTag CellTag = NObjectClient::PrimaryMasterCellTag;

    bool Multicell = false;
};

struct TTransactionAttachOptions
{
    bool AutoAbort = false;
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

    //! If not #InvalidCellTag, a random participant from the given cell will be the coordinator.
    NObjectClient::TCellTag CoordinatorCellTag = NObjectClient::InvalidCellTag;

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
};

struct TLookupRowsOptions
    : public TLookupRowsOptionsBase
{ };

struct TVersionedLookupRowsOptions
    : public TLookupRowsOptionsBase
{
    NTableClient::TRetentionConfigPtr RetentionConfig;
};

struct TSelectRowsOptions
    : public TTabletReadOptions
{
    //! If null then connection defaults are used.
    std::optional<i64> InputRowLimit;
    //! If null then connection defaults are used.
    std::optional<i64> OutputRowLimit;
    //! Limits range expanding.
    ui64 RangeExpansionLimit = 200000;
    //! If |true| then incomplete result would lead to a failure.
    bool FailOnIncompleteResult = true;
    //! If |true| then logging is more verbose.
    bool VerboseLogging = false;
    //! Limits maximum parallel subqueries.
    int MaxSubqueries = std::numeric_limits<int>::max();
    //! Enables generated code caching.
    bool EnableCodeCache = true;
    //! Used to prioritize requests.
    TUserWorkloadDescriptor WorkloadDescriptor;
    //! Combine independent joins in one.
    bool UseMultijoin = true;
    //! Allow queries without any condition on key columns.
    bool AllowFullScan = true;
    //! Allow queries with join condition which implies foreign query with IN operator.
    bool AllowJoinWithoutIndex = false;
    //! Path in Cypress with UDFs.
    std::optional<TString> UdfRegistryPath;
};

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
    std::shared_ptr<const NYTree::IAttributeDictionary> Attributes;
};

struct TCreateNodeOptions
    : public TCreateObjectOptions
    , public TTransactionalOptions
{
    bool Recursive = false;
    bool IgnoreExisting = false;
    bool Force = false;
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
};

struct TUnlockNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{
};

struct TCopyNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{
    bool Recursive = false;
    bool Force = false;
    bool PreserveAccount = false;
    bool PreserveExpirationTime = false;
    bool PreserveCreationTime = false;
};

struct TMoveNodeOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
{
    bool Recursive = false;
    bool Force = false;
    bool PreserveAccount = false;
    bool PreserveExpirationTime = false;
};

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
    bool Force = false;
};

struct TConcatenateNodesOptions
    : public TTimeoutOptions
    , public TTransactionalOptions
    , public TMutatingOptions
{
    bool Append = false;
};

struct TNodeExistsOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
    , public TTransactionalOptions
    , public TSuppressableAccessTrackingOptions
    , public TPrerequisiteOptions
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
    bool Append = true;
    bool ComputeMD5 = false;
    std::optional<NCompression::ECodec> CompressionCodec;
    std::optional<NErasure::ECodec> ErasureCodec;
    TFileWriterConfigPtr Config;
};

struct TGetFileFromCacheOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
{
    NYPath::TYPath CachePath;
};

struct TPutFileToCacheOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
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
    NProfiling::TProfiler Profiler;
};

struct TTableReaderOptions
    : public TTransactionalOptions
{
    bool Unordered = false;
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

struct TListOperationsOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
{
    std::optional<TInstant> FromTime;
    std::optional<TInstant> ToTime;
    std::optional<TInstant> CursorTime;
    EOperationSortDirection CursorDirection = EOperationSortDirection::Past;
    std::optional<TString> UserFilter;

    // XXX(lesysotsky): OwnedBy filter is currently supported only
    // for Cypress operations, so IncludeArchive must be |false|.
    std::optional<TString> OwnedBy;

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
    std::optional<NJobTrackerClient::EJobType> Type;
    std::optional<NJobTrackerClient::EJobState> State;
    std::optional<TString> Address;
    std::optional<bool> WithStderr;
    std::optional<bool> WithFailContext;
    std::optional<bool> WithSpec;

    EJobSortField SortField = EJobSortField::None;
    EJobSortDirection SortOrder = EJobSortDirection::Ascending;

    i64 Limit = 1000;
    i64 Offset = 0;

    bool IncludeCypress = false;
    bool IncludeControllerAgent = false;
    bool IncludeArchive = false;

    EDataSource DataSource = EDataSource::Auto;

    TDuration RunningJobsLookbehindPeriod = TDuration::Minutes(1);

    TListJobsOptions()
    { }
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
    bool IncludeRuntime = false;

    TGetOperationOptions()
    { }
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
};

struct TClusterMeta
{
    std::shared_ptr<NNodeTrackerClient::NProto::TNodeDirectory> NodeDirectory;
    std::shared_ptr<NHiveClient::NProto::TClusterDirectory> ClusterDirectory;
    std::shared_ptr<NChunkClient::NProto::TMediumDirectory> MediumDirectory;
};

struct TOperation
{
    std::optional<NScheduler::TOperationId> Id;

    std::optional<NScheduler::EOperationType> Type;
    std::optional<NScheduler::EOperationState> State;

    std::optional<TInstant> StartTime;
    std::optional<TInstant> FinishTime;

    std::optional<TString> AuthenticatedUser;
    std::optional<std::vector<TString>> Owners;

    std::optional<std::vector<TString>> Pools;

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
};

struct TListOperationsResult
{
    std::vector<TOperation> Operations;
    std::optional<THashMap<TString, i64>> PoolCounts;
    std::optional<THashMap<TString, i64>> UserCounts;
    std::optional<TEnumIndexedVector<i64, NScheduler::EOperationState>> StateCounts;
    std::optional<TEnumIndexedVector<i64, NScheduler::EOperationType>> TypeCounts;
    std::optional<i64> FailedJobsCount;
    bool Incomplete = false;
};

struct TJob
{
    NJobTrackerClient::TJobId Id;
    NJobTrackerClient::EJobType Type;
    NJobTrackerClient::EJobState State;
    TInstant StartTime;
    std::optional<TInstant> FinishTime;
    TString Address;
    std::optional<double> Progress;
    std::optional<ui64> StderrSize;
    std::optional<ui64> FailContextSize;
    std::optional<bool> HasSpec;
    NYson::TYsonString Error;
    NYson::TYsonString BriefStatistics;
    NYson::TYsonString InputPaths;
    NYson::TYsonString CoreInfos;
};

struct TListJobsStatistics
{
    TEnumIndexedVector<i64, NJobTrackerClient::EJobState> StateCounts;
    TEnumIndexedVector<i64, NJobTrackerClient::EJobType> TypeCounts;
};

struct TListJobsResult
{
    std::vector<TJob> Jobs;
    std::optional<int> CypressJobCount;
    std::optional<int> ControllerAgentJobCount;
    std::optional<int> ArchiveJobCount;

    TListJobsStatistics Statistics;
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
        const TTransactionStartOptions& options = TTransactionStartOptions()) = 0;


    // Tables
    virtual TFuture<IUnversionedRowsetPtr> LookupRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const TLookupRowsOptions& options = TLookupRowsOptions()) = 0;

    virtual TFuture<IVersionedRowsetPtr> VersionedLookupRows(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const TVersionedLookupRowsOptions& options = TVersionedLookupRowsOptions()) = 0;

    virtual TFuture<TSelectRowsResult> SelectRows(
        const TString& query,
        const TSelectRowsOptions& options = TSelectRowsOptions()) = 0;

    virtual TFuture<ITableReaderPtr> CreateTableReader(
        const NYPath::TRichYPath& path,
        const TTableReaderOptions& options = TTableReaderOptions()) = 0;

    virtual TFuture<ITableWriterPtr> CreateTableWriter(
        const NYPath::TRichYPath& path,
        const TTableWriterOptions& options = TTableWriterOptions()) = 0;

    // Cypress
    virtual TFuture<NYson::TYsonString> GetNode(
        const NYPath::TYPath& path,
        const TGetNodeOptions& options = TGetNodeOptions()) = 0;

    virtual TFuture<void> SetNode(
        const NYPath::TYPath& path,
        const NYson::TYsonString& value,
        const TSetNodeOptions& options = TSetNodeOptions()) = 0;

    virtual TFuture<void> RemoveNode(
        const NYPath::TYPath& path,
        const TRemoveNodeOptions& options = TRemoveNodeOptions()) = 0;

    virtual TFuture<NYson::TYsonString> ListNode(
        const NYPath::TYPath& path,
        const TListNodeOptions& options = TListNodeOptions()) = 0;

    virtual TFuture<NCypressClient::TNodeId> CreateNode(
        const NYPath::TYPath& path,
        NObjectClient::EObjectType type,
        const TCreateNodeOptions& options = TCreateNodeOptions()) = 0;

    virtual TFuture<TLockNodeResult> LockNode(
        const NYPath::TYPath& path,
        NCypressClient::ELockMode mode,
        const TLockNodeOptions& options = TLockNodeOptions()) = 0;

    virtual TFuture<void> UnlockNode(
        const NYPath::TYPath& path,
        const TUnlockNodeOptions& options = TUnlockNodeOptions()) = 0;

    virtual TFuture<NCypressClient::TNodeId> CopyNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TCopyNodeOptions& options = TCopyNodeOptions()) = 0;

    virtual TFuture<NCypressClient::TNodeId> MoveNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TMoveNodeOptions& options = TMoveNodeOptions()) = 0;

    virtual TFuture<NCypressClient::TNodeId> LinkNode(
        const NYPath::TYPath& srcPath,
        const NYPath::TYPath& dstPath,
        const TLinkNodeOptions& options = TLinkNodeOptions()) = 0;

    virtual TFuture<void> ConcatenateNodes(
        const std::vector<NYPath::TYPath>& srcPaths,
        const NYPath::TYPath& dstPath,
        const TConcatenateNodesOptions& options = TConcatenateNodesOptions()) = 0;

    virtual TFuture<bool> NodeExists(
        const NYPath::TYPath& path,
        const TNodeExistsOptions& options = TNodeExistsOptions()) = 0;


    // Objects
    // TODO(sandello): Non-transactional!
    virtual TFuture<NObjectClient::TObjectId> CreateObject(
        NObjectClient::EObjectType type,
        const TCreateObjectOptions& options = TCreateObjectOptions()) = 0;


    // Files
    virtual TFuture<IFileReaderPtr> CreateFileReader(
        const NYPath::TYPath& path,
        const TFileReaderOptions& options = TFileReaderOptions()) = 0;

    virtual IFileWriterPtr CreateFileWriter(
        const NYPath::TYPath& path,
        const TFileWriterOptions& options = TFileWriterOptions()) = 0;

    // Journals
    virtual IJournalReaderPtr CreateJournalReader(
        const NYPath::TYPath& path,
        const TJournalReaderOptions& options = TJournalReaderOptions()) = 0;

    virtual IJournalWriterPtr CreateJournalWriter(
        const NYPath::TYPath& path,
        const TJournalWriterOptions& options = TJournalWriterOptions()) = 0;

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
        const TTransactionAttachOptions& options = TTransactionAttachOptions()) = 0;

    // Tables
    virtual TFuture<void> MountTable(
        const NYPath::TYPath& path,
        const TMountTableOptions& options = TMountTableOptions()) = 0;

    virtual TFuture<void> UnmountTable(
        const NYPath::TYPath& path,
        const TUnmountTableOptions& options = TUnmountTableOptions()) = 0;

    virtual TFuture<void> RemountTable(
        const NYPath::TYPath& path,
        const TRemountTableOptions& options = TRemountTableOptions()) = 0;

    virtual TFuture<void> FreezeTable(
        const NYPath::TYPath& path,
        const TFreezeTableOptions& options = TFreezeTableOptions()) = 0;

    virtual TFuture<void> UnfreezeTable(
        const NYPath::TYPath& path,
        const TUnfreezeTableOptions& options = TUnfreezeTableOptions()) = 0;

    virtual TFuture<void> ReshardTable(
        const NYPath::TYPath& path,
        const std::vector<NTableClient::TOwningKey>& pivotKeys,
        const TReshardTableOptions& options = TReshardTableOptions()) = 0;

    virtual TFuture<void> ReshardTable(
        const NYPath::TYPath& path,
        int tabletCount,
        const TReshardTableOptions& options = TReshardTableOptions()) = 0;

    virtual TFuture<void> TrimTable(
        const NYPath::TYPath& path,
        int tabletIndex,
        i64 trimmedRowCount,
        const TTrimTableOptions& options = TTrimTableOptions()) = 0;

    virtual TFuture<void> AlterTable(
        const NYPath::TYPath& path,
        const TAlterTableOptions& options = TAlterTableOptions()) = 0;

    virtual TFuture<void> AlterTableReplica(
        const NTabletClient::TTableReplicaId& replicaId,
        const TAlterTableReplicaOptions& options = TAlterTableReplicaOptions()) = 0;

    virtual TFuture<std::vector<NTabletClient::TTableReplicaId>> GetInSyncReplicas(
        const NYPath::TYPath& path,
        NTableClient::TNameTablePtr nameTable,
        const TSharedRange<NTableClient::TKey>& keys,
        const TGetInSyncReplicasOptions& options = TGetInSyncReplicasOptions()) = 0;

    virtual TFuture<std::vector<TTabletInfo>> GetTabletInfos(
        const NYPath::TYPath& path,
        const std::vector<int>& tabletIndexes,
        const TGetTabletsInfoOptions& options = TGetTabletsInfoOptions()) = 0;

    virtual TFuture<TSkynetSharePartsLocationsPtr> LocateSkynetShare(
        const NYPath::TRichYPath& path,
        const TLocateSkynetShareOptions& options = TLocateSkynetShareOptions()) = 0;

    virtual TFuture<std::vector<NTableClient::TColumnarStatistics>> GetColumnarStatistics(
        const std::vector<NYPath::TRichYPath>& path,
        const TGetColumnarStatisticsOptions& options = TGetColumnarStatisticsOptions()) = 0;

    // Files
    virtual TFuture<TGetFileFromCacheResult> GetFileFromCache(
        const TString& md5,
        const TGetFileFromCacheOptions& options = TGetFileFromCacheOptions()) = 0;

    virtual TFuture<TPutFileToCacheResult> PutFileToCache(
        const NYPath::TYPath& path,
        const TString& expectedMD5,
        const TPutFileToCacheOptions& options = TPutFileToCacheOptions()) = 0;

    // Security
    virtual TFuture<void> AddMember(
        const TString& group,
        const TString& member,
        const TAddMemberOptions& options = TAddMemberOptions()) = 0;

    virtual TFuture<void> RemoveMember(
        const TString& group,
        const TString& member,
        const TRemoveMemberOptions& options = TRemoveMemberOptions()) = 0;

    virtual TFuture<TCheckPermissionResult> CheckPermission(
        const TString& user,
        const NYPath::TYPath& path,
        NYTree::EPermission permission,
        const TCheckPermissionOptions& options = TCheckPermissionOptions()) = 0;

    virtual TFuture<TCheckPermissionByAclResult> CheckPermissionByAcl(
        const std::optional<TString>& user,
        NYTree::EPermission permission,
        NYTree::INodePtr acl,
        const TCheckPermissionByAclOptions& options = TCheckPermissionByAclOptions()) = 0;


    // Scheduler
    virtual TFuture<NScheduler::TOperationId> StartOperation(
        NScheduler::EOperationType type,
        const NYson::TYsonString& spec,
        const TStartOperationOptions& options = TStartOperationOptions()) = 0;

    virtual TFuture<void> AbortOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TAbortOperationOptions& options = TAbortOperationOptions()) = 0;

    virtual TFuture<void> SuspendOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TSuspendOperationOptions& options = TSuspendOperationOptions()) = 0;

    virtual TFuture<void> ResumeOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TResumeOperationOptions& options = TResumeOperationOptions()) = 0;

    virtual TFuture<void> CompleteOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TCompleteOperationOptions& options = TCompleteOperationOptions()) = 0;

    virtual TFuture<void> UpdateOperationParameters(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const NYson::TYsonString& parameters,
        const TUpdateOperationParametersOptions& options = TUpdateOperationParametersOptions()) = 0;

    virtual TFuture<NYson::TYsonString> GetOperation(
        const NScheduler::TOperationIdOrAlias& operationIdOrAlias,
        const TGetOperationOptions& options = TGetOperationOptions()) = 0;

    virtual TFuture<void> DumpJobContext(
        const NJobTrackerClient::TJobId& jobId,
        const NYPath::TYPath& path,
        const TDumpJobContextOptions& options = TDumpJobContextOptions()) = 0;

    virtual TFuture<NConcurrency::IAsyncZeroCopyInputStreamPtr> GetJobInput(
        const NJobTrackerClient::TJobId& jobId,
        const TGetJobInputOptions& options = TGetJobInputOptions()) = 0;

    virtual TFuture<NYson::TYsonString> GetJobInputPaths(
        const NJobTrackerClient::TJobId& jobId,
        const TGetJobInputPathsOptions& options = TGetJobInputPathsOptions()) = 0;

    virtual TFuture<TSharedRef> GetJobStderr(
        const NJobTrackerClient::TOperationId& operationId,
        const NJobTrackerClient::TJobId& jobId,
        const TGetJobStderrOptions& options = TGetJobStderrOptions()) = 0;

    virtual TFuture<TSharedRef> GetJobFailContext(
        const NJobTrackerClient::TOperationId& operationId,
        const NJobTrackerClient::TJobId& jobId,
        const TGetJobFailContextOptions& options = TGetJobFailContextOptions()) = 0;

    virtual TFuture<TListOperationsResult> ListOperations(
        const TListOperationsOptions& options = TListOperationsOptions()) = 0;

    virtual TFuture<TListJobsResult> ListJobs(
        const NJobTrackerClient::TOperationId& operationId,
        const TListJobsOptions& options = TListJobsOptions()) = 0;

    virtual TFuture<NYson::TYsonString> GetJob(
        const NScheduler::TOperationId& operationId,
        const NJobTrackerClient::TJobId& jobId,
        const TGetJobOptions& options = TGetJobOptions()) = 0;

    virtual TFuture<NYson::TYsonString> StraceJob(
        const NJobTrackerClient::TJobId& jobId,
        const TStraceJobOptions& options = TStraceJobOptions()) = 0;

    virtual TFuture<void> SignalJob(
        const NJobTrackerClient::TJobId& jobId,
        const TString& signalName,
        const TSignalJobOptions& options = TSignalJobOptions()) = 0;

    virtual TFuture<void> AbandonJob(
        const NJobTrackerClient::TJobId& jobId,
        const TAbandonJobOptions& options = TAbandonJobOptions()) = 0;

    virtual TFuture<NYson::TYsonString> PollJobShell(
        const NJobTrackerClient::TJobId& jobId,
        const NYson::TYsonString& parameters,
        const TPollJobShellOptions& options = TPollJobShellOptions()) = 0;

    virtual TFuture<void> AbortJob(
        const NJobTrackerClient::TJobId& jobId,
        const TAbortJobOptions& options = TAbortJobOptions()) = 0;

    // Metadata
    virtual TFuture<TClusterMeta> GetClusterMeta(
        const TGetClusterMetaOptions& options = TGetClusterMetaOptions()) = 0;
};

DEFINE_REFCOUNTED_TYPE(IClient)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

