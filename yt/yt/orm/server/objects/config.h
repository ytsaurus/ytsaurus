#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/library/query/base/public.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct TGroupTypeHandlerConfig
    : public NYTree::TYsonStruct
{
    bool EnableMembersValidation;

    REGISTER_YSON_STRUCT(TGroupTypeHandlerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TGroupTypeHandlerConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EParentsTableMode,
    (NoSeparateTable)
    (WriteToSeparateTable)
    //(VerifiedReadSeparateTable)
    (ReadSeparateTable)
    (DontWriteToCommonTable)
);

DEFINE_ENUM(ERemoveObjectMode,
    // Set removal time and write to pending_removals table on remove.
    // Delete rows by sweeping pending removals table or when a new object with the same key is created.
    (Asynchronous)
    // Delete object row on remove.
    // Delete rows of previously asynchronously removed objects on sweep
    // or when new object with the same key is created.
    (Synchronous)
    // Delete object row on remove.
    // Do not read or write to pending removals table.
    (SynchronousExclusive)
);

struct TObjectManagerConfig
    : public NYTree::TYsonStruct
{
    // Group.
    TGroupTypeHandlerConfigPtr GroupTypeHandler;

    // Removal.
    // Guarantee parent removal with last finalizing child.
    // May be disabled to omit extra locking.
    bool RemoveParentOnChildFinalization;
    // COMPAT(dgolear): Use `ECascadingRemovalPolicy::AwaitForeign` instead.
    // By default references are removed before finalization,
    // but disabling flag keeps them until finalization is completed.
    bool ClearReferencesOnFinalization;
    ERemoveObjectMode RemoveMode;
    TDuration RemovedObjectsSweepPeriod;
    TDuration RemovedObjectsGraceTimeout;
    TObjectTableReaderConfigPtr RemovedObjectTableReader;

    // Extensible attributes.
    EAttributesExtensibilityMode AttributesExtensibilityMode;

    THashMap<TObjectTypeName, std::vector<NYPath::TYPath>> AllowedExtensibleAttributePaths;

    // History.
    bool EnableHistory;
    THashSet<TObjectTypeName> HistoryDisabledTypes;
    bool HistoryIndexGroupByEnabled;
    THashMap<TString, bool> HistoryLabelsStoreEnabledPerType;

    // Indexes that are allowed to be read in building mode.
    THashSet<std::string> IndexesWithAllowedBuildingRead;

    bool EnableUpsertByUniqueIndex;

    EUtf8Check Utf8Check;

    THashMap<TObjectTypeName, THashMap<std::string, EAttributeMigrationPhase>> AttributeMigrationPhases;

    NQueryClient::TColumnEvaluatorCacheConfigPtr ColumnEvaluatorCache;

    bool SemaphoreProfilingEnabled;
    TDuration SemaphoreProfilingPeriod;

    THashMap<std::string, NYPath::TYPath> ExternalTablePaths;

    std::optional<EIndexOrReferenceMode> TryGetIndexMode(const std::string& indexName) const;
    std::optional<EIndexOrReferenceMode> TryGetReferenceMode(
        const TObjectTypeName& type,
        const NYPath::TYPath& referencePath) const;

    EInternalUpdateFormat InternalUpdateFormat;
    bool SkipUnknownFieldsInPayloads;

    std::optional<EHistoryTimeIndexMode> TryGetHistoryTimeIndexMode(const std::string& indexName) const;

    std::vector<NYPath::TYPath> GetAllowedExtensibleAttributePaths(TObjectTypeValue objectType) const;
    EAttributeMigrationPhase GetAttributeMigrationPhase(TObjectTypeValue objectType, const std::string& migration);

    bool IsHistoryIndexAttributeQueryEnabled(const TObjectTypeName& type, const NYPath::TYPath& attributePath) const;
    bool IsHistoryIndexAttributeStoreEnabled(const TObjectTypeName& type, const NYPath::TYPath& attributePath) const;
    std::optional<TInstant> GetHistoryLastTrimTimeFromRetentionPeriod(const std::string& historyTableName) const;
    std::optional<TInstant> GetHistoryLastTrimTime(const std::string& historyTableName) const;
    std::optional<TTimestamp> GetHistoryLastTrimTimestamp(const std::string& historyTableName) const;
    EParentsTableMode GetParentsTableMode(TObjectTypeValue objectType) const;
    bool MayReadLegacyParentsTable() const;

    std::optional<ECascadingRemovalPolicy> GetParentRemovalPolicy(
        TObjectTypeValue parentType,
        TObjectTypeValue childType) const;

    std::optional<ECascadingRemovalPolicy> GetReferenceRemovalPolicy(
        const TObjectTypeName& ownerTypeName,
        const NYPath::TYPath& referencePath,
        EReferenceKind referenceKind) const;

    REGISTER_YSON_STRUCT(TObjectManagerConfig);

    static void Register(TRegistrar registrar);

private:
    THashMap<std::string, EIndexOrReferenceMode> IndexModePerName_;
    THashMap<TObjectTypeName, THashMap<NYPath::TYPath, EIndexOrReferenceMode>> ReferenceModePerObjectTypePerPath_;
    THashMap<std::string, EHistoryTimeIndexMode> HistoryTimeIndexModePerName_;
    THashMap<TObjectTypeName, THashMap<NYPath::TYPath, EIndexOrReferenceMode>> HistoryIndexModePerTypePerAttribute_;

    // TODO(grigminakov): Deprecate in favour of `HistoryLastTrimTimestampPerTable_`.
    THashMap<std::string, TInstant> HistoryLastTrimTimePerTable_;
    THashMap<std::string, TTimestamp> HistoryLastTrimTimestampPerTable_;
    THashMap<std::string, int> HistoryRetentionPeriodDaysPerTable_;
    EParentsTableMode DefaultParentsTableMode_;
    THashMap<std::string, EParentsTableMode> ParentsTableModes_;

    /// Cascading removal policy overrides for parent-child relationships (i.e., how removal of a
    /// parent with existing children is handled), in order of decreasing priority.
    // Policy that applies to the children objects that are of the type.
    THashMap<std::string, ECascadingRemovalPolicy> ParentRemovalPolicyPerChildType_;
    // Policy that applies to all children of the parent.
    THashMap<std::string, ECascadingRemovalPolicy> ParentRemovalPolicyPerParentType_;
    // Policy that applies to all parent-child relationships.
    std::optional<ECascadingRemovalPolicy> ParentRemovalPolicyOverride_;
    // Otherwise, the policy from the data model is used.

    /// Cascading removal policy overrides for references, in order of decreasing priority.
    // Policy that applies to the reference at the path of the object type.
    THashMap<TObjectTypeName, THashMap<NYPath::TYPath, ECascadingRemovalPolicy>>
        ReferenceRemovalPolicyPerOwnerTypeAndPath_;
    // Policy that applies to all references of a kind (|single|, |multi|, |tabular| or |counting|).
    // Kinds must be subdivided because they have vastly different removal semantics.
    THashMap<EReferenceKind, ECascadingRemovalPolicy> ReferenceRemovalPolicyPerReferenceKind_;
    // Otherwise, the policy from the data model is used.

    EIndexOrReferenceMode GetHistoryIndexMode(const TObjectTypeName& type, const NYPath::TYPath& attributePath) const;
};

DEFINE_REFCOUNTED_TYPE(TObjectManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TBatchSizeBackoffConfig
    : public NYTree::TYsonStruct
{
    i64 BatchSizeInitial;
    i64 BatchSizeLimit;
    i64 BatchSizeMultiplier;
    i64 BatchSizeAdditive;
    std::optional<double> BatchSizeAdditiveRelativeToMax;

    REGISTER_YSON_STRUCT(TBatchSizeBackoffConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBatchSizeBackoffConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSelectObjectHistoryConfig
    : public TBatchSizeBackoffConfig
{
    i64 OutputEventLimit;
    bool ForceAllowTimeModeConversion;
    std::optional<TTimestamp> PatchesBarrier;
    bool ValidatePatchConflicts;

    REGISTER_YSON_STRUCT(TSelectObjectHistoryConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSelectObjectHistoryConfig)

////////////////////////////////////////////////////////////////////////////////

struct THistorySnapshotPolicyConfig
    : public NYTree::TYsonStruct
{
    std::optional<i64> CreationPeriodSeconds;
    bool CreateDailySnapshots;

    // Sets `MaxAllowedCommitTimestamp` inferred from the parameters above.
    bool ForbidCrossPeriodTransactions;

    // Additional snapshot is added if history timestamp is in first `ExtendedSnapshotPeriod` of new era.
    TDuration ExtendedSnapshotPeriod;

    REGISTER_YSON_STRUCT(THistorySnapshotPolicyConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THistorySnapshotPolicyConfig)

////////////////////////////////////////////////////////////////////////////////

struct THistoryLockerConfig
    : public NYTree::TYsonStruct
{
    bool LockSnapshots;
    bool LockForcedAttributes;

    REGISTER_YSON_STRUCT(THistoryLockerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THistoryLockerConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMigrationState,
    (Initial)
    (WriteBoth)
    (AfterBackgroundMigration)
    (ReadNew)
    (RevokeOldTokens)
    (Target)
);

DEFINE_ENUM(EFullScanValidationMode,
    (None)
    (Warning)
    (Error)
);

DEFINE_ENUM(EHistoryWriteMode,
    (Legacy)
    (WriteBoth)
    (WritePatches)
);

DEFINE_ENUM(ELoadScheduledValidationMode,
    (None)
    (Report)
    (Throw)
);

DEFINE_ENUM(EMergeAttributesMode,
    (Old)
    (New)
    (Compare)
);

////////////////////////////////////////////////////////////////////////////////

struct TTransactionManagerConfig
    : public NYTree::TYsonStruct
{
    // YT select options.
    i64 InputRowLimit;
    i64 OutputRowLimit;
    i64 OrderByOutputRowLimit;
    i64 SelectMemoryLimitPerNode;
    bool SelectAllowFullScan;
    // For selects with LIMIT and ORDER BY hacks the heuristics that limit the parallelism of the query over nodes.
    // TODO(bulatman) Remove after YT-24111.
    bool SelectForceParallelExecution;
    bool PassUserTagAsSelectExecutionPool;

    // Select options.
    bool FullScanAllowedByDefault;
    EFullScanValidationMode FullScanValidationMode;
    EIndexResolveStrategy IndexResolveStrategy;
    bool BuildKeyExpression;
    bool FetchFinalizingObjectsByDefault;
    // Select result is generated in fractions of requested `LIMIT`.
    double PartialResultBatchFraction;
    // Returns prepared data if remaining time before request timeout is less than timeout slack.
    TDuration PartialResultSelectTimeoutSlack;
    bool EnableComputedFilter;
    bool EnablePushDownGroupBy;
    int InExpressionValueCountLimit;

    // Lookup options.
    int MaxKeysPerLookupRequest;
    bool CanUseLookupForGetObjects;
    bool EnableUnversionedLookupTimestampColumns;
    // Selected fields will be scheduled before checking object permissions
    // (allowing permissions to be read in the same read phase as selected fields).
    bool DelayPermissionCheckInGetObjects;
    // Selected fields will be scheduled before checking object existence
    // (allowing removal_time and finalization_start_time to be read in the same read phase as selected fields).
    bool DelayExistenceCheckInGetObjects;
    bool CheckFlushLoadsRecursion;

    bool TimestampBufferEnabled;
    ui64 TimestampBufferSize;
    TDuration TimestampBufferUpdatePeriod;
    ui64 TimestampBufferLowWatermark;

    i64 ReadPhaseHardLimit;
    bool AttributePrerequisiteReadLockEnabled;
    std::optional<bool> AllowRemovalWithNonemptyReferencesOverride;
    bool LockTargetObjectOnReferenceRemoval;
    bool UseAdministerPermission;

    bool ValidateDatabaseTimestamp;
    bool ForbidYsonAttributesUsage;
    bool EnableEventLogTableWrite;
    TDuration MinYTRequestTimeout;
    bool EnableDeadlinePropagation;
    bool EnableAttributeMigrations;
    ELoadScheduledValidationMode LoadScheduledValidationMode;
    // Compat option to emit `ObjectUpdated` event instead of `ObjectFinalized`.
    // Can be used to gradually migrate all existing clients to new policy.
    bool EnableObjectFinalizedEvent;

    // If a primary key is specified in the upsert query, unique indexes will not be used to identify upsert candidates.
    bool UsePrimaryKeyAsMainArbiterOnUpsertResolve;

    // Maximum allowed number of instantiated objects in transaction.
    // If the limit is exceeded, an exception is thrown.
    std::optional<size_t> MaxInstantiatedObjectsCount;

    // Use new algorithm for MergeAttributes.
    EMergeAttributesMode MergeAttributesMode;

    // History.
    TSelectObjectHistoryConfigPtr SelectObjectHistory;
    EMigrationState HistoryMigrationState;
    EHistoryWriteMode HistoryWriteMode;
    THistorySnapshotPolicyConfigPtr HistorySnapshotPolicy;
    THistoryLockerConfigPtr HistoryLocker;
    bool HistoryCollapseDeletions;

    // Profile
    bool EnablePerUserProfiling;
    bool EnablePerMethodProfiling;

    // GET request, skip/ignore objects with the correct object_id but the wrong parent, treating them as non-existent.
    bool TreatWrongParentAsNonExistentObject;
    // For testing purposes only.
    std::optional<TDuration> CommitDelay;

    // Order-by clause in the request requires limit to be smaller.
    i64 GetOutputRowLimit(bool orderBy) const;

    REGISTER_YSON_STRUCT(TTransactionManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTransactionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TTransactionConfigsSnapshot
{
    TObjectManagerConfigPtr ObjectManager;
    TTransactionManagerConfigPtr TransactionManager;
};

////////////////////////////////////////////////////////////////////////////////

struct TWatchLogDistributionPolicyConfig
    : public NYTree::TYsonStruct
{
    EDistributionType Type;
    // Hash distribution type configuration, does not take any effect for other distribution types.
    EHashMapper HashMapper;
    EHashInput HashInput;
    NClient::NObjects::EHashType HashType;

    REGISTER_YSON_STRUCT(TWatchLogDistributionPolicyConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TWatchLogDistributionPolicyConfig)

////////////////////////////////////////////////////////////////////////////////

struct TWatchManagerDistributionPolicyConfig
    : public NYTree::TYsonStruct
{
    TWatchLogDistributionPolicyConfigPtr Default;

    TWatchLogDistributionPolicyConfigPtr GetByType(TObjectTypeValue objectType) const;

    REGISTER_YSON_STRUCT(TWatchManagerDistributionPolicyConfig);

    static void Register(TRegistrar registrar);

private:
    THashMap<TObjectTypeName, TWatchLogDistributionPolicyConfigPtr> PerType_;
};

DEFINE_REFCOUNTED_TYPE(TWatchManagerDistributionPolicyConfig)

////////////////////////////////////////////////////////////////////////////////

struct TWatchManagerConfig
    : public NYTree::TYsonStruct
{
    bool Enabled;
    bool ProfilingEnabled;
    bool ValidateEventTags;
    bool ValidateRequestTags;
    bool ValidateTokenMinorVersion;

    TDuration ProfilingPeriod;

    TDuration BarrierWaitPollInterval;
    TDuration BarrierWaitMaxTimeLimit;

    ui32 ResultEventCountLimit;
    ui32 PerTabletSelectLimit;

    TWatchManagerDistributionPolicyConfigPtr DistributionPolicy;

    // COMPAT(dgolear): Turn on by default and remove when tested enough, https://st.yandex-team.ru/YTORM-712.
    bool AllowFilteringByMeta;
    std::optional<std::string> QueuesCluster;

    bool UseUpperCapBoundAsTabletCount;

    bool IsLabelsStoreEnabled(TObjectTypeValue objectType) const;
    bool IsQueryFilterEnabled(TObjectTypeValue objectType) const;
    bool IsQuerySelectorEnabled(TObjectTypeValue objectType) const;

    const THashSet<std::string>& GetChangedAttributesPaths(TObjectTypeValue objectType) const;

    EWatchLogState GetLogState(const TObjectTypeName& objectName, const std::string& logName) const;
    bool IsLogStoreEnabled(const TObjectTypeName& objectName, const std::string& logName) const;
    bool IsLogQueryEnabled(const TObjectTypeName& objectName, const std::string& logName, bool explicitly = false) const;

    std::optional<TDuration> GetLogRefreshPeriod(TObjectTypeValue objectType, const std::string& logName) const;

    REGISTER_YSON_STRUCT(TWatchManagerConfig);

    static void Register(TRegistrar registrar);

private:
    bool LabelsStoreEnabled_;
    THashMap<TObjectTypeName, bool> LabelsStoreEnabledPerType_;

    bool QueryFilterEnabled_;
    THashMap<TObjectTypeName, bool> QueryFilterEnabledPerType_;

    // NB! This option does not have any effect on filtering events on the server side.
    // It only adds information about events and could be used for additional filtering on the client side.
    // Be careful modifying this option, as it may invalidate all events in watch logs.
    THashMap<TObjectTypeName, THashSet<std::string>> ChangedAttributesPathsPerType_;

    bool QuerySelectorEnabled_;
    THashMap<TObjectTypeName, bool> QuerySelectorEnabledPerType_;

    THashMap<TObjectTypeName, THashMap<std::string, EWatchLogState>> StatePerTypePerLog_;

    std::optional<TDuration> RefreshDefaultPeriod_;
    THashMap<TObjectTypeName, THashMap<std::string, TDuration>> RefreshPeriodPerTypePerLog_;
};

DEFINE_REFCOUNTED_TYPE(TWatchManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TWatchLogChangedAttributesConfig
    : public TRefCounted
{
    TWatchLogChangedAttributesConfig(
        THashMap<NYPath::TYPath, size_t> pathToIndex,
        std::string md5);

    THashMap<NYPath::TYPath, size_t> PathToIndex;
    std::string MD5;
};

DEFINE_REFCOUNTED_TYPE(TWatchLogChangedAttributesConfig)

////////////////////////////////////////////////////////////////////////////////

struct TWatchManagerChangedAttributesConfig
    : public TRefCounted
{
    using TPerLogConfig = THashMap<
        NClient::NObjects::TObjectTypeValue,
        THashMap<std::string, TWatchLogChangedAttributesConfigPtr>>;

    explicit TWatchManagerChangedAttributesConfig(
        TPerLogConfig typeToConfig);

    TWatchLogChangedAttributesConfigPtr TryGetLogChangedAttributesConfig(
        TObjectTypeValue objectType,
        const std::string& watchLog) const;

private:
    TPerLogConfig PerLog_;
};

DEFINE_REFCOUNTED_TYPE(TWatchManagerChangedAttributesConfig)

////////////////////////////////////////////////////////////////////////////////

struct TWatchManagerExtendedConfig
{
    TWatchManagerExtendedConfig() = default;

    TWatchManagerExtendedConfig(
        TWatchManagerConfigPtr watchManagerConfig,
        TWatchManagerChangedAttributesConfigPtr changedAttributesConfig);

    TWatchManagerConfigPtr WatchManager;
    TWatchManagerChangedAttributesConfigPtr ChangedAttributes;
};

////////////////////////////////////////////////////////////////////////////////

struct TObjectTableReaderConfig
    : public NYTree::TYsonStruct
{
    bool ReadByTablets;
    // If not null, each Read() returns at most this many rows from one shard.
    // The key fields of the table must be the first fields requested from the reader to use this option.
    std::optional<int> BatchSize;

    REGISTER_YSON_STRUCT(TObjectTableReaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TObjectTableReaderConfig)

////////////////////////////////////////////////////////////////////////////////

struct TPoolWeightManagerConfig
    : public NYTree::TYsonStruct
{
    // Fair share worker pool.
    bool EnableFairShareWorkerPool;
    THashMap<std::string, double> FairSharePoolWeights;

    REGISTER_YSON_STRUCT(TPoolWeightManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPoolWeightManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
