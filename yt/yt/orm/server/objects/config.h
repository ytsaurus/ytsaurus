#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/library/query/base/public.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TGroupTypeHandlerConfig
    : public NYTree::TYsonStruct
{
public:
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

DEFINE_ENUM(EParentFinalizationMode,
    // If parent is finalizing (because having finalizing children or having other finalizers),
    // finalize other children too.
    (FinalizeChildren)
);

class TObjectManagerConfig
    : public NYTree::TYsonStruct
{
public:
    // Group.
    TGroupTypeHandlerConfigPtr GroupTypeHandler;

    // Removal.
    EParentFinalizationMode ParentFinalizationMode;
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

    // Schema validation.
    bool EnableTableSchemaValidation;

    // Indexes that are allowed to be read in building mode.
    THashSet<TString> IndexesWithAllowedBuildingRead;

    EUtf8Check Utf8Check;

    NQueryClient::TColumnEvaluatorCacheConfigPtr ColumnEvaluatorCache;

    std::optional<EIndexMode> TryGetIndexMode(const TString& indexName) const;

    std::vector<NYPath::TYPath> GetAllowedExtensibleAttributePaths(TObjectTypeValue objectType) const;

    bool IsHistoryIndexAttributeQueryEnabled(const TObjectTypeName& type, const NYPath::TYPath& attributePath) const;
    bool IsHistoryIndexAttributeStoreEnabled(const TObjectTypeName& type, const NYPath::TYPath& attributePath) const;
    THistoryTime GetHistoryLastTrimTime(const TString& historyTableName) const;
    EParentsTableMode GetParentsTableMode(TObjectTypeValue objectType) const;
    bool MayReadLegacyParentsTable() const;

    REGISTER_YSON_STRUCT(TObjectManagerConfig);

    static void Register(TRegistrar registrar);

private:
    THashMap<TString, EIndexMode> IndexModePerName_;
    THashMap<TObjectTypeName, THashMap<NYPath::TYPath, EIndexMode>> HistoryIndexModePerTypePerAttribute_;
    THashMap<TString, TInstant> LastTrimTimePerHistoryTable_;
    EParentsTableMode DefaultParentsTableMode_;
    THashMap<TString, EParentsTableMode> ParentsTableModes_;

    EIndexMode GetHistoryIndexMode(const TObjectTypeName& type, const NYPath::TYPath& attributePath) const;
};

DEFINE_REFCOUNTED_TYPE(TObjectManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TBatchSizeBackoffConfig
    : public NYTree::TYsonStruct
{
public:
    i64 BatchSizeLimit;
    i64 BatchSizeMultiplier;
    i64 BatchSizeAdditive;
    std::optional<double> BatchSizeAdditiveRelativeToMax;

    REGISTER_YSON_STRUCT(TBatchSizeBackoffConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBatchSizeBackoffConfig)

////////////////////////////////////////////////////////////////////////////////

class TSelectObjectHistoryConfig
    : public TBatchSizeBackoffConfig
{
public:
    i64 OutputEventLimit;
    bool RestrictHistoryFilter;
    bool ForceAllowTimeModeConversion;

    REGISTER_YSON_STRUCT(TSelectObjectHistoryConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSelectObjectHistoryConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMigrationState,
    (Initial)
    (WriteBoth)
    (AfterBackgroundMigration)
    (ReadNew)
    (RevokeOldTokens)
    (Target)
);

////////////////////////////////////////////////////////////////////////////////

class TTransactionManagerConfig
    : public NYTree::TYsonStruct
{
public:
    i64 InputRowLimit;
    i64 OutputRowLimit;
    i64 OrderByOutputRowLimit;
    i64 SelectMemoryLimitPerNode;
    int MaxKeysPerLookupRequest;
    i64 ReadPhaseHardLimit;
    TSelectObjectHistoryConfigPtr SelectObjectHistory;
    bool TimestampBufferEnabled;
    ui64 TimestampBufferSize;
    TDuration TimestampBufferUpdatePeriod;
    ui64 TimestampBufferLowWatermark;
    bool AttributePrerequisiteReadLockEnabled;
    bool AllowIndexPrimaryKeyInContinuationToken;
    bool FullScanAllowedByDefault;
    std::optional<bool> AllowParentRemovalOverride;
    std::optional<bool> AllowRemovalWithNonemptyReferencesOverride;
    bool ScalarAttributeLoadsNullAsTyped;
    bool AnyToOneAttributeValueGetterReturnsNull;
    bool UseAdministerPermission;
    bool IgnoreContinuationSelectivity;
    bool PassUserTagAsSelectExecutionPool;
    bool ValidateDatabaseTimestamp;
    bool FetchFinalizingObjectsByDefault;
    EFetchRootOptimizationLevel FetchRootOptimizationLevel;
    bool ForbidYsonAttributesUsage;
    bool EnableEventLogTableWrite;
    EMigrationState HistoryMigrationState;
    bool BuildKeyExpression;
    bool VersionedSelectEnabled;
    TDuration MinYTRequestTimeout;
    bool EnableDeadlinePropagation;
    bool EnableAttributeMigrations;
    bool EnableComputedFilter;
    // Select result is generated in fractions of requested `LIMIT`.
    double PartialResultBatchFraction;
    // Returns prepared data if remaining time before request timeout is less than timeout slack.
    TDuration PartialResultSelectTimeoutSlack;
    bool CanUseLookupForGetObjects;

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

class TWatchLogDistributionPolicyConfig
    : public NYTree::TYsonStruct
{
public:
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

class TWatchManagerDistributionPolicyConfig
    : public NYTree::TYsonStruct
{
public:
    TWatchLogDistributionPolicyConfigPtr Default;

    TWatchLogDistributionPolicyConfigPtr GetByType(TObjectTypeValue objectType) const;

    REGISTER_YSON_STRUCT(TWatchManagerDistributionPolicyConfig);

    static void Register(TRegistrar registrar);

private:
    THashMap<TObjectTypeName, TWatchLogDistributionPolicyConfigPtr> PerType_;
};

DEFINE_REFCOUNTED_TYPE(TWatchManagerDistributionPolicyConfig)

////////////////////////////////////////////////////////////////////////////////

class TWatchManagerConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enabled;
    bool ProfilingEnabled;
    bool ValidateEventTags;
    bool ValidateRequestTags;
    bool ValidateStartTimestampDataCompleteness;
    bool ValidateTokenMinorVersion;

    TDuration ProfilingPeriod;

    TDuration BarrierWaitPollInterval;
    TDuration BarrierWaitMaxTimeLimit;

    ui32 ResultEventCountLimit;
    ui32 PerTabletSelectLimit;

    TWatchManagerDistributionPolicyConfigPtr DistributionPolicy;

    // COMPAT(dgolear): Turn on by default and remove when tested enough, https://st.yandex-team.ru/YTORM-712.
    bool AllowFilteringByMeta;
    std::optional<TString> QueuesCluster;

    bool IsLabelsStoreEnabled(TObjectTypeValue objectType) const;
    bool IsQueryFilterEnabled(TObjectTypeValue objectType) const;
    bool IsQuerySelectorEnabled(TObjectTypeValue objectType) const;

    const THashSet<TString>& GetChangedAttributesPaths(TObjectTypeValue objectType) const;

    EWatchLogState GetLogState(const TString& objectName, const TString& logName) const;
    bool IsLogStoreEnabled(const TObjectTypeName& objectName, const TString& logName) const;
    bool IsLogQueryEnabled(const TObjectTypeName& objectName, const TString& logName, bool explicitly = false) const;

    std::optional<TDuration> GetLogRefreshPeriod(TObjectTypeValue objectType, const TString& logName) const;

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
    THashMap<TObjectTypeName, THashSet<TString>> ChangedAttributesPathsPerType_;

    bool QuerySelectorEnabled_;
    THashMap<TObjectTypeName, bool> QuerySelectorEnabledPerType_;

    THashMap<TObjectTypeName, THashMap<TString, EWatchLogState>> StatePerTypePerLog_;

    std::optional<TDuration> RefreshDefaultPeriod_;
    THashMap<TObjectTypeName, THashMap<TString, TDuration>> RefreshPeriodPerTypePerLog_;
};

DEFINE_REFCOUNTED_TYPE(TWatchManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TWatchLogChangedAttributesConfig
    : public TRefCounted
{
    TWatchLogChangedAttributesConfig(
        THashMap<TString, size_t> pathToIndex,
        TString md5);

    THashMap<TString, size_t> PathToIndex;
    TString MD5;
};

DEFINE_REFCOUNTED_TYPE(TWatchLogChangedAttributesConfig)

////////////////////////////////////////////////////////////////////////////////

class TWatchManagerChangedAttributesConfig
    : public TRefCounted
{
public:
    using TPerLogConfig = THashMap<
        NClient::NObjects::TObjectTypeValue,
        THashMap<TString, TWatchLogChangedAttributesConfigPtr>>;

    explicit TWatchManagerChangedAttributesConfig(
        TPerLogConfig typeToConfig);

    TWatchLogChangedAttributesConfigPtr TryGetLogChangedAttributesConfig(
        TObjectTypeValue objectType,
        const TString& watchLog) const;

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

class TObjectTableReaderConfig
    : public NYTree::TYsonStruct
{
public:
    bool ReadByTablets;
    // If not null, each Read() returns at most this many rows from one shard.
    // The key fields of the table must be the first fields requested from the reader to use this option.
    std::optional<int> BatchSize;

    REGISTER_YSON_STRUCT(TObjectTableReaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TObjectTableReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TPoolWeightManagerConfig
    : public NYTree::TYsonStruct
{
public:
    // Fair share worker pool.
    bool EnableFairShareWorkerPool;
    THashMap<TString, double> FairSharePoolWeights;

    REGISTER_YSON_STRUCT(TPoolWeightManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPoolWeightManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
