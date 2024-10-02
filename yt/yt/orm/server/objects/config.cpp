#include "config.h"

#include <yt/yt/orm/client/objects/registry.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/library/query/engine_api/config.h>

namespace NYT::NOrm::NServer::NObjects {

using namespace NClient::NObjects;

////////////////////////////////////////////////////////////////////////////////

void TGroupTypeHandlerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_members_validation", &TThis::EnableMembersValidation)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

std::optional<EIndexMode> TObjectManagerConfig::TryGetIndexMode(const TString& indexName) const
{
    if (auto it = IndexModePerName_.find(indexName); it != IndexModePerName_.end()) {
        return it->second;
    }
    return std::nullopt;
}

std::vector<NYPath::TYPath> TObjectManagerConfig::GetAllowedExtensibleAttributePaths(
    TObjectTypeValue objectType) const
{
    auto it = AllowedExtensibleAttributePaths.find(
        GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(objectType));
    if (it != AllowedExtensibleAttributePaths.end()) {
        return it->second;
    }
    return {};
}

bool TObjectManagerConfig::IsHistoryIndexAttributeQueryEnabled(
    const TObjectTypeName& type, const TString& attributePath) const
{
    auto mode = GetHistoryIndexMode(type, attributePath);
    return mode == EIndexMode::Enabled;
}

bool TObjectManagerConfig::IsHistoryIndexAttributeStoreEnabled(
    const TObjectTypeName& type, const TString& attributePath) const
{
    auto mode = GetHistoryIndexMode(type, attributePath);
    return mode == EIndexMode::Enabled || mode == EIndexMode::Building;
}

THistoryTime TObjectManagerConfig::GetHistoryLastTrimTime(const TString& historyTable) const
{
    auto it = LastTrimTimePerHistoryTable_.find(historyTable);
    if (it != LastTrimTimePerHistoryTable_.end()) {
        return THistoryTime{it->second};
    }
    return {};
}

EParentsTableMode TObjectManagerConfig::GetParentsTableMode(TObjectTypeValue objectType) const
{
    auto it = ParentsTableModes_.find(
        GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(objectType));
    if (it != ParentsTableModes_.end()) {
        return it->second;
    }
    return DefaultParentsTableMode_;
}

bool TObjectManagerConfig::MayReadLegacyParentsTable() const
{
    for (auto& mode : ParentsTableModes_) {
        if (mode.second != EParentsTableMode::DontWriteToCommonTable) {
            return true;
        }
    }
    return DefaultParentsTableMode_ != EParentsTableMode::DontWriteToCommonTable;
}

void TObjectManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("group_type_handler", &TThis::GroupTypeHandler)
        .DefaultNew();

    registrar.Parameter("parent_finalization_mode", &TThis::ParentFinalizationMode)
        .Default(EParentFinalizationMode::FinalizeChildren);
    registrar.Parameter("remove_mode", &TThis::RemoveMode)
        .Default(ERemoveObjectMode::Asynchronous);
    registrar.Parameter("removed_objects_sweep_period", &TThis::RemovedObjectsSweepPeriod)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("removed_objects_grace_timeout", &TThis::RemovedObjectsGraceTimeout)
        .Default(TDuration::Hours(24));
    registrar.Parameter("removed_object_table_reader", &TThis::RemovedObjectTableReader)
        .DefaultNew();

    registrar.Parameter("attributes_extensibility_mode", &TThis::AttributesExtensibilityMode)
        .Default(EAttributesExtensibilityMode::None);

    registrar.Parameter("allowed_extensible_attribute_paths", &TThis::AllowedExtensibleAttributePaths)
        .Default({});

    registrar.Parameter("enable_history", &TThis::EnableHistory)
        .Default(true);
    registrar.Parameter("history_disabled_types", &TThis::HistoryDisabledTypes)
        .Default();
    registrar.Parameter("history_index_group_by_enabled", &TThis::HistoryIndexGroupByEnabled)
        .Default(false);
    registrar.Parameter("history_index_mode_per_type_per_attribute", &TThis::HistoryIndexModePerTypePerAttribute_)
        .Default();
    registrar.Parameter("last_trim_time_per_history_table", &TThis::LastTrimTimePerHistoryTable_)
        .Default();
    registrar.Parameter("default_parents_table_mode", &TThis::DefaultParentsTableMode_)
        .Default(EParentsTableMode::WriteToSeparateTable);
    registrar.Parameter("parents_table_modes", &TThis::ParentsTableModes_)
        .Default();

    registrar.Parameter("enable_table_schema_validation", &TThis::EnableTableSchemaValidation)
        .Default(true);

    registrar.Parameter("index_mode_per_name", &TThis::IndexModePerName_)
        .Default({});
    registrar.Parameter("indexes_with_allowed_building_read", &TThis::IndexesWithAllowedBuildingRead)
        .Default();

    registrar.Parameter("utf8_check", &TThis::Utf8Check)
        .Default(EUtf8Check::Disable);

    registrar.Parameter("column_evaluator_cache", &TThis::ColumnEvaluatorCache)
        .DefaultNew();
}

EIndexMode TObjectManagerConfig::GetHistoryIndexMode(
    const TObjectTypeName& type, const NYPath::TYPath& attributePath) const
{
    auto perObjectIt = HistoryIndexModePerTypePerAttribute_.find(type);

    if (perObjectIt != HistoryIndexModePerTypePerAttribute_.end()) {
        auto it = perObjectIt->second.find(attributePath);
        if (it != perObjectIt->second.end()) {
            return it->second;
        }
    }
    return EIndexMode::Building;
}

////////////////////////////////////////////////////////////////////////////////

void TBatchSizeBackoffConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("batch_size_limit", &TThis::BatchSizeLimit)
        .Default(100'000);
    registrar.Parameter("batch_size_multiplier", &TThis::BatchSizeMultiplier)
        .Default(2)
        .GreaterThanOrEqual(2);
    registrar.Parameter("batch_size_additive", &TThis::BatchSizeAdditive)
        .Default(100)
        .GreaterThanOrEqual(1);
    registrar.Parameter("batch_size_additive_relative_to_max", &TThis::BatchSizeAdditiveRelativeToMax)
        .Default(0.1);

    registrar.Postprocessor([] (TThis* config) {
        if (config->BatchSizeAdditiveRelativeToMax) {
            auto value = *config->BatchSizeAdditiveRelativeToMax;
            if (value < 0.0 || value > 1.0) {
                THROW_ERROR_EXCEPTION(
                    "Parameter \"batch_size_additive_relative_to_max\" must be in range [0, 1], but got %v",
                    value);
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TSelectObjectHistoryConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("output_event_limit", &TThis::OutputEventLimit)
        .Default(100'000)
        .GreaterThanOrEqual(1);
    registrar.Parameter("restrict_history_filter", &TThis::RestrictHistoryFilter)
        .Default(false);
    registrar.Parameter("force_allow_time_mode_conversion", &TThis::ForceAllowTimeModeConversion)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TTransactionManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("input_row_limit", &TThis::InputRowLimit)
        .Default(10'000'000);
    registrar.Parameter("output_row_limit", &TThis::OutputRowLimit)
        .Default(10'000'000);
    registrar.Parameter("order_by_output_row_limit", &TThis::OrderByOutputRowLimit)
        .Default(1'000'000);
    registrar.Parameter("select_memory_limit_per_node", &TThis::SelectMemoryLimitPerNode)
        .Default(1_GB);
    registrar.Parameter("read_phase_hard_limit", &TThis::ReadPhaseHardLimit)
        .Default(0);
    registrar.Parameter("max_keys_per_lookup_request", &TThis::MaxKeysPerLookupRequest)
        .GreaterThan(0)
        .Default(2'000);
    registrar.Parameter("select_object_history", &TThis::SelectObjectHistory)
        .DefaultNew();
    registrar.Parameter("timestamp_buffer_enabled", &TThis::TimestampBufferEnabled)
        .Default(false);
    registrar.Parameter("timestamp_buffer_size", &TThis::TimestampBufferSize)
        .Default(1'000);
    registrar.Parameter("timestamp_buffer_update_period", &TThis::TimestampBufferUpdatePeriod)
        .Default(TDuration::MilliSeconds(500));
    registrar.Parameter("timestamp_buffer_low_watermark", &TThis::TimestampBufferLowWatermark)
        .Default(200);
    // COMPAT(dgolear)
    registrar.Parameter("attribute_prerequisite_read_lock_enabled",
        &TThis::AttributePrerequisiteReadLockEnabled)
        .Default(false);
    // TODO(bulatman): remove after YT-16716 solved.
    registrar.Parameter("allow_index_primary_key_in_continuation_token",
        &TThis::AllowIndexPrimaryKeyInContinuationToken)
        .Default(false);
    registrar.Parameter("full_scan_allowed_by_default", &TThis::FullScanAllowedByDefault)
        .Default(true);
    registrar.Parameter("allow_parent_removal_override", &TThis::AllowParentRemovalOverride)
        .Default();
    registrar.Parameter(
        "allow_removal_with_nonempty_reference_override",
        &TThis::AllowRemovalWithNonemptyReferencesOverride)
        .Default();
    registrar.Parameter("scalar_attribute_loads_null_as_typed", &TThis::ScalarAttributeLoadsNullAsTyped)
        .Default(true);
    registrar.Parameter("any_to_one_attribute_value_getter_returns_null",
        &TThis::AnyToOneAttributeValueGetterReturnsNull)
        .Default(true);
    registrar.Parameter("use_administer_permission", &TThis::UseAdministerPermission)
        .Default(false);
    // TODO(bulatman): remove after YT-16716 solved.
    registrar.Parameter("ignore_continuation_selectivity", &TThis::IgnoreContinuationSelectivity)
        .Default(false);

    registrar.Parameter("commit_delay", &TThis::CommitDelay)
        .Default();
    registrar.Parameter("pass_user_tag_as_select_execution_pool", &TThis::PassUserTagAsSelectExecutionPool)
        .Default(false);
    registrar.Parameter("validate_database_timestamp", &TThis::ValidateDatabaseTimestamp)
        .Default(true);
    registrar.Parameter("fetch_finalizing_objects_by_default", &TThis::FetchFinalizingObjectsByDefault)
        .Default(true);
    registrar.Parameter("fetch_root_optimization_level", &TThis::FetchRootOptimizationLevel)
        .Default(EFetchRootOptimizationLevel::None);
    registrar.Parameter("forbid_yson_attributes_usage", &TThis::ForbidYsonAttributesUsage)
        .Default(true);
    registrar.Parameter("enable_event_log_table_write", &TThis::EnableEventLogTableWrite)
        .Default(false);
    registrar.Parameter("history_migration_state", &TThis::HistoryMigrationState)
        .Default(EMigrationState::Initial);
    registrar.Parameter("build_key_expression", &TThis::BuildKeyExpression)
        .Default(true);
    registrar.Parameter("versioned_select_enabled", &TThis::VersionedSelectEnabled)
        .Default(false);
    registrar.Parameter("min_yt_request_timeout", &TThis::MinYTRequestTimeout)
        .Default(TDuration::MilliSeconds(500));
    registrar.Parameter("enable_deadline_propagation", &TThis::EnableDeadlinePropagation)
        .Default(false);
    registrar.Parameter("enable_attribute_migrations", &TThis::EnableAttributeMigrations)
        .Default(true);
    registrar.Parameter("enable_computed_filter", &TThis::EnableComputedFilter)
        .Default(true);
    registrar.Parameter("partial_result_batch_fraction", &TThis::PartialResultBatchFraction)
        .Default(1.0);
    registrar.Parameter("partial_result_select_timeout_slack", &TThis::PartialResultSelectTimeoutSlack)
        .Default(TDuration::MilliSeconds(500));
    registrar.Parameter("can_use_lookup_for_get_objects", &TThis::CanUseLookupForGetObjects)
        .Default(false);

    registrar.Postprocessor([] (TThis* config) {
        if (config->TimestampBufferLowWatermark >= config->TimestampBufferSize) {
            THROW_ERROR_EXCEPTION("timestamp_buffer_low_watermark must be less than timestamp_buffer_size");
        }
    });
}

i64 TTransactionManagerConfig::GetOutputRowLimit(bool orderBy) const
{
    if (orderBy) {
        return OrderByOutputRowLimit;
    } else {
        return OutputRowLimit;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TWatchLogDistributionPolicyConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("type", &TThis::Type)
        .Default(EDistributionType::Uniform);
    registrar.Parameter("hash_mapper", &TThis::HashMapper)
        .Default(EHashMapper::Modulo);
    registrar.Parameter("hash_input", &TThis::HashInput)
        .Default(EHashInput::ObjectKey);
    registrar.Parameter("hash_type", &TThis::HashType)
        .Default(EHashType::ArcadiaUtil);
}

////////////////////////////////////////////////////////////////////////////////

TWatchLogDistributionPolicyConfigPtr TWatchManagerDistributionPolicyConfig::GetByType(
    TObjectTypeValue objectType) const
{
    auto it = PerType_.find(GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(objectType));
    return it == PerType_.end() ? Default : it->second;
}

void TWatchManagerDistributionPolicyConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("default", &TThis::Default)
        .DefaultNew();

    registrar.Parameter("per_type", &TThis::PerType_)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

bool TWatchManagerConfig::IsLabelsStoreEnabled(TObjectTypeValue objectType) const
{
    return GetOrDefault(LabelsStoreEnabledPerType_,
        GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(objectType),
        LabelsStoreEnabled_);
}

bool TWatchManagerConfig::IsQueryFilterEnabled(TObjectTypeValue objectType) const
{
    return GetOrDefault(QueryFilterEnabledPerType_,
        GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(objectType),
        QueryFilterEnabled_);
}

bool TWatchManagerConfig::IsQuerySelectorEnabled(TObjectTypeValue objectType) const
{
    return GetOrDefault(QuerySelectorEnabledPerType_,
        GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(objectType),
        QuerySelectorEnabled_);
}

const THashSet<TString>& TWatchManagerConfig::GetChangedAttributesPaths(TObjectTypeValue objectType) const
{
    static const THashSet<TString> EmptyHashSet;
    auto it = ChangedAttributesPathsPerType_.find(GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(objectType));
    return it == ChangedAttributesPathsPerType_.end()
        ? EmptyHashSet
        : it->second;
}

bool TWatchManagerConfig::IsLogStoreEnabled(const TObjectTypeName& objectName, const TString& logName) const
{
    auto mode = GetLogState(objectName, logName);
    return mode == EWatchLogState::QueryStore || mode == EWatchLogState::Store;
}

bool TWatchManagerConfig::IsLogQueryEnabled(const TObjectTypeName& objectName, const TString& logName, bool explicitly) const
{
    auto mode = GetLogState(objectName, logName);
    return mode == EWatchLogState::QueryStore || explicitly && mode == EWatchLogState::Store;
}

std::optional<TDuration> TWatchManagerConfig::GetLogRefreshPeriod(
    TObjectTypeValue objectType, const TString& logName) const
{
    const auto& objectTypeName = GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(objectType);
    if (!IsLogStoreEnabled(objectTypeName, logName)) {
        return std::nullopt;
    }
    if (auto perObjectIt = RefreshPeriodPerTypePerLog_.find(objectTypeName);
        perObjectIt != RefreshPeriodPerTypePerLog_.end())
    {
        if (auto it = perObjectIt->second.find(logName); it != perObjectIt->second.end()) {
            return it->second;
        }
    }
    return RefreshDefaultPeriod_;
}

void TWatchManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enabled", &TThis::Enabled)
        .Default(true);
    registrar.Parameter("profiling_enabled", &TThis::ProfilingEnabled)
        .Default(true);
    registrar.Parameter("validate_event_tags", &TThis::ValidateEventTags)
        .Default(true);
    registrar.Parameter("validate_request_tags", &TThis::ValidateRequestTags)
        .Default(true);
    registrar.Parameter("validate_start_timestamp_data_completeness", &TThis::ValidateStartTimestampDataCompleteness)
        .Default(true);
    registrar.Parameter("validate_token_minor_version", &TThis::ValidateTokenMinorVersion)
        .Default(false);

    registrar.Parameter("barrier_wait_poll_interval", &TThis::BarrierWaitPollInterval)
        .Default(TDuration::MilliSeconds(50));
    registrar.Parameter("barrier_wait_max_time_limit", &TThis::BarrierWaitMaxTimeLimit)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("result_event_count_limit", &TThis::ResultEventCountLimit)
        .Default(25'000)
        .GreaterThan(0);
    registrar.Parameter("per_tablet_select_limit", &TThis::PerTabletSelectLimit)
        .Default(5'000)
        .LessThanOrEqual(50'000)
        .GreaterThan(0);

    registrar.Parameter("profiling_period", &TThis::ProfilingPeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("distribution_policy", &TThis::DistributionPolicy)
        .DefaultNew();

    registrar.Parameter("allow_filtering_by_meta", &TThis::AllowFilteringByMeta)
        .Default(false);

    registrar.Parameter("queues_cluster", &TThis::QueuesCluster)
        .Default();

    registrar.Parameter("state_per_type_per_log", &TThis::StatePerTypePerLog_)
        .Default();

    registrar.Parameter("refresh_default_period", &TThis::RefreshDefaultPeriod_)
        .Default();
    registrar.Parameter("refresh_period_per_type_per_log", &TThis::RefreshPeriodPerTypePerLog_)
        .Default();

    registrar.Parameter("labels_store_enabled", &TThis::LabelsStoreEnabled_)
        .Default(false);
    registrar.Parameter("labels_store_enabled_per_type", &TThis::LabelsStoreEnabledPerType_)
        .Default({});

    registrar.Parameter("query_filter_enabled", &TThis::QueryFilterEnabled_)
        .Default(false);
    registrar.Parameter("query_filter_enabled_per_type", &TThis::QueryFilterEnabledPerType_)
        .Default({});

    registrar.Parameter("changed_attributes_paths_per_type", &TThis::ChangedAttributesPathsPerType_)
        .Default({});

    registrar.Parameter("query_selector_enabled", &TThis::QuerySelectorEnabled_)
        .Default(false);
    registrar.Parameter("query_selector_enabled_per_type", &TThis::QuerySelectorEnabledPerType_)
        .Default({});
}

EWatchLogState TWatchManagerConfig::GetLogState(const TObjectTypeName& objectName, const TString& logName) const
{
    auto perObjectIt = StatePerTypePerLog_.find(objectName);

    if (perObjectIt != StatePerTypePerLog_.end()) {
        auto it = perObjectIt->second.find(logName);
        if (it != perObjectIt->second.end()) {
            return it->second;
        }
    }
    return EWatchLogState::QueryStore;
}

////////////////////////////////////////////////////////////////////////////////

TWatchLogChangedAttributesConfig::TWatchLogChangedAttributesConfig(
    THashMap<TString, size_t> pathToIndex,
    TString md5)
    : PathToIndex(std::move(pathToIndex))
    , MD5(md5)
{ }

TWatchManagerChangedAttributesConfig::TWatchManagerChangedAttributesConfig(
    TWatchManagerChangedAttributesConfig::TPerLogConfig typeToConfig)
    : PerLog_(std::move(typeToConfig))
{ }

TWatchManagerExtendedConfig::TWatchManagerExtendedConfig(
    TWatchManagerConfigPtr watchManagerConfig,
    TWatchManagerChangedAttributesConfigPtr changedAttributesConfig)
    : WatchManager(std::move(watchManagerConfig))
    , ChangedAttributes(std::move(changedAttributesConfig))
{ }

TWatchLogChangedAttributesConfigPtr TWatchManagerChangedAttributesConfig::TryGetLogChangedAttributesConfig(
    TObjectTypeValue objectType,
    const TString& watchLog) const
{
    if (auto it = PerLog_.find(objectType); it != PerLog_.end()) {
        if (auto configIt = it->second.find(watchLog); configIt != it->second.end()) {
            return configIt->second;
        }
    }
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

void TObjectTableReaderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("read_by_tablets", &TThis::ReadByTablets)
        .Default(true);

    registrar.Parameter("batch_size", &TThis::BatchSize)
        .Optional();

    registrar.Postprocessor([] (TThis* config) {
        if (config->BatchSize && *config->BatchSize < 1) {
            THROW_ERROR_EXCEPTION("Invalid batch size: %v", *config->BatchSize);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TPoolWeightManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_fair_share_worker_pool", &TThis::EnableFairShareWorkerPool)
        .Default(true);
    registrar.Parameter("fair_share_pool_weights", &TThis::FairSharePoolWeights)
        .Default({});

    registrar.Postprocessor([] (TThis* config) {
        config->FairSharePoolWeights.try_emplace("default", 1.0);
        config->FairSharePoolWeights.try_emplace("root", 4.0);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
