#include "watch_manager.h"

#include "attribute_schema.h"
#include "connection_validators.h"
#include "continuation.h"
#include "db_config.h"
#include "db_schema.h"
#include "helpers.h"
#include "private.h"
#include "transaction.h"
#include "transaction_manager.h"
#include "watch_log.h"
#include "watch_log_consumer_interop.h"
#include "watch_log_event_matcher.h"

#include <yt/yt/orm/server/objects/proto/continuation_token.pb.h>

#include <yt/yt/orm/server/master/bootstrap.h>
#include <yt/yt/orm/server/master/config.h>
#include <yt/yt/orm/server/master/helpers.h>
#include <yt/yt/orm/server/master/yt_connector.h>

#include <yt/yt/orm/client/objects/registry.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/api/rowset.h>

#include <yt/yt/library/profiling/producer.h>

#include <yt/yt/library/syncmap/map.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/crypto/crypto.h>

#include <yt/yt/core/misc/atomic_object.h>
#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/ypath/helpers.h>

#include <library/cpp/iterator/enumerate.h>

#include <util/datetime/cputimer.h>

namespace NYT::NOrm::NServer::NObjects {

using namespace NYT::NApi;
using namespace NYT::NConcurrency;
using namespace NYT::NTableClient;
using namespace NYT::NYTree;
using namespace NYT::NYson;
using namespace NYT::NProfiling;

////////////////////////////////////////////////////////////////////////////////

namespace {

static const TString WatchLogConsumerProfilerUserTag = "watch-log-consumer-profiler";

TString CalculateConfigurationMD5Hash(const std::set<TString>& configuration)
{
    NCrypto::TMD5Hasher hasher;
    for (const auto& string : configuration) {
        hasher.Append(string);
    }
    return hasher.GetHexDigestUpperCase();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TWatchLogConsumerSensors
{
public:
    TWatchLogConsumerSensors(
        const TProfiler profiler,
        TObjectId consumerId)
        : Profiler_(profiler.WithRequiredTag("consumer", consumerId))
        , ConsumerId_(std::move(consumerId))
    { }

    void Update(
        const NProto::TWatchQueryContinuationToken& continuationToken,
        const std::vector<TTabletInfo>& tabletInfos)
    {
        PerTabletLags_.reserve(continuationToken.event_offsets_size());
        for (const auto& eventOffset : continuationToken.event_offsets()) {
            int tabletIndex = eventOffset.tablet_index();
            THROW_ERROR_EXCEPTION_IF(tabletIndex < 0 || tabletIndex >= std::ssize(tabletInfos),
                "Watch log consumer %Qv continuation token offset tablet index %v is out of [0, %v) range",
                ConsumerId_,
                eventOffset.tablet_index(),
                tabletInfos.size());
            auto it = PerTabletLags_.find(tabletIndex);
            if (it == PerTabletLags_.end()) {
                auto profiler = Profiler_.WithTag("tablet_index", ToString(tabletIndex));
                it = PerTabletLags_.emplace(
                    tabletIndex,
                    TSensors{
                        .Offset = profiler.Gauge("/watch/lag"),
                        .TimeOffsetSeconds = profiler.Gauge("/watch/consumer_lag/time"),
                    }).first;
            }

            auto offset = tabletInfos[tabletIndex].TotalRowCount - eventOffset.row_index();
            auto timeOffset = GetTimeOffset(tabletIndex, eventOffset, tabletInfos);

            it->second.Offset.Update(offset);
            if (timeOffset) {
                it->second.TimeOffsetSeconds.Update(timeOffset->SecondsFloat());
            }
        }
    }

private:
    struct TSensors
    {
        TGauge Offset;
        TGauge TimeOffsetSeconds;
    };

    const TProfiler Profiler_;
    const TString ConsumerId_;

    THashMap<int, TSensors> PerTabletLags_;

    std::optional<TDuration> GetTimeOffset(
        int tabletIndex,
        const NProto::TWatchQueryContinuationToken::TEventIndex& eventOffset,
        const std::vector<TTabletInfo>& tabletInfos)
    {
        std::optional<TDuration> timeOffset;
        if (eventOffset.has_timestamp()) {
            auto eventTime = NTransactionClient::UnixTimeFromTimestamp(eventOffset.timestamp());
            auto lastWriteTime = NTransactionClient::UnixTimeFromTimestamp(
                tabletInfos[tabletIndex].LastWriteTimestamp);
            if (lastWriteTime == 0) {
                timeOffset.emplace(TDuration::Zero());
            } else {
                if (lastWriteTime < eventTime) {
                    THROW_ERROR_EXCEPTION("Watch log consumer %Qv continuation token timestamp %v "
                        "is greater than tablet last write timestamp %v",
                        ConsumerId_,
                        eventOffset.timestamp(),
                        tabletInfos[tabletIndex].LastWriteTimestamp)
                        << TErrorAttribute("tablet_index", tabletIndex);
                }

                timeOffset.emplace(TDuration::Seconds(lastWriteTime - eventTime));
            }
        }
        return timeOffset;
    }
};

DECLARE_REFCOUNTED_STRUCT(TWatchLogSensors)

struct TWatchLogSensors
    : public TRefCounted
{
    const TProfiler Profiler;

    TSyncMap<i64, TCounter> SkippedRowsCounterPerTablet;

    // Updated only in `WatchLogMan` thread.
    THashMap<TString, TWatchLogConsumerSensors> WatchLogConsumerSensors;

    TWatchLogSensors(TString objectName, TString logName)
        : Profiler(NObjects::Profiler.WithTags(NProfiling::TTagSet()
            .WithRequiredTag(NProfiling::TTag("object_type", std::move(objectName)))
            .WithRequiredTag(NProfiling::TTag("log_name", std::move(logName))))
            .WithSparse())
    { }
};

DEFINE_REFCOUNTED_TYPE(TWatchLogSensors)

////////////////////////////////////////////////////////////////////////////////

struct TRegisteredLog
    : public TWatchLog
{
    std::unique_ptr<TDBTable> Table;
    std::unique_ptr<IWatchLogEventMatcher> EventMatcher;

    TWatchLogSensorsPtr Sensors;
    TPeriodicExecutorPtr LogRefreshExecutor;

    TRegisteredLog(
        TWatchLog log,
        std::unique_ptr<TDBTable> table,
        std::unique_ptr<IWatchLogEventMatcher> matcher,
        TPeriodicExecutorPtr refreshExecutor)
        : TWatchLog(std::move(log))
        , Table(std::move(table))
        , EventMatcher(std::move(matcher))
        , Sensors(New<TWatchLogSensors>(ObjectName, Name))
        , LogRefreshExecutor(std::move(refreshExecutor))
    { }
};

////////////////////////////////////////////////////////////////////////////////

struct TWatchLogConsumerDetail
{
    TSnapshotWatchLogConsumer SnapshotWatchLogConsumer;
    NProto::TWatchQueryContinuationToken DeserializedContinuationToken;
};

////////////////////////////////////////////////////////////////////////////////

class TWatchManager::TImpl
    : public TRefCounted
{
public:
    TImpl(NMaster::IBootstrap* bootstrap, TWatchManagerConfigPtr config)
        : Bootstrap_(bootstrap)
        , InitialConfig_(config)
        , WatchLogManagerQueue_(New<TActionQueue>("WatchLogMan"))
        , WatchLogProfilingExecutor_(New<TPeriodicExecutor>(
            WatchLogManagerQueue_->GetInvoker(),
            BIND(&TImpl::OnWatchLogProfile, MakeWeak(this)),
            config->ProfilingPeriod))
        , ErrorCount_(Profiler.Counter("/watch/error_count"))
    {
        Bootstrap_->SubscribeConfigUpdate(BIND(&TImpl::OnConfigUpdate, MakeWeak(this)));
    }

    void Initialize()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        // SetConfig() should be invoked only after all logs are registered, so it cannot be called from constructor.
        SetConfig(InitialConfig_);

        const auto& ytConnector = Bootstrap_->GetYTConnector();
        ytConnector->SubscribeValidateConnection(BIND(&TImpl::OnValidateConnection, MakeWeak(this)));
        ytConnector->SubscribeStartedLeading(BIND(&TImpl::OnStartedLeading, MakeWeak(this)));
        ytConnector->SubscribeStoppedLeading(BIND(&TImpl::OnStoppedLeading, MakeWeak(this)));
    }

    TWatchManagerConfigPtr GetConfig() const
    {
        return ExtendedConfig_.Load().WatchManager;
    }

    TFuture<std::vector<TTabletInfo>> GetTabletInfos(
        TObjectTypeValue objectType,
        const TString& logName,
        const std::vector<int>& tablets)
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        const auto& client = ytConnector->GetClient(ytConnector->FormatUserTag());
        const auto tablePath = ytConnector->GetTablePath(GetLogTableOrThrow(objectType, logName));

        if (!tablets.empty()) {
            return client->GetTabletInfos(tablePath, tablets);
        } else {
            return client->GetTableMountCache()->GetTableInfo(tablePath)
                .Apply(BIND([client, tablePath] (const NTabletClient::TTableMountInfoPtr& tableMountInfo) {
                    std::vector<int> indexes(tableMountInfo->Tablets.size());
                    std::iota(indexes.begin(), indexes.end(), 0);
                    return client->GetTabletInfos(tablePath, indexes);
                }));
        }
    }

    TFuture<int> GetTabletCount(TObjectTypeValue objectType, const TString& logName)
    {
        return GetTableMountInfo(Bootstrap_->GetYTConnector(), GetLogTableOrThrow(objectType, logName))
            .Apply(BIND([] (const NTabletClient::TTableMountInfoPtr& tableMountInfo) {
                return static_cast<int>(tableMountInfo->Tablets.size());
            }));
    }

    TErrorOr<std::vector<TTabletInfo>> WaitForBarrier(
        TObjectTypeValue objectType,
        const TString& logName,
        const std::vector<int>& tablets,
        TTimestamp barrierTimestamp,
        TDuration timeLimit)
    {
        YT_LOG_DEBUG("Waiting for barrier");

        auto config = GetConfig();

        if (timeLimit > config->BarrierWaitMaxTimeLimit) {
            return TError("Time limit must be less or equal to %v, but got %v",
                config->BarrierWaitMaxTimeLimit,
                timeLimit);
        }

        TErrorOr<std::vector<TTabletInfo>> tabletInfosOrError;

        TWallTimer timer;
        timer.Start();

        do {
            tabletInfosOrError = WaitFor(GetTabletInfos(objectType, logName, tablets));
            if (!tabletInfosOrError.IsOK()) {
                return tabletInfosOrError;
            }
            auto currentBarrierTimestamp = GetBarrierTimestamp(tabletInfosOrError.Value());

            if (currentBarrierTimestamp >= barrierTimestamp) {
                YT_LOG_DEBUG("Barrier timestamp reached (Target: %v, Current: %v)",
                    barrierTimestamp,
                    currentBarrierTimestamp);
                break;
            }

            if (timer.GetElapsedTime() > timeLimit) {
                return TError("Time limit exceeded waiting for barrier timestamp")
                    << TErrorAttribute("current_timestamp", currentBarrierTimestamp)
                    << TErrorAttribute("target_timestamp", barrierTimestamp)
                    << TErrorAttribute("time_limit", timeLimit);
            }

            YT_LOG_DEBUG(
                "Barrier timestamp not reached yet, falling asleep (Target: %v, Current: %v, SleepDuration: %v)",
                barrierTimestamp,
                currentBarrierTimestamp,
                config->BarrierWaitPollInterval);

            TDelayedExecutor::WaitForDuration(config->BarrierWaitPollInterval);
        } while (true);

        return tabletInfosOrError;
    }

    void RegisterLogs(const IObjectTypeHandler* typeHandler)
    {
        YT_VERIFY(typeHandler);
        auto objectType = typeHandler->GetType();

        auto watchLogs = typeHandler->GetWatchLogs();
        for (const auto& log: watchLogs) {
            ValidateSelectors(log.Selector, typeHandler, /*abortOnFail*/ true);
        }

        EmplaceOrCrash(LogsPerObjectType_, objectType, std::move(watchLogs));

        const auto& objectTableName = typeHandler->GetTable()->GetName();
        for (const auto& log : LogsPerObjectType_[objectType]) {
            THROW_ERROR_EXCEPTION_UNLESS(log.Name.EndsWith("watch_log"),
                "Watch log name must end with _watch_log, but got %Qv", log.Name);
            auto logTableName = objectTableName + "_" + log.Name;
            YT_LOG_INFO("Registering watch log (ObjectType: %v, ObjectTableName: %v, LogName: %v)",
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(objectType),
                objectTableName,
                log.Name);
            auto table = std::make_unique<TDBTable>(logTableName);
            table->Initialize(
                /*keyFields*/ {&WatchLogSchema.Key.TabletIndex},
                /*otherFields*/ {&WatchLogSchema.Fields.Record});

            bool allowFilteringByMeta = log.Filter.Query.Contains("/meta");
            auto eventMatcher = CreateWatchLogEventMatcher(
                log.Filter,
                log.Selector,
                log.RequiredTags,
                log.ExcludedTags,
                InitialConfig_->ValidateEventTags,
                allowFilteringByMeta);

            auto watchLogRefreshExecutor = New<TPeriodicExecutor>(
                WatchLogManagerQueue_->GetInvoker(),
                BIND(&TImpl::OnRefreshWatchLog, MakeWeak(this), objectType, log.Name));

            const std::pair<TObjectTypeValue, const TString&> key{objectType, log.Name};

            EmplaceOrCrash(
                RegisteredLogs_,
                key,
                TRegisteredLog(
                    log,
                    std::move(table),
                    std::move(eventMatcher),
                    std::move(watchLogRefreshExecutor)));
        }
    }

    const TDBTable* GetLogTableOrCrash(TObjectTypeValue type, const TString& logName) const
    {
        const auto& table = GetLogByNameOrCrash(type, logName).Table;
        YT_VERIFY(table);
        return table.get();
    }

    const TDBTable* GetLogTableOrThrow(TObjectTypeValue type, const TString& logName) const
    {
        const auto& table = GetLogByNameOrThrow(type, logName).Table;
        YT_VERIFY(table);
        return table.get();
    }

    const std::vector<TWatchLog>& GetLogs(TObjectTypeValue type) const
    {
        return GetOrCrash(LogsPerObjectType_, type);
    }

    const TRegisteredLog& GetLogByNameOrCrash(
        TObjectTypeValue type,
        const TString& logName) const
    {
        const std::pair<TObjectTypeValue, const TString&> key{type, logName};

        return GetOrCrash(RegisteredLogs_, key);
    }

    const TRegisteredLog& GetLogByNameOrThrow(
        TObjectTypeValue objectType,
        const TString& logName) const
    {
        const std::pair<TObjectTypeValue, const TString&> key{objectType, logName};
        auto it = RegisteredLogs_.find(key);
        if (it == RegisteredLogs_.end()) {
            THROW_ERROR_EXCEPTION(
                "Could not find watch log %Qv of object type %v",
                logName,
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(objectType));
        }
        return it->second;
    }

    bool IsLogRegistered(
        TObjectTypeValue objectType,
        const TString& logName) const
    {
        const std::pair<TObjectTypeValue, const TString&> key{objectType, logName};
        return RegisteredLogs_.contains(key);
    }

    const IWatchLogEventMatcher* GetLogEventMatcher(
        TObjectTypeValue type,
        const TString& logName) const
    {
        const auto* eventMatcher = GetLogByNameOrCrash(type, logName).EventMatcher.get();
        YT_VERIFY(eventMatcher);
        return eventMatcher;
    }

    void UpdateQueryProfiling(
        TObjectTypeValue type,
        const TString& logName,
        TWatchQueryProfilingValues values)
    {
        auto& sensors = GetLogByNameOrCrash(type, logName).Sensors;

        for (const auto& [tablet, skippedRows] : values.PerTabletSkippedRows) {
            auto* skippedRowsSensor = sensors->SkippedRowsCounterPerTablet.FindOrInsert(
                tablet, [tablet = tablet, &sensors]
            {
                return sensors->Profiler.WithTag("tablet", std::to_string(tablet))
                    .Counter("/watch/skipped_rows");
            }).first;
            skippedRowsSensor->Increment(skippedRows);
        }
    }

    TWatchManagerExtendedConfig GetExtendedConfig() const
    {
        return ExtendedConfig_.Load();
    }

private:
    NMaster::IBootstrap* const Bootstrap_;
    const TWatchManagerConfigPtr InitialConfig_;

    const TActionQueuePtr WatchLogManagerQueue_;

    const TPeriodicExecutorPtr WatchLogProfilingExecutor_;
    NProfiling::TCounter ErrorCount_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(WatchLogManagerThread);

    TAtomicObject<TWatchManagerExtendedConfig> ExtendedConfig_;

    THashMap<TObjectTypeValue, std::vector<TWatchLog>> LogsPerObjectType_;
    THashMap<std::pair<TObjectTypeValue, TString>, TRegisteredLog> RegisteredLogs_;

    TWatchManagerChangedAttributesConfigPtr PrepareChangedAttributesConfig(TWatchManagerConfigPtr config)
    {
        TWatchManagerChangedAttributesConfig::TPerLogConfig perLogConfig;

        for (const auto& [objectType, logs] : LogsPerObjectType_) {
            const auto& configChangedAttributePaths = config->GetChangedAttributesPaths(objectType);

            std::set<TString> configPathsSet(
                configChangedAttributePaths.begin(),
                configChangedAttributePaths.end());
            auto universalChangedPathsSet = configPathsSet;
            for (const auto& log : logs) {
                if (!config->IsLogStoreEnabled(log.ObjectName, log.Name)) {
                    continue;
                }
                universalChangedPathsSet.insert(log.Selector.begin(), log.Selector.end());
            }
            for (const auto& log : logs) {
                if (!config->IsLogStoreEnabled(log.ObjectName, log.Name)) {
                    continue;
                }
                std::set<TString> changedAttributePaths(log.Selector.begin(), log.Selector.end());
                // NB! Make a copy.
                changedAttributePaths.merge(changedAttributePaths.empty()
                    ? std::set<TString>(universalChangedPathsSet)
                    : std::set<TString>(configPathsSet));
                THashMap<TString, size_t> pathToIndex;
                pathToIndex.reserve(changedAttributePaths.size());
                for (const auto& [index, attribute]: Enumerate(changedAttributePaths)) {
                    InsertOrCrash(pathToIndex, std::pair(attribute, index));
                }
                YT_LOG_DEBUG("Updating watch log changed attributes ("
                    "ObjectType: %v, "
                    "LogName: %v, "
                    "ChangedAttributePaths: %v)",
                    NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(objectType),
                    log.Name,
                    changedAttributePaths);
                perLogConfig[objectType][log.Name] = New<TWatchLogChangedAttributesConfig>(
                    std::move(pathToIndex),
                    CalculateConfigurationMD5Hash(changedAttributePaths));
            }
        }

        return New<TWatchManagerChangedAttributesConfig>(perLogConfig);
    }

    void UpdateWatchLogRefreshPeriods(TWatchManagerConfigPtr config)
    {
        for (const auto& [key, registeredLog] : RegisteredLogs_) {
            const auto& [objectType, logName] = key;

            registeredLog.LogRefreshExecutor->SetPeriod(
                config->GetLogRefreshPeriod(objectType, logName));
        }
    }

    void OnConfigUpdate(const NMaster::TMasterDynamicConfigPtr& masterConfig)
    {
        if (NMaster::AreConfigsEqual(GetConfig(), masterConfig->WatchManager)) {
            return;
        }
        YT_LOG_INFO("Updating watch manager configuration");

        auto config = masterConfig->WatchManager;
        SetConfig(std::move(config));
    }

    void SetConfig(TWatchManagerConfigPtr config)
    {
        WatchLogProfilingExecutor_->SetPeriod(config->ProfilingPeriod);

        UpdateWatchLogRefreshPeriods(config);

        auto changedAttributesConfig = PrepareChangedAttributesConfig(config);
        ExtendedConfig_.Store(TWatchManagerExtendedConfig(config, changedAttributesConfig));
    }

    void OnValidateConnection()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        const auto& ytConnector = Bootstrap_->GetYTConnector();

        auto tableVersionValidator = CreateTableValidator(
            ytConnector,
            /*enableTableSchemaValidation*/ false,
            Bootstrap_->GetDBConfig().VersionCompatibility);
        for (const auto& [key, registeredLog] : RegisteredLogs_) {
            YT_VERIFY(registeredLog.Table.get());
            tableVersionValidator->Schedule({.Table = registeredLog.Table.get()});
        }
        tableVersionValidator->Validate();
    }

    auto GetSnapshotWatchLogConsumers(ISession* session)
    {
        THashMap<std::pair<TObjectTypeValue, TString>, std::vector<TWatchLogConsumerDetail>> PerLogConsumers;

        YT_LOG_DEBUG("Loading watch log consumer snapshot");

        session->ScheduleLoad([&] (ILoadContext* context) {
            const auto& ytConnector = Bootstrap_->GetYTConnector();
            const auto& interop = Bootstrap_->GetWatchLogConsumerInterop();

            context->ScheduleSelect(
                interop->GetQueryString(ytConnector),
                [&] (const IUnversionedRowsetPtr& rowset) {
                    YT_LOG_DEBUG("Parsing watch log consumers (Count: %v)",
                        rowset->GetRows().Size());
                    for (auto row : rowset->GetRows()) {
                        auto consumer = interop->Parse(row);
                        if (!consumer.ObjectType) {
                            YT_LOG_DEBUG(
                                "Skipping watch log consumer profiling because it "
                                "does not contain object type (ConsumerId: %v)",
                                consumer.Id);
                            continue;
                        }
                        if (!consumer.ContinuationToken) {
                            YT_LOG_DEBUG(
                                "Skipping watch log consumer profiling because it "
                                "does not contain continuation token (ConsumerId: %v)",
                                consumer.Id);
                            continue;
                        }
                        auto objectTypeValue = NClient::NObjects::GetGlobalObjectTypeRegistry()
                            ->GetTypeValueByNameOrThrow(consumer.ObjectType);

                        NProto::TWatchQueryContinuationToken continuationToken;
                        try {
                            DeserializeContinuationToken(*consumer.ContinuationToken, &continuationToken);
                        } catch (const std::exception& ex) {
                            YT_LOG_DEBUG(ex,
                                "Error parsing watch log consumer continuation token (ConsumerId: %v)",
                                consumer.Id);
                            continue;
                        }
                        if (!continuationToken.log_name()) {
                            YT_LOG_DEBUG("Skipping watch log consumer profiling because its "
                                "continuation token does not contain log name (ConsumerId: %v)",
                                consumer.Id);
                            continue;
                        }
                        const std::pair<TObjectTypeValue, const TString&> key = {
                            objectTypeValue,
                            continuationToken.log_name()
                        };
                        if (!IsLogRegistered(objectTypeValue, continuationToken.log_name())) {
                            YT_LOG_DEBUG("Skipping watch log consumer profiling because its "
                                "continuation token contains nonexistent log name (ConsumerId: %v, LogName: %v)",
                                consumer.Id,
                                continuationToken.log_name());
                            continue;
                        }
                        PerLogConsumers[key].push_back(TWatchLogConsumerDetail{
                            std::move(consumer),
                            std::move(continuationToken)
                        });
                    }
            });
        });

        YT_LOG_DEBUG("Querying watch log consumers");
        session->FlushLoads();

        return PerLogConsumers;
    }

    void ProfileWatchLogConsumers()
    {
        try {
            YT_LOG_DEBUG("Profiling watch log consumers");

            auto transactionManager = Bootstrap_->GetTransactionManager();
            auto transaction = WaitFor(transactionManager->StartReadOnlyTransaction({
                .UserTag = WatchLogConsumerProfilerUserTag,
                .ReadingTransactionOptions = {
                    .AllowFullScan = true,
                }
            })).ValueOrThrow();

            auto* session = transaction->GetSession();

            auto perLogConsumers = GetSnapshotWatchLogConsumers(session);

            std::vector<TFuture<void>> perLogFutures;
            perLogFutures.reserve(perLogConsumers.size());

            for (auto& logInfoAndConsumers : perLogConsumers) {
                const auto& objectType = logInfoAndConsumers.first.first;
                const auto& logName = logInfoAndConsumers.first.second;
                auto& consumers = logInfoAndConsumers.second;

                const auto& registeredLog = GetLogByNameOrThrow(objectType, logName);

                perLogFutures.push_back(GetTabletInfos(objectType, logName, {})
                    .Apply(BIND([consumers = std::move(consumers), logSensors = registeredLog.Sensors, objectType] (
                        const std::vector<TTabletInfo>& tabletInfos)
                    {
                        for (auto& [consumer, deserializedContinuationToken] : consumers) {
                            auto it = logSensors->WatchLogConsumerSensors.find(consumer.Id);
                            if (it == logSensors->WatchLogConsumerSensors.end()) {
                                TWatchLogConsumerSensors sensors(logSensors->Profiler, consumer.Id);
                                it = logSensors->WatchLogConsumerSensors.emplace(
                                    std::move(consumer.Id),
                                    std::move(sensors)).first;
                            }

                            try {
                                it->second.Update(deserializedContinuationToken, tabletInfos);
                            } catch (const std::exception& ex) {
                                // Consumer id has been already moved out of #consumer,
                                // but must be within the #ex.
                                YT_LOG_WARNING(ex,
                                    "Error creating watch log consumer sensors (ObjectType: %v)",
                                    NClient::NObjects::GetGlobalObjectTypeRegistry()
                                        ->GetTypeNameByValueOrCrash(objectType));
                                continue;
                            }
                        }
                    })));
            }

            WaitForUnique(AllSucceeded(std::move(perLogFutures)))
                .ThrowOnError();

            YT_LOG_DEBUG("Finished profiling watch log consumers");
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error profiling watch log consumers");
            ErrorCount_.Increment();
        }
    }

    void OnWatchLogProfile()
    {
        VERIFY_THREAD_AFFINITY(WatchLogManagerThread);

        if (!GetConfig()->ProfilingEnabled || !Bootstrap_->GetYTConnector()->IsLeading()) {
            return;
        }
        ProfileWatchLogConsumers();
    }

    void OnRefreshWatchLog(TObjectTypeValue objectType, const TString& logName)
    {
        VERIFY_THREAD_AFFINITY(WatchLogManagerThread);

        try {
            YT_LOG_DEBUG("Refreshing watch log (LogName: %v, ObjectType: %v)",
                logName,
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(objectType));
            auto ytConnector = Bootstrap_->GetYTConnector();
            auto logTable = GetLogTableOrCrash(objectType, logName);
            const auto tablePath = ytConnector->GetTablePath(logTable);

            auto tableMountInfo = WaitFor(GetTableMountInfo(ytConnector, logTable))
                .ValueOrThrow();

            auto transaction = WaitFor(Bootstrap_->GetTransactionManager()
                ->StartReadWriteTransaction())
                .ValueOrThrow();
            auto storeContext = transaction->CreateStoreContext();

            NProto::TWatchRecord record;
            record.set_dummy(true);
            for (size_t i = 0; i < tableMountInfo->Tablets.size(); ++i) {
                storeContext->WriteRow(
                    logTable,
                    TObjectKey(static_cast<i64>(i)),
                    std::array{&WatchLogSchema.Fields.Record},
                    ToUnversionedValues(storeContext->GetRowBuffer(), record));
            }

            storeContext->FillTransaction();

            WaitFor(transaction->Commit())
                .ValueOrThrow();

            YT_LOG_DEBUG("Refreshed watch log (LogName: %v, ObjectType: %v)",
                logName,
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(objectType));
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error refreshing watch log (LogName: %v, ObjectType: %v)",
                logName,
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(objectType));
        }
    }

    void OnStartedLeading()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        WatchLogProfilingExecutor_->Start();

        for (const auto& [_, registeredLog] : RegisteredLogs_) {
            registeredLog.LogRefreshExecutor->Start();
        }
    }

    void OnStoppedLeading()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);
        YT_UNUSED_FUTURE(WatchLogProfilingExecutor_->Stop());

        for (const auto& [_, registeredLog] : RegisteredLogs_) {
            YT_UNUSED_FUTURE(registeredLog.LogRefreshExecutor->Stop());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TWatchManager::TWatchManager(
    NMaster::IBootstrap* bootstrap,
    TWatchManagerConfigPtr config)
    : Impl_(New<TImpl>(bootstrap, std::move(config)))
{ }

TWatchManager::~TWatchManager()
{ }

void TWatchManager::Initialize()
{
    Impl_->Initialize();
}

TWatchManagerConfigPtr TWatchManager::GetConfig() const
{
    return Impl_->GetConfig();
}

TFuture<std::vector<TTabletInfo>> TWatchManager::GetTabletInfos(
    TObjectTypeValue objectType,
    const TString& logName,
    const std::vector<int>& tablets)
{
    return Impl_->GetTabletInfos(objectType, logName, tablets);
}

TFuture<int> TWatchManager::GetTabletCount(TObjectTypeValue objectType, const TString& logName)
{
    return Impl_->GetTabletCount(objectType, logName);
}

TErrorOr<std::vector<TTabletInfo>> TWatchManager::WaitForBarrier(
    TObjectTypeValue objectType,
    const TString& logName,
    const std::vector<int>& tablets,
    TTimestamp barrierTimestamp,
    TDuration timeLimit)
{
    return Impl_->WaitForBarrier(objectType, logName, tablets, barrierTimestamp, timeLimit);
}

void TWatchManager::RegisterLogs(const IObjectTypeHandler* typeHandler)
{
    Impl_->RegisterLogs(typeHandler);
}

const TDBTable* TWatchManager::GetLogTableOrCrash(
    TObjectTypeValue type,
    const TString& logName) const
{
    return Impl_->GetLogTableOrCrash(type, logName);
}

const TDBTable* TWatchManager::GetLogTableOrThrow(
    TObjectTypeValue type,
    const TString& logName) const
{
    return Impl_->GetLogTableOrThrow(type, logName);
}

const std::vector<TWatchLog>& TWatchManager::GetLogs(
    TObjectTypeValue type) const
{
    return Impl_->GetLogs(type);
}

const TWatchLog& TWatchManager::GetLogByNameOrCrash(
    TObjectTypeValue type,
    const TString& logName) const
{
    return Impl_->GetLogByNameOrCrash(type, logName);
}

const TWatchLog& TWatchManager::GetLogByNameOrThrow(
    TObjectTypeValue type,
    const TString& logName) const
{
    return Impl_->GetLogByNameOrThrow(type, logName);
}

bool TWatchManager::IsLogRegistered(
    TObjectTypeValue objectType,
    const TString& logName) const
{
    return Impl_->IsLogRegistered(objectType, logName);
}

const IWatchLogEventMatcher* TWatchManager::GetLogEventMatcher(
    TObjectTypeValue type,
    const TString& logName) const
{
    return Impl_->GetLogEventMatcher(type, logName);
}

void TWatchManager::UpdateQueryProfiling(
    TObjectTypeValue objectType,
    const TString& logName,
    TWatchQueryProfilingValues values) const
{
    return Impl_->UpdateQueryProfiling(objectType, logName, std::move(values));
}

TWatchManagerExtendedConfig TWatchManager::GetExtendedConfig() const
{
    return Impl_->GetExtendedConfig();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
