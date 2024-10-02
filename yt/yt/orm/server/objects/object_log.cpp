#include "object_log.h"

#include "attribute_schema.h"
#include "config.h"
#include "db_config.h"
#include "db_schema.h"
#include "helpers.h"
#include "history_manager.h"
#include "object_manager.h"
#include "object_reflection.h"
#include "persistence.h"
#include "private.h"
#include "tags.h"
#include "transaction.h"
#include "watch_log.h"
#include "watch_log_event_matcher.h"
#include "watch_manager.h"

#include <yt/yt/orm/server/master/bootstrap.h>
#include <yt/yt/orm/server/master/config.h>

#include <yt/yt/orm/server/objects/proto/history_event_etc.pb.h>
#include <yt/yt/orm/server/objects/proto/watch_record.pb.h>

#include <yt/yt/orm/server/access_control/access_control_manager.h>

#include <yt/yt/orm/library/query/filter_matcher.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <library/cpp/protobuf/interop/cast.h>

#include <google/protobuf/timestamp.pb.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;
using namespace NClient::NObjects;

using NYT::ToProto;
using NYT::NOrm::NServer::NObjects::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TObjectLogContextCompositor
    : public IObjectLogContext
{
public:
    void AddChild(IObjectLogContextPtr&& context)
    {
        Children_.push_back(std::move(context));
    }

    void Preload() override
    {
        for (auto& child : Children_) {
            child->Preload();
        }
    }

    void Write() override
    {
        for (auto& child : Children_) {
            child->Write();
        }
    }
private:
    std::vector<IObjectLogContextPtr> Children_;
};

////////////////////////////////////////////////////////////////////////////////

template <TEventTypeValue Type>
std::vector<TObject*> GetObjects(ISessionChangeTracker* tracker, IObjectFilter* filter)
{
    if constexpr (Type == ObjectCreatedEventTypeValue) {
        return tracker->GetCreatedObjects(filter);
    } else if constexpr (Type == ObjectUpdatedEventTypeValue) {
        return tracker->GetUpdatedObjects();
    } else if constexpr (Type == ObjectRemovedEventTypeValue) {
        return tracker->GetRemovedObjects(filter);
    }
}

////////////////////////////////////////////////////////////////////////////////

class THistoryFilter
    : public IObjectFilter
{
public:
    THistoryFilter(NMaster::IBootstrap* bootstrap, THashMap<TObjectTypeValue, bool>&& perTypeSkipEventPermission)
        : ObjectManager_(bootstrap->GetObjectManager())
        , PerTypeSkipEventPermission_(perTypeSkipEventPermission)
    { }

    bool SkipByType(IObjectTypeHandler* typeHandler, TObjectTypeValue typeValue, int count) override
    {
        YT_VERIFY(typeHandler);
        auto historyDisabledForType = ObjectManager_->IsHistoryDisabledForType(typeValue);
        auto skipByTypePermissions = GetOrCrash(PerTypeSkipEventPermission_, typeValue);
        auto skipByTypeHandler = !typeHandler->HasHistoryEnabledAttributes();

        if (historyDisabledForType || skipByTypePermissions || skipByTypeHandler) {
            SkippedObjects_ += count;
            return true;
        }

        return false;
    }

    int GetFilteredByType() const
    {
        return SkippedObjects_;
    }

protected:
    TObjectManagerPtr ObjectManager_;
    THashMap<TObjectTypeValue, bool> PerTypeSkipEventPermission_;
    int SkippedObjects_ = 0;
};

struct THistoryWriteCounters
{
    // Common counters.
    i64 WrittenEventCount = 0;
    i64 FilteredByHistoryFilter = 0;

    // Counters meaningful for updates only.
    i64 FilteredByObjectState = 0;
    i64 FilteredByHistory = 0;
    i64 FilteredBeforeLoad = 0;
};

class THistoryWriteContext
    : public TObjectLogContextCompositor
{
public:
    THistoryWriteContext(NLogging::TLogger logger, ISessionChangeTracker* tracker, const TMutatingTransactionOptions& options, TTransaction* transaction)
        : Logger(logger)
        , Filter_(transaction->GetBootstrap(), tracker->BuildTypeToSkipEventPermissionMap(options.SkipHistory))
        , StoreContext_(transaction->CreateStoreContext())
        , Bootstrap_(transaction->GetBootstrap())
        , Transaction_(transaction)
        , TransactionId_(transaction->GetId())
        , HistoryManager_(Bootstrap_->MakeHistoryManager(transaction))
        , TransactionContext_(options.TransactionContext)
        , ObjectManager_(Bootstrap_->GetObjectManager())
    {
        auto* tracingContext = NTracing::TryGetCurrentTraceContext();
        if (tracingContext) {
            TransactionContext_.Items.insert({"trace_id", ToString(tracingContext->GetTraceId())});
        }
    }

    THistoryFilter& GetFilter()
    {
        return Filter_;
    }

    void Write() override
    {
        TObjectLogContextCompositor::Write();
        StoreContext_->FillTransaction();

        YT_LOG_DEBUG("History events written "
            "(Count: %v, FilteredByObjectState: %v, FilteredByHistoryFilter: %v, "
            "FilteredByHistory: %v, FilteredBeforeLoad: %v, FilteredByType: %v)",
            Counters_.WrittenEventCount,
            Counters_.FilteredByObjectState,
            Counters_.FilteredByHistoryFilter,
            Counters_.FilteredByHistory,
            Counters_.FilteredBeforeLoad,
            Filter_.GetFilteredByType());
    }

    void WriteHistoryEvent(TObject* object, TEventTypeValue eventType)
    {
        auto* primaryTable = HistoryManager_->GetPrimaryWriteTable();
        WriteHistoryEvent(object, eventType, primaryTable);

        if (auto* secondaryTable = HistoryManager_->GetSecondaryWriteTable()) {
            YT_VERIFY(secondaryTable->TimeMode == EHistoryTimeMode::Logical);
            std::optional<THistoryTime> historyTimeOverride;
            if (HistoryManager_->IsBackgroundMigrationAllowed() &&
                primaryTable->TimeMode == EHistoryTimeMode::Physical)
            {
                auto primaryTime = Transaction_->GetHistoryEventTime(primaryTable);
                if (auto* time = std::get_if<TInstant>(&primaryTime)) {
                    historyTimeOverride = ConvertPhysicalToLogicalTime(*time);
                }
            }

            WriteHistoryEvent(object, eventType, secondaryTable, historyTimeOverride);
        }
    }

    void WriteHistoryEvent(
        TObject* object,
        TEventTypeValue eventType,
        const THistoryIndexTable* historyIndexTable,
        std::optional<THistoryTime> historyTimeOverride = std::nullopt)
    {
        auto* typeHandler = object->GetTypeHandler();
        auto accessControlManager = Bootstrap_->GetAccessControlManager();
        auto identity = NAccessControl::TryGetAuthenticatedUserIdentity().value_or(
            NRpc::GetRootAuthenticationIdentity());

        NAttributes::TYsonStringBuilder ysonStringBuilder;
        std::unique_ptr<NAttributes::IYsonBuilder> optionalBuilderHolder;
        std::unique_ptr<IYsonConsumer> optionalConsumerHolder;
        if (Transaction_->GetConfig()->ForbidYsonAttributesUsage) {
            std::vector<std::unique_ptr<NYson::IYsonConsumer>> ownedConsumers;
            ownedConsumers.push_back(
                NAttributes::CreateAttributesDetectingConsumer([]
                {
                    THROW_ERROR_EXCEPTION(
                        "Encountered forbidden YSON-attributes on "
                        "history enabled attribute while flushing transaction");
                }));
            optionalConsumerHolder = std::make_unique<TTeeYsonConsumer>(
                /*consumers*/ std::vector<IYsonConsumer*>{ysonStringBuilder.GetConsumer()},
                /*ownedConsumers*/ std::move(ownedConsumers));
            optionalBuilderHolder = std::make_unique<NAttributes::TYsonBuilder>(
                NAttributes::EYsonBuilderForwardingPolicy::Forward,
                &ysonStringBuilder,
                optionalConsumerHolder.get());
        }

        if (eventType != ObjectRemovedEventTypeValue) {
            auto* builder = optionalBuilderHolder ? optionalBuilderHolder.get() : &ysonStringBuilder;
            typeHandler->GetRootAttributeSchema()
                ->GetHistoryEnabledAttributes(Transaction_, object, builder);
        } else {
            ysonStringBuilder->OnBeginMap();
            ysonStringBuilder->OnEndMap();
        }

        auto historyEnabledAttributes = ysonStringBuilder.Flush();
        auto filter = typeHandler->GetHistoryFilter();
        if (filter && !filter->Match(historyEnabledAttributes)
            .ValueOrThrow())
        {
            ++Counters_.FilteredByHistoryFilter;
            return;
        }

        ++Counters_.WrittenEventCount;

        NProto::THistoryEventEtc etc;
        if (!TransactionContext_.Items.empty()) {
            ToProto(
                etc.mutable_transaction_context(),
                TransactionContext_);
        }
        if (ObjectManager_->AreHistoryEnabledAttributePathsEnabled()) {
            const auto& historyEnabledAttributePaths =
                object->GetTypeHandler()->GetHistoryEnabledAttributePaths();
            NYT::ToProto(
                etc.mutable_history_enabled_attributes(),
                historyEnabledAttributePaths);
        }
        etc.set_user_tag(ToProto<TProtobufString>(identity.UserTag));

        auto rawTime = historyTimeOverride ?
            GetRawHistoryTime(*historyTimeOverride) :
            GetRawHistoryTime(Transaction_->GetHistoryEventTime(historyIndexTable));
        if (!historyIndexTable->OptimizeForAscendingTime) {
            rawTime = ~rawTime;
        }

        auto rawEventType = historyIndexTable->UsePositiveEventTypes ? eventType : -eventType;
        auto writeHistoryEventRow = [&] {
            const THistoryEventsTable* historyEventsTable = historyIndexTable->PrimaryTable;
            if (historyEventsTable->UseUuidInKey) {
                StoreContext_->WriteRow(
                    historyEventsTable,
                    TObjectKey(TObjectKey::TKeyFields{
                        object->GetType(),
                        object->GetKey().ToString(),
                        object->GetUuid(),
                        rawTime,
                        ToString(TransactionId_),
                    }),
                    std::array{
                        &historyEventsTable->Fields.EventType,
                        &historyEventsTable->Fields.User,
                        &historyEventsTable->Fields.Value,
                        &historyEventsTable->Fields.Etc,
                    },
                    ToUnversionedValues(
                        StoreContext_->GetRowBuffer(),
                        rawEventType,
                        identity.User,
                        historyEnabledAttributes,
                        etc));
            } else {
                StoreContext_->WriteRow(
                    historyEventsTable,
                    TObjectKey(TObjectKey::TKeyFields{
                        object->GetType(),
                        object->GetKey().ToString(),
                        rawTime,
                        ToString(TransactionId_),
                        rawEventType,
                    }),
                    std::array{
                        &historyEventsTable->Fields.Uuid,
                        &historyEventsTable->Fields.User,
                        &historyEventsTable->Fields.Value,
                        &historyEventsTable->Fields.Etc,
                    },
                    ToUnversionedValues(
                        StoreContext_->GetRowBuffer(),
                        object->GetUuid(),
                        identity.User,
                        historyEnabledAttributes,
                        etc));
            }
        };

        auto writeIndexRow = [&] (const TString& indexingEventType) {
            if (historyIndexTable->UseUuidInKey) {
                StoreContext_->WriteRow(
                    historyIndexTable,
                    TObjectKey(TObjectKey::TKeyFields{
                        object->GetType(),
                        rawEventType,
                        indexingEventType,
                        object->GetKey().ToString(),
                        object->GetUuid(),
                        rawTime,
                        ToString(TransactionId_),
                    }),
                    std::array{
                        &historyIndexTable->Fields.User,
                        &historyIndexTable->Fields.Value,
                        &historyIndexTable->Fields.Etc,
                    },
                    ToUnversionedValues(
                        StoreContext_->GetRowBuffer(),
                        identity.User,
                        historyEnabledAttributes,
                        etc));
            } else {
                StoreContext_->WriteRow(
                    historyIndexTable,
                    TObjectKey(TObjectKey::TKeyFields{
                        object->GetType(),
                        rawEventType,
                        indexingEventType,
                        object->GetKey().ToString(),
                        rawTime,
                        ToString(TransactionId_),
                    }),
                    std::array{
                        &historyIndexTable->Fields.Uuid,
                        &historyIndexTable->Fields.User,
                        &historyIndexTable->Fields.Value,
                        &historyIndexTable->Fields.Etc,
                    },
                    ToUnversionedValues(
                        StoreContext_->GetRowBuffer(),
                        object->GetUuid(),
                        identity.User,
                        historyEnabledAttributes,
                        etc));
            }
        };

        writeHistoryEventRow();

        const auto& historyIndexedAttributes = typeHandler->GetHistoryIndexedAttributes();
        bool canStoreToHistoryIndex = std::any_of(historyIndexedAttributes.begin(), historyIndexedAttributes.end(),
            [&, objectType = object->GetType()] (const TString& attribute) {
                return ObjectManager_->IsHistoryIndexAttributeStoreEnabled(objectType, attribute);
            });
        if (!canStoreToHistoryIndex) {
            return;
        }

        if (eventType != ObjectUpdatedEventTypeValue) {
            writeIndexRow(OtherHistoryIndexEventType);
        } else {
            for (const auto& attribute : historyIndexedAttributes) {
                if (ObjectManager_->IsHistoryIndexAttributeStoreEnabled(
                    object->GetType(),
                    attribute))
                {
                    auto resolveResult = ResolveAttribute(
                        object->GetTypeHandler(),
                        attribute,
                        /*callback*/ {},
                        /*validateProtoSchemaCompliance*/ false);
                    if (resolveResult.Attribute->ShouldStoreHistoryIndexedAttribute(
                        object,
                        resolveResult.SuffixPath))
                    {
                        writeIndexRow(attribute);
                    }
                }
            }
        }
    }

    const THistoryFilter& GetFilter() const
    {
        return Filter_;
    }

    THistoryWriteCounters& GetCounters()
    {
        return Counters_;
    }

private:
    NLogging::TLogger Logger;
    THistoryFilter Filter_;
    std::unique_ptr<IStoreContext> StoreContext_;
    NMaster::IBootstrap* Bootstrap_;
    TTransaction* Transaction_;
    TTransactionId TransactionId_;
    IHistoryManagerPtr HistoryManager_;
    TTransactionContext TransactionContext_;
    TObjectManagerPtr ObjectManager_;
    THistoryWriteCounters Counters_;
};

template <TEventTypeValue EventType>
class THistoryWriterBase
    : public IObjectLogContext
{
public:
    THistoryWriterBase(ISessionChangeTracker* tracker, THistoryWriteContext* writeContext)
        : Objects_(GetObjects<EventType>(tracker, &writeContext->GetFilter()))
        , Context_(writeContext)
    { }

    void Write() override
    {
        for (auto* object : Objects_) {
            Context_->WriteHistoryEvent(object, EventType);
        }
    }

    void Preload() override
    { }

protected:
    std::vector<TObject *> Objects_;
    THistoryWriteContext* Context_;
};

using THistoryCreatedWriter = THistoryWriterBase<ObjectCreatedEventTypeValue>;
using THistoryRemovedWriter = THistoryWriterBase<ObjectRemovedEventTypeValue>;

class THistoryUpdatedWriter
    : public THistoryWriterBase<ObjectUpdatedEventTypeValue>
{
public:
    using THistoryWriterBase::THistoryWriterBase;

    void Preload() override
    {
        std::vector<TObject *> result;
        for (auto* object : Objects_) {
            auto* typeHandler = object->GetTypeHandler();

            if (!object->IsStoreScheduled()) {
                ++Context_->GetCounters().FilteredByObjectState;
                continue;
            }

            if (Context_->GetFilter().SkipByType(typeHandler, typeHandler->GetType(), /*count*/ 1)) {
                continue;
            }

            if (!typeHandler->HasStoreScheduledHistoryAttributes(object)) {
                ++Context_->GetCounters().FilteredBeforeLoad;
                continue;
            }

            typeHandler->PreloadHistoryEnabledAttributes(object);
            object->ScheduleMetaResponseLoad();
            result.push_back(object);
        }

        Objects_ = std::move(result);
    }

    void Write() override
    {
        for (auto* object : Objects_) {
            if (object->GetTypeHandler()->HasHistoryEnabledAttributeForStore(object)) {
                Context_->WriteHistoryEvent(object, ObjectUpdatedEventTypeValue);
            } else {
                ++Context_->GetCounters().FilteredByHistory;
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TWatchLogWriteCounters
{
    int FilteredByType = 0;
    int FilteredByLogStoreConfig = 0;
    int FilteredByEventLogMatcher = 0;
};

class TWatchLogWriteContext
    : public TObjectLogContextCompositor
    , public IObjectFilter
{
public:
    explicit TWatchLogWriteContext(
        ISessionChangeTracker* tracker,
        const TMutatingTransactionOptions& options,
        TTransaction* owner,
        const NLogging::TLogger& logger)
        : Bootstrap_(owner->GetBootstrap())
        , Owner_(owner)
        , HistoryTime_(Bootstrap_->MakeHistoryManager(owner)->GetWatchlogHistoryTime())
        , WatchManager_(Bootstrap_->GetWatchManager())
        , Config_(WatchManager_->GetExtendedConfig())
        , Logger(logger)
        , Options_(options)
        , PerTypeSkipEventPermissions_(tracker->BuildTypeToSkipEventPermissionMap(options.SkipWatchLog))
    { }

    // Returns a copy of the watch manager dynamic config,
    // which was created on watch record writer construction.
    const TWatchManagerConfigPtr& GetConfig() const
    {
        return Config_.WatchManager;
    }

    bool SkipByType(IObjectTypeHandler* /*typeHandler*/, TObjectTypeValue type, int count) override
    {
        if (!GetWatchLogs(type).empty() && !GetOrCrash(PerTypeSkipEventPermissions_, type)) {
            return false;
        }

        Counters_.FilteredByType += count;
        return true;
    }

    void PreloadUpdatedObject(TObject* object) const
    {
        THashSet<TString> resolvedPaths;
        for (const auto& log : GetWatchLogs(object->GetType())) {
            if (!GetConfig()->IsLogStoreEnabled(log.ObjectName, log.Name)) {
                continue;
            }

            if (auto config = Config_.ChangedAttributes
                ->TryGetLogChangedAttributesConfig(object->GetType(), log.Name))
            {
                auto* typeHandler = object->GetTypeHandler();
                for (const auto& [path, index] : config->PathToIndex) {
                    if (!resolvedPaths.emplace(path).second) {
                        continue;
                    }
                    auto result = ResolveAttribute(
                        typeHandler,
                        path,
                        /*callback*/ {},
                        /*validateProtoSchemaCompliance*/ false);
                    result.Attribute->ForEachLeafAttribute([object] (const TScalarAttributeSchema* schema) {
                        if (schema->HasStoreScheduledGetter() &&
                            schema->RunStoreScheduledGetter(object) &&
                            schema->HasPreloader() &&
                            schema->HasChangedGetter() &&
                            schema->GetChangedGetterType() != EChangedGetterType::ConstantFalse &&
                            schema->GetChangedGetterType() != EChangedGetterType::ConstantTrue)
                        {
                            schema->RunPreloader(object);
                        }
                        return false;
                    });
                }
            }
        }
    }

    bool IsLabelsLoadRequired(const TObjectTypeValue objectType) const
    {
        const auto& logs = GetWatchLogs(objectType);
        return GetConfig()->IsLabelsStoreEnabled(objectType) ||
            std::any_of(logs.begin(), logs.end(),
                [&] (const auto& log) {
                    return GetConfig()->IsLogStoreEnabled(log.ObjectName, log.Name) && log.Filter.Query;
                });
    }

    template <TEventTypeValue EventType>
    void OnEvent(TObject* object)
    {
        try {
            GuardedOnEvent<EventType>(object);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error preparing watch event for %v",
                object->GetDisplayName())
                << ex;
        }
    }

    bool Enabled() const
    {
        return Config_.WatchManager->Enabled;
    }

    void Write() override
    {
        if (!Enabled()) {
            return;
        }

        TObjectLogContextCompositor::Write();

        NProto::TWatchRecord_TTransactionContext protoTransactionContext;
        if (!Options_.TransactionContext.Items.empty()) {
            ToProto(
                &protoTransactionContext,
                Options_.TransactionContext);
        }

        auto context = Owner_->CreateStoreContext();

        for (const auto& [key, events] : EventsPerObject_) {
            const auto& [objectType, objectKey] = key;
            bool isAnyLogStoreEnabled = false;
            bool doesEventMatchAnyLog = false;
            for (const auto& log : GetWatchLogs(objectType)) {
                if (!GetConfig()->IsLogStoreEnabled(log.ObjectName, log.Name)) {
                    continue;
                }
                isAnyLogStoreEnabled = true;

                const auto* matcher = WatchManager_->GetLogEventMatcher(objectType, log.Name);
                auto changedAttributesConfig = Config_.ChangedAttributes
                    ->TryGetLogChangedAttributesConfig(objectType, log.Name);

                NProto::TWatchRecord record;
                for (const auto& event : events) {
                    NProto::TWatchRecord_TEvent protoEvent;
                    InitializeProtoEvent(objectKey, event, log.Name, &protoEvent);
                    if (matcher->Match(protoEvent, changedAttributesConfig, context->GetRowBuffer())
                        .ValueOrThrow())
                    {
                        *record.add_events() = std::move(protoEvent);
                    }
                }
                if (record.events().empty()) {
                    continue;
                }

                record.mutable_transaction_context()->CopyFrom(protoTransactionContext);
                Visit(HistoryTime_,
                    [&] (TInstant time) {
                        *record.mutable_history_time() = NProtoInterop::CastToProto(time);
                    },
                    [&] (NTableClient::TTimestamp timestamp) {
                        record.set_history_timestamp(timestamp);
                    },
                    [] (std::monostate) {
                        YT_ABORT();
                    });

                doesEventMatchAnyLog = true;
                context->WriteRow(
                    WatchManager_->GetLogTableOrCrash(objectType, log.Name),
                    TObjectKey(GetTabletIndex(objectType, objectKey, log.Name)),
                    std::array{&WatchLogSchema.Fields.Record},
                    ToUnversionedValues(
                        context->GetRowBuffer(),
                        std::move(record)));
            }

            if (isAnyLogStoreEnabled) {
                if (!doesEventMatchAnyLog) {
                    ++Counters_.FilteredByEventLogMatcher;
                }
            } else {
                ++Counters_.FilteredByLogStoreConfig;
            }
        }

        context->FillTransaction();

        YT_LOG_DEBUG_UNLESS(EventsPerObject_.empty(),
            "Watch records flushed (Count: %v, FilteredByType: %v, "
            "FilteredByLogStoreConfig: %v, FilteredByEventLogMatcher: %v)",
            EventsPerObject_.size(),
            Counters_.FilteredByType,
            Counters_.FilteredByLogStoreConfig,
            Counters_.FilteredByEventLogMatcher);

        EventsPerObject_.clear();
    }

    const std::vector<TWatchLog>& GetWatchLogs(TObjectTypeValue type) const
    {
        return WatchManager_->GetLogs(type);
    }

private:
    struct TChangedAttributes
    {
        TString ChangedSummary;
        TString PathsMD5;
    };

    struct TEvent
    {
        TEventTypeName TypeName;
        TString Meta;
        TString Labels;
        THashMap<TString, TChangedAttributes> PerLogChangedAttributes;
        std::optional<std::vector<const TTagSet*>> ChangedTags;
    };

    struct TLogInfo
    {
        NTabletClient::TTableMountInfoPtr TableMountInfo;

        // If random mode enabled, send transaction modifications to the same tablet for better write performance.
        i64 RandomTabletIndex;
    };

    NMaster::IBootstrap* const Bootstrap_;
    TTransaction* const Owner_;
    const THistoryTime HistoryTime_;
    const TWatchManagerPtr WatchManager_;
    const TWatchManagerExtendedConfig Config_;
    NLogging::TLogger Logger;
    TMutatingTransactionOptions Options_;
    THashMap<TObjectTypeValue, bool> PerTypeSkipEventPermissions_;
    TWatchLogWriteCounters Counters_;

    THashMap<std::pair<TObjectTypeValue, TObjectKey>, std::vector<TEvent>> EventsPerObject_;
    THashMap<std::pair<TObjectTypeValue, TString>, TLogInfo> PerLogInfo_;

    template <TEventTypeValue EventType>
    void GuardedOnEvent(TObject* object)
    {
        const auto type = object->GetType();
        const auto key = object->GetKey();

        auto meta = GetObjectIdentityMeta(Owner_, object).ToString();

        // One pair (type, key) can relate to different object instantiations with
        // different labels, so we always generate filtered labels from scratch.
        TString labels;
        if (IsLabelsLoadRequired(type)) {
            if (auto labelsMap = object->Labels().Load()) {
                labels = ConvertToYsonString(labelsMap).ToString();
            }
        }

        THashMap<TString, TChangedAttributes> perLogChangedAttributes;
        for (const auto& log : GetWatchLogs(type)) {
            if (!GetConfig()->IsLogStoreEnabled(log.ObjectName, log.Name)) {
                continue;
            }
            auto config = Config_.ChangedAttributes
                ->TryGetLogChangedAttributesConfig(type, log.Name);
            YT_VERIFY(config);
            if (EventType == ObjectUpdatedEventTypeValue) {
                TString attributesChangedSummary(config->PathToIndex.size(), '\0');
                for (const auto& [path, index] : config->PathToIndex) {
                    // Ignore required permissions: there is no access control model for watches.
                    //
                    // Allow paths not compliant with the proto schema
                    // to separate watch configuration from the proto schema changes.
                    auto resolveResult = ResolveAttribute(
                        object->GetTypeHandler(),
                        path,
                        /*callback*/ {},
                        /*validateProtoSchemaCompliance*/ false);

                    auto changed = object->GetTypeHandler()->IsAttributeChanged(object, resolveResult);

                    attributesChangedSummary[index] = (!changed.has_value()
                        ? NClient::EOptionalBoolean::OB_UNKNOWN
                        : (changed.value()
                            ? NClient::EOptionalBoolean::OB_TRUE
                            : NClient::EOptionalBoolean::OB_FALSE));
                }

                auto changedAttributes = TChangedAttributes{
                    .ChangedSummary = attributesChangedSummary,
                    .PathsMD5 = config->MD5
                };
                EmplaceOrCrash(perLogChangedAttributes, log.Name, changedAttributes);
            }
        }

        std::optional<std::vector<const TTagSet*>> changedTagSets = std::nullopt;
        if constexpr (EventType == ObjectUpdatedEventTypeValue) {
            changedTagSets = object
                ->GetTypeHandler()
                ->CollectChangedAttributeTagSets(object);
        }

        EventsPerObject_[std::pair(type, key)].push_back(TEvent{
            GetEventTypeNameByValueOrThrow(EventType),
            std::move(meta),
            std::move(labels),
            std::move(perLogChangedAttributes),
            std::move(changedTagSets)});
    }

    void InitializeProtoEvent(
        const TObjectKey& objectKey,
        const TEvent& event,
        const TString& logName,
        NProto::TWatchRecord_TEvent* protoEvent) const
    {
        protoEvent->set_type(event.TypeName);
        protoEvent->set_object_id(objectKey.ToString());
        protoEvent->set_meta_yson(event.Meta);
        if (event.ChangedTags) {
            const auto& changedTags = event.ChangedTags.value();
            protoEvent->mutable_changed_attribute_tags()->Reserve(changedTags.size());
            for (const auto* tagSet : changedTags) {
                auto& changedAttributeTags = *protoEvent->add_changed_attribute_tags();
                ToProto(changedAttributeTags, *tagSet);
            }
        }
        if (event.Labels) {
            protoEvent->set_labels_yson(event.Labels);
        }

        if (auto it = event.PerLogChangedAttributes.find(logName); it != event.PerLogChangedAttributes.end()) {
            const auto& changedAttributes = it->second;
            auto* protoChangedAttributes = protoEvent->mutable_changed_attributes();
            protoChangedAttributes->set_changed_summary(changedAttributes.ChangedSummary);
            protoChangedAttributes->set_paths_md5(changedAttributes.PathsMD5);
        }
    }

    i64 GetTabletIndex(
        TObjectTypeValue objectType,
        const TObjectKey& objectKey,
        const TString& logName)
    {
        const auto& distributionPolicy = GetConfig()->DistributionPolicy->GetByType(objectType);

        switch (distributionPolicy->Type) {
            case EDistributionType::Uniform:
                return GetLogInfo(objectType, logName).RandomTabletIndex;
            case EDistributionType::Hash: {
                auto tabletCount = GetLogInfo(objectType, logName).TableMountInfo->Tablets.size();
                YT_VERIFY(tabletCount > 0);

                auto hash = CalculateHash(
                    objectType,
                    objectKey,
                    distributionPolicy->HashInput,
                    distributionPolicy->HashType);
                return GetTabletIndexByHash(hash, tabletCount, distributionPolicy->HashMapper);
            }
        }
    }

    TFingerprint CalculateHash(
        TObjectTypeValue objectType,
        const TObjectKey& objectKey,
        EHashInput hashInput,
        EHashType hashType) const
    {
        TObjectKey key;
        auto* object = Owner_->GetSession()->GetObject(objectType, objectKey);

        switch (hashInput) {
            case EHashInput::TableKey:
                key = object->GetTypeHandler()->GetObjectTableKey(object);
                break;
            case EHashInput::ObjectKey:
                key = objectKey;
                break;
            case EHashInput::TableKeyFirstField:
                key = TObjectKey(object->GetTypeHandler()->GetObjectTableKey(object)[0]);
                break;
        }
        return key.CalculateHash(hashType);
    }

    i64 GetTabletIndexByHash(TFingerprint hash, size_t tabletCount, EHashMapper hashMapper) const
    {
        YT_VERIFY(tabletCount > 0);
        if (tabletCount == 1) {
            return 0;
        }
        switch (hashMapper) {
            case EHashMapper::Range: {
                TFingerprint rangeSize = Max<TFingerprint>() / tabletCount + 1;
                return hash / rangeSize;
            }
            case EHashMapper::Modulo:
                return hash % tabletCount;
        }
    }

    const TLogInfo& GetLogInfo(TObjectTypeValue objectType, const TString& logName)
    {
        std::pair<TObjectTypeValue, TStringBuf> key{objectType, logName};

        return GetOrInsert(PerLogInfo_, key, [&] {
            auto mountInfo = WaitFor(GetTableMountInfo(
                Bootstrap_->GetYTConnector(),
                WatchManager_->GetLogTableOrCrash(objectType, logName)))
                .ValueOrThrow();
            auto randomTabletIndex = mountInfo->GetRandomMountedTabletIndex();
            return TLogInfo{
                .TableMountInfo = std::move(mountInfo),
                .RandomTabletIndex = randomTabletIndex,
            };
        });
    }
};

template <TEventTypeValue EventType>
class TWatchLogWriterBase
    : public IObjectLogContext
{
public:
    TWatchLogWriterBase(ISessionChangeTracker* tracker, TWatchLogWriteContext* writer)
        : Objects_(GetObjects<EventType>(tracker, writer))
        , Context_(*writer)
    { }

    void Preload() override
    {
        if constexpr (EventType == ObjectUpdatedEventTypeValue) {
            std::vector<TObject *> result;
            result.reserve(Objects_.size());
            for (auto* object : Objects_) {
                auto objectType = object->GetType();
                if (object->IsStoreScheduled() &&
                    !Context_.SkipByType(nullptr, objectType, /*count*/ 1))
                {
                    result.push_back(object);
                }
            }
            Objects_ = std::move(result);
        }

        for (auto* object : Objects_) {
            if (Context_.IsLabelsLoadRequired(object->GetType())) {
                object->Labels().ScheduleLoad();
            }
            object->ScheduleMetaResponseLoad();
            if constexpr (EventType == ObjectUpdatedEventTypeValue) {
                Context_.PreloadUpdatedObject(object);
            }
        }
    }

    void Write() override
    {
        for (auto* object : Objects_) {
            Context_.OnEvent<EventType>(object);
        }
    }

protected:
    std::vector<TObject *> Objects_;
    TWatchLogWriteContext& Context_;
};

using TWatchLogCreatedWriter = TWatchLogWriterBase<ObjectCreatedEventTypeValue>;
using TWatchLogUpdatedWriter = TWatchLogWriterBase<ObjectUpdatedEventTypeValue>;
using TWatchLogRemovedWriter = TWatchLogWriterBase<ObjectRemovedEventTypeValue>;

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

IObjectLogContextPtr MakeHistoryWriteContext(
    const TMutatingTransactionOptions& options,
    ISessionChangeTracker* tracker,
    TTransaction* transaction,
    NLogging::TLogger logger)
{
    auto context = std::make_unique<THistoryWriteContext>(logger, tracker, options, transaction);
    context->AddChild(std::make_unique<THistoryCreatedWriter>(tracker, context.get()));
    context->AddChild(std::make_unique<THistoryUpdatedWriter>(tracker, context.get()));
    context->AddChild(std::make_unique<THistoryRemovedWriter>(tracker, context.get()));
    return context;
}

////////////////////////////////////////////////////////////////////////////////

IObjectLogContextPtr MakeWatchLogWriteContext(
    const TMutatingTransactionOptions& options,
    ISessionChangeTracker* tracker,
    TTransaction* transaction,
    NLogging::TLogger logger)
{
    auto context = std::make_unique<TWatchLogWriteContext>(tracker, options, transaction, logger);
    if (!context->Enabled()) {
        return context;
    }

    // NB! The order of event types matters here.
    context->AddChild(std::make_unique<TWatchLogRemovedWriter>(tracker, context.get()));
    context->AddChild(std::make_unique<TWatchLogCreatedWriter>(tracker, context.get()));
    context->AddChild(std::make_unique<TWatchLogUpdatedWriter>(tracker, context.get()));
    return context;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
