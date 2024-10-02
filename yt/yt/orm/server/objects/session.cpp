#include "session.h"

#include "db_config.h"
#include "object.h"
#include "object_manager.h"
#include "object_log.h"
#include "persistence.h"
#include "private.h"

#include <yt/yt/orm/server/access_control/access_control_manager.h>

#include <yt/yt/orm/server/master/bootstrap.h>
#include <yt/yt/orm/server/master/event_log.h>

#include <yt/yt_proto/yt/orm/data_model/generic.pb.h>

#include <yt/yt/orm/client/objects/registry.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <library/cpp/yt/misc/source_location.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct TChildrenAttributeHelper
{
    static void Add(const TChildrenAttributeBase* attribute, TObject* child)
    {
        const_cast<TChildrenAttributeBase*>(attribute)->DoAdd(child);
    }

    static void Remove(const TChildrenAttributeBase* attribute, TObject* child)
    {
        const_cast<TChildrenAttributeBase*>(attribute)->DoRemove(child);
    }
};

////////////////////////////////////////////////////////////////////////////////

namespace {

using namespace NClient::NObjects;
using namespace NTableClient;
using namespace NYT::NTracing;

////////////////////////////////////////////////////////////////////////////////

class TIndexSession
    : public IIndexSession
{
public:
    void TryAddLoadedKey(const TObjectKey& key, bool exists) override
    {
        // Does not rewrite value, add if not exists.
        UniqueKeyExists_.insert({key, exists});
    }

    void AddUniqueKeyOrThrow(const TObjectKey& key, const TString& indexName) override
    {
        auto it = UniqueKeyExists_.find(key);
        THROW_ERROR_EXCEPTION_IF(
            it == UniqueKeyExists_.end(),
            "Unique key %Qv of index %Qv must be loaded",
            key,
            indexName);
        THROW_ERROR_EXCEPTION_IF(
            it->second,
            NClient::EErrorCode::UniqueValueAlreadyExists,
            "Unique key %Qv of index %Qv already exists within the transaction",
            key,
            indexName);
        it->second = true;
    }

    void RemoveUniqueKey(const TObjectKey& key) override
    {
        // Touch-index must operate even for index under building and so does not expect each key to exist.
        UniqueKeyExists_[key] = false;
    }

private:
    THashMap<TObjectKey, bool> UniqueKeyExists_;
}; // class TIndexSession

////////////////////////////////////////////////////////////////////////////////

class TSession
    : public ISession
    , public ISessionChangeTracker
{
public:
    TSession(
        NMaster::IBootstrap* bootstrap,
        TTransaction* owner,
        TTransactionConfigsSnapshot configsSnapshot,
        const NLogging::TLogger& logger)
        : Bootstrap_(bootstrap)
        , Owner_(owner)
        , ConfigsSnapshot_(std::move(configsSnapshot))
        , Logger(logger)
        , EventLogger_(Bootstrap_->CreateEventLogger(Logger))
    { }

    IObjectTypeHandler* GetTypeHandlerOrCrash(TObjectTypeValue type) const override
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        return objectManager->GetTypeHandlerOrCrash(type);
    }

    IObjectTypeHandler* GetTypeHandlerOrThrow(TObjectTypeValue type) const override
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        return objectManager->GetTypeHandlerOrThrow(type);
    }

    TObject* CreateObject(
        TObjectTypeValue type,
        TObjectKey key,
        TObjectKey parentKey,
        bool allowExisting = false) override
    {
        Owner_->EnsureReadWrite();

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto* typeHandler = objectManager->GetTypeHandlerOrThrow(type);

        if (!key) {
            // TODO(YP-3888): Make this unconditional and force the caller to generate the key.
            if (allowExisting) {
                THROW_ERROR_EXCEPTION(
                    "The key must be provided when creating an object "
                    "with the option of allowing it to already exist");
            }

            key = GenerateObjectKey(typeHandler, Owner_);
        }

        ValidateObjectKey(typeHandler, key);

        auto parentType = typeHandler->GetParentType();
        if (parentType != TObjectTypeValues::Null && !parentKey) {
            THROW_ERROR_EXCEPTION("Objects of type %Qv require explicit parent of type %Qv",
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(type),
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(parentType));
        }
        if (parentType == TObjectTypeValues::Null && parentKey) {
            THROW_ERROR_EXCEPTION("Objects of type %Qv do not require explicit parent",
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(type));
        }

        auto typeKeyPair = std::pair(type, key);

        TObject* object = nullptr;

        if (auto it = InstantiatedObjects_.find(typeKeyPair); it != InstantiatedObjects_.end()) {
            auto* existingObject = it->second.get();
            object = existingObject;
        }

        if (!object && allowExisting) {
            // TODO(YP-3888): schedule even without upsert. That would prevent a stray scheduled
            // existence check from getting out of sync with creation (i.e., if someone calls
            // |ScheduleExistenceCheck| without a subsequent |Create| + |allowExisting|, that
            // object may resurface in a |Get| call). That's not done right now, but is unsafe.
            DoScheduleExistenceCheck(typeKeyPair, parentKey);
            if (ExistenceCheckSuccessful(typeKeyPair)) {
                object = GetObject(type, key, parentKey);
            }
        }

        if (object) {
            auto objectState = object->GetState();
            switch (objectState) {
                case EObjectState::Instantiated:
                    if (!object->DoesExist()) {
                        break;
                    }
                    [[fallthrough]];
                case EObjectState::Creating:
                case EObjectState::Finalized:
                case EObjectState::Created:
                    if (!allowExisting) {
                        THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidObjectState,
                            "%v is already in %Qlv state",
                            object->GetDisplayName(/*keyOnly*/ true),
                            objectState);
                    }
                    return object;
                case EObjectState::Finalizing:
                case EObjectState::Removing:
                    [[fallthrough]];
                case EObjectState::Removed:
                    [[fallthrough]];
                case EObjectState::CreatedRemoving:
                    [[fallthrough]];
                case EObjectState::CreatedRemoved:
                    [[fallthrough]];
                case EObjectState::Unknown:
                    YT_ABORT();
            }
        } else {
            auto objectHolder = ReleaseExistenceCheckingObject(typeKeyPair);
            if (!objectHolder) {
                objectHolder = typeHandler->InstantiateObject(key, parentKey, this);
            }
            object = objectHolder.get();
            EmplaceOrCrash(InstantiatedObjects_, typeKeyPair, std::move(objectHolder));
        }

        object->InitializeCreating(/*predecessor*/ FindRemovedObject(type, key));

        EmplaceOrCrash(CreatedObjects_, typeKeyPair, object);

        typeHandler->InitializeCreatedObject(Owner_, object);

        if (parentType != TObjectTypeValues::Null) {
            auto* parent = GetObject(parentType, parentKey);
            auto* attribute = typeHandler->GetParentChildrenAttribute(parent);
            TChildrenAttributeHelper::Add(attribute, object);
        }

        YT_LOG_DEBUG("Object created (ObjectKey: %v, ParentKey: %v, Type: %v)",
            key,
            parentKey,
            NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(type));

        return object;
    }

    TObject* GetObject(
        TObjectTypeValue type,
        TObjectKey key,
        TObjectKey parentKey = {}) override
    {
        if (!key) {
            THROW_ERROR_EXCEPTION(
                NClient::EErrorCode::InvalidObjectId,
                "%v key cannot be empty",
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetCapitalizedHumanReadableTypeNameOrCrash(type));
        }

        auto validateParent = [&parentKey] (const TObject* object) {
            if (!parentKey) {
                return;
            }
            auto oldParentKey = object->GetParentKey();
            if (parentKey != oldParentKey) {
                THROW_ERROR_EXCEPTION(
                    "%v has a parent key %v but was provided with %v",
                    object->GetDisplayName(/*keyOnly*/ true),
                    oldParentKey,
                    parentKey);
            }
        };

        auto typeKeyPair = std::pair(type, key);
        auto instantiatedIt = InstantiatedObjects_.find(typeKeyPair);
        if (instantiatedIt == InstantiatedObjects_.end()) {
            if (auto* removedObject = FindRemovedObject(type, key)) {
                validateParent(removedObject);
                return removedObject;
            }
            const auto& objectManager = Bootstrap_->GetObjectManager();
            auto* typeHandler = objectManager->GetTypeHandlerOrThrow(type);

            auto objectHolder = ReleaseExistenceCheckingObject(typeKeyPair);
            if (!objectHolder) {
                objectHolder = typeHandler->InstantiateObject(key, parentKey, this);
            }

            auto* object = objectHolder.get();
            instantiatedIt = EmplaceOrCrash(InstantiatedObjects_,
                typeKeyPair,
                std::move(objectHolder));
            object->InitializeInstantiated(parentKey);

            TString serializedParentKey;
            if (!typeHandler->HasParent()) {
                serializedParentKey = "<None>";
            } else if (parentKey) {
                serializedParentKey = parentKey.ToString();
            } else {
                serializedParentKey = "<Unknown>";
            }

            YT_LOG_DEBUG("Object instantiated (ObjectKey: %v, ParentKey: %v, Type: %v)",
                key,
                serializedParentKey,
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(type));
        } else {
            validateParent(instantiatedIt->second.get());
        }

        return instantiatedIt->second.get();
    }

    void RemoveObject(TObject* object) override
    {
        RemoveObjects({object});
    }

    void RemoveObjects(std::vector<TObject*> objects) override
    {
        TTraceContextGuard guard(CreateTraceContextFromCurrent("NYT::NOrm::TSession::RemoveObjects"));

        Owner_->EnsureReadWrite();

        auto context = Owner_->CreateUpdateContext();
        {
            TTraceContextGuard guard(CreateTraceContextFromCurrent("NYT::NOrm::TSession::PreloadObjectsRemoval"));

            for (auto* object : objects) {
                PreloadObjectRemoval(object, context.get());
            }
            context->Commit();
            FlushLoads();
        }
        {
            TTraceContextGuard guard(CreateTraceContextFromCurrent("NYT::NOrm::TSession::CheckObjectsRemoval"));

            for (auto* object : objects) {
                CheckObjectRemoval(object);
            }
        }
        {
            TTraceContextGuard guard(CreateTraceContextFromCurrent("NYT::NOrm::TSession::PrepareObjectFinalization"));

            for (auto* object : objects) {
                PrepareObjectFinalization(object);
            }
        }
        {
            TTraceContextGuard guard(CreateTraceContextFromCurrent("NYT::NOrm::TSession::FinalizeObjectRemoval"));

            for (auto* object : objects) {
                FinalizeObjectRemoval(object, context.get());
            }
            context->Commit();
        }
    }

    void ScheduleExistenceCheck(
        TObjectTypeValue type,
        TObjectKey key,
        TObjectKey parentKey) override
    {
        DoScheduleExistenceCheck(std::pair(type, std::move(key)), std::move(parentKey));
    }

    void ScheduleLoad(
        TLoadCallback callback,
        ELoadPriority priority = ELoadPriority::Default) override
    {
        YT_VERIFY(ScheduledLoads_.IsValidIndex(priority));
        ScheduledLoads_[priority].push_back(std::move(callback));
    }

    void ScheduleFinalize(TFinalizeCallback callback) override
    {
        Owner_->EnsureReadWrite();
        THROW_ERROR_EXCEPTION_IF(Finalized_, "Session already finalized");
        ScheduledFinalizes_.push_back(std::move(callback));
    }

    void ScheduleStore(TStoreCallback callback) override
    {
        Owner_->EnsureReadWrite();
        ScheduledStores_.push_back(std::move(callback));
    }

    void ProfileObjectsAttributes() override
    {
        for (const auto& [_, object] : InstantiatedObjects_) {
            auto state = object->GetState();
            if (state == EObjectState::Instantiated || state == EObjectState::Created) {
                object->GetTypeHandler()->ProfileAttributes(Owner_, object.get());
            }
        }
    }

    void FlushTransaction(const TMutatingTransactionOptions& options) override
    {
        Finalize();
        FlushObjectsDeletion();
        ValidateCreatedObjects();
        FlushObjectsCreation();
        FlushHistoryAndWatchLogs(options);
        std::vector<TError> errors;
        while (HasPendingLoads() || HasPendingStores()) {
            FlushLoadsOnce(&errors, std::source_location::current());
            FlushStoresOnce(&errors);
        }

        if (!errors.empty()) {
            THROW_ERROR_EXCEPTION("Persistence failure")
                << errors;
        }
    }

    std::vector<TObject*> GetCreatedObjects(IObjectFilter* filter) const override
    {
        std::vector<TObject*> result;
        for (const auto& [_, object] : CreatedObjects_) {
            auto* typeHandler = object->GetTypeHandler();
            if (!filter->SkipByType(typeHandler, typeHandler->GetType(), /*count*/ 1)) {
                result.push_back(object);
            }
        }
        return result;
    }

    std::vector<TObject*> GetRemovedObjects(IObjectFilter* filter) const override
    {
        std::vector<TObject*> result;
        for (const auto& [type, removedObjects] : RemovedObjects_) {
            auto* typeHandler = Bootstrap_->GetObjectManager()->FindTypeHandler(type);
            if (!filter->SkipByType(typeHandler, type, /*count*/ std::ssize(removedObjects))) {
                for (const auto& [objectId, object] : removedObjects) {
                    result.push_back(object);
                }
            }
        }
        return result;
    }

    std::vector<TObject*> GetUpdatedObjects() const override
    {
        std::vector<TObject*> result;
        result.reserve(InstantiatedObjects_.size());
        for (auto& [_, objectPtr]  : InstantiatedObjects_) {
            auto objectState = objectPtr->GetState();
            if (objectState == EObjectState::Instantiated || objectState == EObjectState::Finalized) {
                result.push_back(objectPtr.get());
            }
        }
        return result;
    }

    void FlushHistoryAndWatchLogs(const TMutatingTransactionOptions& options)
    {
        auto historyWriteContext = MakeHistoryWriteContext(options, this, Owner_, Logger);
        auto watchLogWriteContext = MakeWatchLogWriteContext(options, this, Owner_, Logger);

        historyWriteContext->Preload();
        watchLogWriteContext->Preload();
        historyWriteContext->Write();
        watchLogWriteContext->Write();
    }

    void FlushLoads(std::source_location location = std::source_location::current()) override
    {
        std::vector<TError> errors;
        while (HasPendingLoads()) {
            FlushLoadsOnce(&errors, location);
        }
        if (!errors.empty()) {
            THROW_ERROR_EXCEPTION("Persistence failure")
                << errors
                << TErrorAttribute("location", NYT::ToString(location));
        }
    }

    const TTransactionConfigsSnapshot& GetConfigsSnapshot() const override
    {
        return ConfigsSnapshot_;
    }

    TIndexSession* GetOrCreateIndexSession(const TString& indexName) override
    {
        return &IndexSessionPerName_[indexName];
    }

    TTransaction* GetOwner() const override
    {
        return Owner_;
    }

    void SetTestingStorageOptions(TTestingStorageOptions options) override
    {
        TestingStorageOptions_ = std::move(options);
    }

    NQueryClient::TColumnEvaluatorPtr GetObjectTableEvaluator(TObjectTypeValue type) override
    {
        auto* typeHandler = GetTypeHandlerOrCrash(type);
        if (auto it = ColumnEvaluatorCache_.find(type); it != ColumnEvaluatorCache_.end()) {
            return it->second;
        }
        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto commonCache = objectManager->GetColumnEvaluatorCache();
        // "Find" returns a copy of the evaluator. We store it in the session's
        // data member and use during the entire transaction. Since requests within a single transaction are not
        // executed concurrently, there should not be any problems using a non-thread-safe evaluator.
        auto copiedEvaluatorIt = EmplaceOrCrash(
            ColumnEvaluatorCache_,
            type,
            commonCache->Find(typeHandler->GetTableSchema()));
        return copiedEvaluatorIt->second;
    }

private:
    NMaster::IBootstrap* const Bootstrap_;
    TTransaction* const Owner_;
    const TTransactionConfigsSnapshot ConfigsSnapshot_;
    const NLogging::TLogger& Logger;
    const NMaster::TEventLoggerPtr EventLogger_;

    using TTypeKeyPair = std::pair<TObjectTypeValue, TObjectKey>;
    THashMap<TTypeKeyPair, std::unique_ptr<TObject>> InstantiatedObjects_;
    std::vector<std::unique_ptr<TObject>> RemovedObjectsHolders_;

    // Objects that have an existence check is scheduled to determine whether they are
    // |Created| or |Instantiated| (for |Create| + |allowExisting|). When the check is done,
    // they move to |InstantiatedObjects| before being returned to the caller.
    THashMap<TTypeKeyPair, std::unique_ptr<TObject>> ExistenceCheckingObjects_;

    THashMap<TTypeKeyPair, TObject*> CreatedObjects_;
    THashMap<TObjectTypeValue, THashMap<TObjectKey, TObject*>> RemovedObjects_;

    TEnumIndexedArray<ELoadPriority, std::vector<TLoadCallback>> ScheduledLoads_;
    std::vector<TFinalizeCallback> ScheduledFinalizes_;
    std::vector<TStoreCallback> ScheduledStores_;

    bool Finalized_ = false; // Eventually should expand to a full lifecycle enum.

    THashMap<TString, TIndexSession> IndexSessionPerName_;

    TTestingStorageOptions TestingStorageOptions_;

    THashMap<TObjectTypeValue, NQueryClient::TColumnEvaluatorPtr> ColumnEvaluatorCache_;

    void CheckChildrenRemovalAllowed(const TObject* parent, TObjectTypeValue childrenType, int childrenCount) const
    {
        const auto* childrenTypeHandler = GetTypeHandlerOrThrow(childrenType);
        if (!childrenCount || !childrenTypeHandler->IsParentRemovalForbidden()) {
            return;
        }
        bool removalForbidden = !ConfigsSnapshot_.TransactionManager->AllowParentRemovalOverride
            .value_or(Owner_->RemovalWithNonEmptyReferencesAllowed()
            .value_or(false));
        THROW_ERROR_EXCEPTION_IF(removalForbidden,
            NClient::EErrorCode::RemovalForbidden,
            "Removal of %v is forbidden since it has %v related children of type %v",
            parent->GetDisplayName(/*keyOnly*/ true),
            childrenCount,
            NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrThrow(childrenType));
    }

    void PreloadObjectRemoval(const TObject* object, IUpdateContext* context)
    {
        YT_LOG_DEBUG("Preparing object removal (ObjectKey: %v, Type: %v)",
            object->GetKey(),
            NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(object->GetType()));

        object->GetTypeHandler()->PreloadObjectRemoval(Owner_, object, context);
        if (Bootstrap_->GetDBConfig().EnableFinalizers && object->GetTypeHandler()->HasParent()) {
            object->GetTypeHandler()->GetParent(object)->Finalizers().ScheduleLoad();
        }

        object->GetTypeHandler()->ForEachChildrenAttribute(object,
            [context, this, parent = object] (const TChildrenAttributeBase* childrenAttribute) {
                childrenAttribute->ScheduleLoad();
                context->AddPreparer([childrenAttribute, this, parent] (IUpdateContext* context) {
                    const auto& children = childrenAttribute->UntypedLoad();

                    // Checking in preload stage to prevent excessive lookups.
                    CheckChildrenRemovalAllowed(parent, childrenAttribute->GetChildrenType(), children.size());

                    for (auto* child : children) {
                        PreloadObjectRemoval(child, context);
                    }
                });
            });
    }

    static void CheckObjectStateBeforeRemoval(const TObject* object)
    {
        switch (object->GetState()) {
            case EObjectState::Instantiated:
                object->ValidateExists();
                break;
            case EObjectState::Created:
            case EObjectState::Finalized:
                break;
            case EObjectState::Finalizing:
                THROW_ERROR_EXCEPTION("%v cannot be removed during finalization",
                    object->GetDisplayName(/*keyOnly*/ true));
            case EObjectState::Creating:
                THROW_ERROR_EXCEPTION("%v cannot be removed during creation",
                    object->GetDisplayName(/*keyOnly*/ true));
            case EObjectState::Removing:
            case EObjectState::Removed:
            case EObjectState::CreatedRemoving:
            case EObjectState::CreatedRemoved:
                THROW_ERROR_EXCEPTION(NClient::EErrorCode::NoSuchObject,
                    "%v is already removed",
                    object->GetDisplayName(/*keyOnly*/ true));
            case EObjectState::Unknown:
                YT_ABORT();
        }
    }

    void CheckObjectRemoval(const TObject* object)
    {
        CheckObjectStateBeforeRemoval(object);

        if (object->IsFinalized()) {
            return;
        }

        YT_LOG_DEBUG("Finalizing object removal (ObjectKey: %v, Type: %v)",
            object->GetKey(),
            NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(object->GetType()));

        object->GetTypeHandler()->CheckObjectRemoval(Owner_, object);
        for (const auto* observer : object->LifecycleObservers()) {
            observer->CheckObjectRemoval();
        }

        object->GetTypeHandler()->ForEachChildrenAttribute(
            object,
            [this] (const TChildrenAttributeBase* childrenAttribute) {
                const auto& children = childrenAttribute->UntypedLoad();
                for (const auto* child : children) {
                    CheckObjectRemoval(child);
                }
            });
    }

    void PrepareObjectFinalization(TObject* object)
    {
        if (object->IsFinalized()) {
            return;
        }

        if (object->GetState() == EObjectState::Created) {
            object->SetState(EObjectState::CreatedRemoving);
        } else {
            object->SetState(EObjectState::Finalizing);
        }
        YT_LOG_DEBUG("Object finalized (ObjectKey: %v, Type: %v)",
            object->GetKey(),
            NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(object->GetType()));

        object->GetTypeHandler()->StartObjectRemoval(Owner_, object);
        for (auto* observer : object->LifecycleObservers()) {
            observer->OnObjectRemovalStart();
        }

        bool asynchronousRemovalsEnabled =
            Bootstrap_->GetObjectManager()->GetConfig()->RemoveMode == ERemoveObjectMode::Asynchronous &&
            Bootstrap_->GetDBConfig().EnableAsynchronousRemovals;

        if (object->GetState() == EObjectState::Finalizing && asynchronousRemovalsEnabled) {
            // TODO(dgolear): Waiting for YT-20282 :(
            ScheduleStore([object] (IStoreContext* context) {
                context->WriteRow(
                    object->GetTypeHandler()->GetTable(),
                    object->GetTypeHandler()->GetObjectTableKey(object),
                    std::array{
                        &ObjectsTable.Fields.ExistenceLock,
                    },
                    ToUnversionedValues(
                        context->GetRowBuffer(),
                        false));
            });
        }

        if (object->GetState() != EObjectState::CreatedRemoving) {
            object->SetState(EObjectState::Finalized);
        }

        bool hasFinalizingChildren = false;
        bool hasNonFinalizingChildren = false;
        object->GetTypeHandler()->ForEachChildrenAttribute(object,
            [&] (const TChildrenAttributeBase* childrenAttribute)
        {
            const auto& children = childrenAttribute->UntypedLoad();
            for (auto* child : children) {
                PrepareObjectFinalization(child);
                if (child->HasActiveFinalizers()) {
                    hasFinalizingChildren = true;
                } else {
                    hasNonFinalizingChildren = true;
                }
            }
        });
        if (hasFinalizingChildren) {
            YT_LOG_DEBUG("Cannot remove object since it has finalizing children; "
                "adding children finalizer (ObjectKey: %v, Type: %v)",
                object->GetKey(),
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(object->GetType()));
            object->AddFinalizer(ChildrenFinalizer, NRpc::RootUserName);
        }

        if (ConfigsSnapshot_.ObjectManager->ParentFinalizationMode == EParentFinalizationMode::FinalizeChildren &&
            object->HasActiveFinalizers() &&
            hasNonFinalizingChildren)
        {
            PropagateFinalizationToChildren(object);
        }
    }

    void PropagateFinalizationToChildren(TObject* object)
    {
        object->GetTypeHandler()->ForEachChildrenAttribute(object,
            [&] (const TChildrenAttributeBase* childrenAttribute)
        {
            const auto& children = childrenAttribute->UntypedLoad();
            for (auto* child : children) {
                if (!child->HasActiveFinalizers()) {
                    THROW_ERROR_EXCEPTION_IF(child->GetState() == EObjectState::CreatedRemoving,
                        "Cannot finalize %v since it is being created",
                        child->GetDisplayName());
                    YT_LOG_DEBUG(
                        "Object's child is finalizing, adding parent finalizer to it (ObjectKey: %v, Type: %v)",
                        object->GetKey(),
                        NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(object->GetType()));
                    child->AddFinalizer(ParentFinalizer, NRpc::RootUserName);
                    PropagateFinalizationToChildren(child);
                }
            }
        });
    }

    void FinalizeObjectRemoval(TObject* object, IUpdateContext* context)
    {
        THROW_ERROR_EXCEPTION_UNLESS(object->IsFinalized() || object->GetState() == EObjectState::CreatedRemoving,
            "Cannot remove %v in %Qlv state", object->GetDisplayName(), object->GetState());
        if (object->IsFinalized() && object->HasActiveFinalizers()) {
            YT_LOG_DEBUG("Skipping object removal since it has active finalizers (ObjectKey: %v, Type: %v)",
                object->GetKey(),
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(object->GetType()));
            return;
        }

        if (object->GetState() != EObjectState::CreatedRemoving) {
            object->SetState(EObjectState::Removing);
        }

        YT_LOG_DEBUG("Object removed (ObjectKey: %v, Type: %v)",
            object->GetKey(),
            NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(object->GetType()));

        object->GetTypeHandler()->FinishObjectRemoval(Owner_, object);
        for (auto* observer : object->LifecycleObservers()) {
            observer->OnObjectRemovalFinish();
        }

        if (object->GetState() == EObjectState::CreatedRemoving) {
            object->SetState(EObjectState::CreatedRemoved);
        } else {
            RemovedObjects_[object->GetType()][object->GetKey()] = object;
            object->SetState(EObjectState::Removed);
        }

        auto typeKeyPair = std::pair(object->GetType(), object->GetKey());
        {
            auto it = InstantiatedObjects_.find(typeKeyPair);
            if (it != InstantiatedObjects_.end()) {
                RemovedObjectsHolders_.push_back(std::move(it->second));
                InstantiatedObjects_.erase(it);
            }
        }
        {
            auto it = CreatedObjects_.find(typeKeyPair);
            if (it != CreatedObjects_.end()) {
                CreatedObjects_.erase(it);
            }
        }

        auto* typeHandler = object->GetTypeHandler();
        auto parentType = typeHandler->GetParentType();
        if (parentType != TObjectTypeValues::Null) {
            auto* parent = GetObject(parentType, object->GetParentKey());
            auto* attribute = typeHandler->GetParentChildrenAttribute(parent);
            TChildrenAttributeHelper::Remove(attribute, object);
            if (parent->IsFinalizerActive(ChildrenFinalizer)) {
                YT_VERIFY(parent->IsFinalized());
                // NB: Shedule it separately as direct check may lead to quadratic complexity.
                Owner_->ScheduleCommitAction(TCommitActionTypes::RemoveChildrenFinalizers, parent);
            }
        }

        object->GetTypeHandler()->ForEachChildrenAttribute(object,
            [context, this] (const TChildrenAttributeBase* childrenAttribute) mutable {
                // NB: Make a copy of children. Otherwise, children removal above might invalidate iterators.
                auto children = childrenAttribute->UntypedLoad();
                context->AddFinalizer([children = std::move(children), this] (IUpdateContext* context) {
                    for (auto* child : children) {
                        if (child->IsFinalizerActive(ParentFinalizer)) {
                            child->CompleteFinalizer(ParentFinalizer);
                        }
                        FinalizeObjectRemoval(child, context);
                    }
                });
            });
    }

    void DoScheduleExistenceCheck(
        const TTypeKeyPair& typeKeyPair,
        const TObjectKey& parentKey)
    {
        if (InstantiatedObjects_.contains(typeKeyPair)) {
            // Already scheduled in another call, meaningless in |Create|, nothing to do here.
            return;
        }
        if (FindRemovedObject(typeKeyPair.first, typeKeyPair.second)) {
            // The object is removed within the session, but not in storage. Checking the table
            // would report it as existing. Instead, consider the existence check failed.
            return;
        }
        if (ExistenceCheckingObjects_.contains(typeKeyPair)) {
            // Already scheduled, nothing to do here.
            return;
        }
        auto objectHolder = GetTypeHandlerOrCrash(typeKeyPair.first)->InstantiateObject(
            typeKeyPair.second,
            parentKey,
            this);

        objectHolder->InitializeInstantiated(parentKey); // This schedules the check.
        ExistenceCheckingObjects_.emplace(typeKeyPair, std::move(objectHolder));
    }

    bool ExistenceCheckSuccessful(TObjectTypeValue type, TObjectKey key) override
    {
        return ExistenceCheckSuccessful(std::pair(type, std::move(key)));
    }

    // True if a check was scheduled and the object does exist.
    bool ExistenceCheckSuccessful(const TTypeKeyPair& typeKeyPair)
    {
        auto it = ExistenceCheckingObjects_.find(typeKeyPair);
        return it != ExistenceCheckingObjects_.end() && it->second->DoesExist();
    }

    std::unique_ptr<TObject> ReleaseExistenceCheckingObject(const TTypeKeyPair& typeKeyPair)
    {
        std::unique_ptr<TObject> objectHolder;
        auto it = ExistenceCheckingObjects_.find(typeKeyPair);
        if (it != ExistenceCheckingObjects_.end()) {
            objectHolder = std::move(it->second);
            EraseOrCrash(ExistenceCheckingObjects_, typeKeyPair);
        }
        return objectHolder;
    }

    TObject* FindRemovedObject(TObjectTypeValue type, const TObjectKey& key)
    {
        auto it = RemovedObjects_.find(type);
        if (it == RemovedObjects_.end()) {
            return nullptr;
        }
        auto it2 = it->second.find(key);
        return it2 == it->second.end() ? nullptr : it2->second;
    }

    bool HasPendingLoads()
    {
        for (const auto& loads : ScheduledLoads_) {
            if (!loads.empty()) {
                return true;
            }
        }
        return false;
    }

    bool HasPendingStores()
    {
        return !ScheduledStores_.empty();
    }

    void ValidateCreatedObjects()
    {
        i64 eventCount = 0;

        std::vector<std::unique_ptr<TObjectExistenceChecker>> checkers;
        std::vector<TObject*> parents;
        for (const auto& item : CreatedObjects_) {
            const auto& key = item.first;
            auto* object = item.second;

            if (object->GetState() != EObjectState::Created) {
                continue;
            }

            ++eventCount;

            if (!FindRemovedObject(key.first, key.second)) {
                auto checker = std::make_unique<TObjectExistenceChecker>(object);
                checker->ScheduleCheck();
                checkers.push_back(std::move(checker));
            }

            auto* typeHandler = object->GetTypeHandler();
            auto parentType = typeHandler->GetParentType();
            if (parentType != TObjectTypeValues::Null) {
                auto parentKey = object->GetParentKey();
                auto* parent = GetObject(parentType, std::move(parentKey));
                parents.push_back(parent);
            }

            typeHandler->ValidateCreatedObject(Owner_, object);
        }

        for (const auto& checker : checkers) {
            if (checker->Check()) {
                auto* object = checker->GetObject();
                THROW_ERROR_EXCEPTION(
                    NClient::EErrorCode::DuplicateObjectId,
                    "%v already exists",
                    object->GetDisplayName(/*keyOnly*/ true));
            }
        }

        for (const auto* parent : parents) {
            parent->ValidateNotFinalized();
        }

        YT_LOG_DEBUG_UNLESS(eventCount == 0,
            "Created objects are validated (Count: %v)",
            eventCount);
    }

    void FlushObjectsCreation()
    {
        auto context = Owner_->CreateStoreContext();

        i64 eventCount = 0;

        const auto* parentsTable = Bootstrap_->GetParentsTable();

        bool asynchronousRemovalsEnabled = Bootstrap_->GetDBConfig().EnableAsynchronousRemovals;
        bool clearPendingRemovals =
            asynchronousRemovalsEnabled &&
            ConfigsSnapshot_.ObjectManager->RemoveMode != ERemoveObjectMode::SynchronousExclusive;

        for (const auto& item : CreatedObjects_) {
            auto* object = item.second;

            if (object->GetState() != EObjectState::Created) {
                continue;
            }

            ++eventCount;

            auto* typeHandler = object->GetTypeHandler();
            auto objectsTableKey = typeHandler->GetObjectTableKey(object);
            const auto* table = typeHandler->GetTable();

            // Deletion process is asynchronous, so
            // there is a chance that the data from a previous
            // incarnation is present yet.
            //
            // Delete it explicitly to have clean row before the creation.
            context->DeleteRow(table, objectsTableKey);

            if (clearPendingRemovals) {
                auto key = typeHandler->GetObjectTableKey(object);
                context->DeleteRow(
                    &PendingRemovalsTable,
                    TObjectKey{object->GetType(), key.ToString()});
            }

            if (typeHandler->HasParent()) {
                auto* parent = typeHandler->GetParent(object);
                const auto* typedParentsTable = typeHandler->GetParentsTable();
                EParentsTableMode parentsMode = typedParentsTable
                    ? ConfigsSnapshot_.ObjectManager->GetParentsTableMode(typeHandler->GetType())
                    : EParentsTableMode::NoSeparateTable;
                YT_VERIFY(parent);

                auto parentTypeHandler = parent->GetTypeHandler();
                if (parent->DoesExist()) {
                    context->LockRow(
                        parentTypeHandler->GetTable(),
                        parentTypeHandler->GetObjectTableKey(parent),
                        std::array{&ObjectsTable.Fields.ExistenceLock},
                        ELockType::SharedStrong);
                }
                auto parentKey = object->GetParentKey();
                YT_VERIFY(parentKey);

                if (parentsMode >= EParentsTableMode::WriteToSeparateTable) {
                    context->WriteRow(
                        typedParentsTable,
                        object->GetKey(),
                        typeHandler->GetParentKeyFields(),
                        ToUnversionedRow(parentKey, context->GetRowBuffer()).Elements());
                }
                if (parentsMode != EParentsTableMode::DontWriteToCommonTable && parentsTable) {
                    context->WriteRow(
                        parentsTable,
                        TObjectKey(object->GetKey().ToString(), object->GetType()),
                        std::array{&parentsTable->Fields.ParentId},
                        ToUnversionedValues(
                            context->GetRowBuffer(),
                            parentKey.ToString()));
                }
            }
        }

        context->FillTransaction();

        YT_LOG_DEBUG_UNLESS(eventCount == 0, "Objects creation prepared (Count: %v)", eventCount);
    }

    THashMap<TObjectTypeValue, bool> BuildTypeToSkipEventPermissionMap(bool skipDefault) override
    {
        THashMap<TObjectTypeValue, bool> perTypeSkipEventPermission;
        for (const auto& pair : RemovedObjects_) {
            auto type = pair.first;
            GetOrInsert(perTypeSkipEventPermission, type, [&] { return ShouldSkipEvent(type, skipDefault); });
        }
        for (const auto& [_, object] : InstantiatedObjects_) {
            if (!object->IsStoreScheduled()) {
                continue;
            }
            auto type = object.get()->GetType();
            GetOrInsert(perTypeSkipEventPermission, type, [&] { return ShouldSkipEvent(type, skipDefault); });
        }
        for (const auto& [_, object] : CreatedObjects_) {
            auto type = object->GetType();
            GetOrInsert(perTypeSkipEventPermission, type, [&] { return ShouldSkipEvent(type, skipDefault); });
        }
        return perTypeSkipEventPermission;
    }

    bool ShouldSkipEvent(TObjectTypeValue type, bool skipDefault)
    {
        auto accessControlManager = Bootstrap_->GetAccessControlManager();
        if (!skipDefault) {
            return false;
        }
        auto userId = NAccessControl::TryGetAuthenticatedUserIdentity().value_or(
            NRpc::GetRootAuthenticationIdentity()).User;
        if (accessControlManager->IsSuperuser(userId)) {
            return true;
        }
        if (TObjectTypeValues::Schema == type) {
            return false;
        }
        auto checkResult = accessControlManager->CheckCachedPermission(
            userId,
            TObjectTypeValues::Schema,
            TObjectKey(NClient::NObjects::GetGlobalObjectTypeRegistry()
                ->GetTypeNameByValueOrCrash(type)),
            NAccessControl::TAccessControlPermissionValues::Use,
            EventGenerationSkipAccessControlPath);

        return checkResult.Action == NAccessControl::EAccessControlAction::Allow;
    }

    void FlushObjectsDeletion()
    {
        auto now = Owner_->GetCommitStartTime();

        auto context = Owner_->CreateStoreContext();

        const auto& objectManager = Bootstrap_->GetObjectManager();

        i64 eventCount = 0;

        const auto* parentsTable = Bootstrap_->GetParentsTable();

        bool asynchronousRemovalsEnabled = Bootstrap_->GetDBConfig().EnableAsynchronousRemovals;
        bool removeSynchronously =
            !asynchronousRemovalsEnabled ||
            ConfigsSnapshot_.ObjectManager->RemoveMode != ERemoveObjectMode::Asynchronous;

        for (const auto& [type, removedObjects] : RemovedObjects_) {
            auto* typeHandler = objectManager->FindTypeHandler(type);
            YT_VERIFY(typeHandler);

            auto parentType = typeHandler->GetParentType();
            const auto* table = typeHandler->GetTable();
            const auto* typedParentsTable = typeHandler->GetParentsTable();
            EParentsTableMode parentsMode = typedParentsTable
                ? ConfigsSnapshot_.ObjectManager->GetParentsTableMode(typeHandler->GetType())
                : EParentsTableMode::NoSeparateTable;

            for (const auto& item : removedObjects) {
                ++eventCount;

                const auto* object = item.second;
                auto key = typeHandler->GetObjectTableKey(object);
                if (!removeSynchronously) {
                    context->WriteRow(
                        table,
                        key,
                        std::array{
                            &ObjectsTable.Fields.MetaRemovalTime,
                            &ObjectsTable.Fields.ExistenceLock,
                        },
                        ToUnversionedValues(
                            context->GetRowBuffer(),
                            now,
                            false));
                }

                TObjectKey objectTypeKey(object->GetKey().ToString(), object->GetType());
                if (Bootstrap_->GetDBConfig().EnableTombstones) {
                    context->WriteRow(
                        &TombstonesTable,
                        objectTypeKey,
                        std::array{&TombstonesTable.Fields.RemovalTime},
                        ToUnversionedValues(
                            context->GetRowBuffer(),
                            now));
                }
                if (parentType != TObjectTypeValues::Null) {
                    if (parentsMode >= EParentsTableMode::WriteToSeparateTable) {
                        context->DeleteRow(typedParentsTable, object->GetKey());
                    }
                    if (parentsMode != EParentsTableMode::DontWriteToCommonTable && parentsTable) {
                        context->DeleteRow(parentsTable, objectTypeKey);
                    }
                }

                if (removeSynchronously) {
                    context->DeleteRow(table, key);
                } else {
                    context->WriteRow(
                        &PendingRemovalsTable,
                        TObjectKey{object->GetType(), key.ToString()},
                        std::array{&PendingRemovalsTable.Fields.RemovalTime},
                        ToUnversionedValues(
                            context->GetRowBuffer(),
                            now));
                }
            }
        }

        context->FillTransaction();

        YT_LOG_DEBUG_UNLESS(eventCount == 0, "Prepared objects deletion (Count: %v)", eventCount);
    }

    void Finalize()
    {
        THROW_ERROR_EXCEPTION_IF(Finalized_, "Session already finalized");

        while (!ScheduledFinalizes_.empty()) {
            std::vector<TFinalizeCallback> finalizes;
            finalizes.swap(ScheduledFinalizes_);
            for (const auto& callback : finalizes) {
                callback();
            }
        }

        Finalized_ = true;
    }

    void FlushLoadsOnce(std::vector<TError>* errors, std::source_location location)
    {
        TEnumIndexedArray<ELoadPriority, std::vector<TLoadCallback>> allScheduledLoads;
        std::swap(allScheduledLoads, ScheduledLoads_);
        for (auto priority : TEnumTraits<ELoadPriority>::GetDomainValues()) {
            auto& scheduledLoads = allScheduledLoads[priority];
            if (scheduledLoads.empty()) {
                continue;
            }

            EventLogger_->LogFluently(NLogging::ELogLevel::Debug,
                "Preparing reads",
                NMaster::TEventLogOptions{
                    .WriteTableLog = GetConfigsSnapshot().TransactionManager->EnableEventLogTableWrite,
                })
                .Item("priority").Value(priority)
                .Item("count").Value(scheduledLoads.size())
                .Item("location").Value(NYT::ToString(location));

            auto context = Owner_->CreateLoadContext(TestingStorageOptions_);

            for (const auto& callback : scheduledLoads) {
                try {
                    callback(context.get());
                } catch (const std::exception& ex) {
                    errors->push_back(ex);
                }
            }

            context->RunReads();
        }
    }

    void FlushStoresOnce(std::vector<TError>* errors)
    {
        if (ScheduledStores_.empty()) {
            return;
        }

        auto context = Owner_->CreateStoreContext();

        decltype(ScheduledStores_) swappedStores;
        std::swap(ScheduledStores_, swappedStores);
        for (const auto& callback : swappedStores) {
            try {
                callback(context.get());
            } catch (const std::exception& ex) {
                errors->push_back(ex);
            }
        }

        context->FillTransaction();

        YT_LOG_DEBUG_UNLESS(ScheduledStores_.empty(),
            "Writes prepared (Count: %v)",
            ScheduledStores_.size());
    }
}; // class TSession

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ISession> CreateSession(
    NMaster::IBootstrap* bootstrap,
    TTransaction* owner,
    TTransactionConfigsSnapshot configsSnapshot,
    const NLogging::TLogger& logger)
{
    return std::make_unique<TSession>(
        bootstrap,
        owner,
        std::move(configsSnapshot),
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
