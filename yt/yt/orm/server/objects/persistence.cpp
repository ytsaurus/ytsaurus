#include "persistence.h"

#include "attribute_schema.h"
#include "config.h"
#include "db_schema.h"
#include "helpers.h"
#include "key_util.h"
#include "object.h"
#include "object_manager.h"
#include "private.h"
#include "session.h"
#include "type_handler.h"

#include <yt/yt/orm/server/master/bootstrap.h>

#include <yt/yt/orm/library/query/filter_matcher.h>
#include <yt/yt/orm/library/query/filter_introspection.h>

#include <yt/yt/library/query/base/ast.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/client/api/rowset.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <library/cpp/yt/misc/variant.h>

#include <ranges>

namespace NYT::NOrm::NServer::NObjects {

using namespace NYT::NTableClient;
using namespace NYT::NApi;
using namespace NYT::NQueryClient::NAst;
using namespace NYT::NYson;
using namespace NYT::NYPath;

////////////////////////////////////////////////////////////////////////////////

void TControlAttributeObserver::OnCall(TStringBuf method)
{
    MethodsCalled_.emplace(method);
}

bool TControlAttributeObserver::IsCalled() const
{
    return !MethodsCalled_.empty();
}

bool TControlAttributeObserver::IsCalled(TStringBuf method) const
{
    return MethodsCalled_.contains(method);
}

////////////////////////////////////////////////////////////////////////////////

auto TAttributeObservers::Get() const -> const TObservers&
{
    return Observers_;
}

void TAttributeObservers::Add(std::function<void(TObject*, bool)> observer) const
{
    Observers_.push_back(std::move(observer));
}

////////////////////////////////////////////////////////////////////////////////

TScalarAttributeDescriptorBase::TScalarAttributeDescriptorBase(
    const TDBField* field,
    bool alwaysScheduleLoadOnStore)
    : Field(field)
    , ScheduleLoadOnStore(alwaysScheduleLoadOnStore)
{ }

TScalarAttributeBase* TScalarAttributeDescriptorBase::AttributeBaseGetter(TObject* /*object*/) const
{
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

TObjectExistenceChecker::TObjectExistenceChecker(TObject* object)
    : Object_(object)
{ }

TObject* TObjectExistenceChecker::GetObject() const
{
    return Object_;
}

void TObjectExistenceChecker::ScheduleCheck(TObjectKey parentKey) const
{
    auto* this_ = const_cast<TObjectExistenceChecker*>(this);

    if (this_->LoadState_ >= NDetail::EAttributeLoadState::Scheduled) {
        if (parentKey && !ParentCheckScheduled_) {
            // This makes sure the check in InitializeInstantiated is first.
            THROW_ERROR_EXCEPTION(
                "Provided parent key %v for the existence check of %v "
                "but a check without a parent key is already scheduled",
                parentKey,
                Object_->GetDisplayName(/*keyOnly*/ true));
        }
        return;
    }

    this_->LoadState_ = NDetail::EAttributeLoadState::Scheduled;
    if (parentKey) {
        this_->ParentCheckScheduled_ = true;
    }

    Object_->GetSession()->ScheduleLoad(
        [this_, parentKey = std::move(parentKey)] (ILoadContext* context) {
            this_->LoadFromDB(context, std::move(parentKey));
        },
        ELoadPriority::Parent);
}

bool TObjectExistenceChecker::Check(TObjectKey parentKey, std::source_location location) const
{
    ScheduleCheck(std::move(parentKey));
    if (LoadState_ == NDetail::EAttributeLoadState::Scheduled) {
        Object_->GetSession()->FlushLoads(location);
    }
    THROW_ERROR_EXCEPTION_IF(LoadState_ != NDetail::EAttributeLoadState::Loaded,
        "Expected %Qlv state of existence check, but got %Qlv for %v",
        NDetail::EAttributeLoadState::Loaded,
        LoadState_,
        Object_->GetDisplayName(/*keyOnly*/ true));

    return ExistsOrError_.ValueOrThrow();
}

void TObjectExistenceChecker::LoadFromDB(ILoadContext* context, TObjectKey parentKey)
{
    switch (LoadState_) {
        case NDetail::EAttributeLoadState::None:
        case NDetail::EAttributeLoadState::Loaded:
            YT_ABORT();
        case NDetail::EAttributeLoadState::Scheduled:
            LoadState_ = NDetail::EAttributeLoadState::Loading;
            break;
        case NDetail::EAttributeLoadState::Loading:
            THROW_ERROR_EXCEPTION("Unexpected recursive call during existence check");
    }

    auto* typeHandler = Object_->GetTypeHandler();
    if (!typeHandler->HasParent()) {
        if (parentKey) {
            LoadState_ = NDetail::EAttributeLoadState::Loaded;
            ExistsOrError_ = TError(
                "%v does not have a parent but was provided a parent key %v",
                Object_->GetDisplayName(/*keyOnly*/ true),
                parentKey);
            return;
        }

        const auto* table = typeHandler->GetTable();
        auto handler = [this] (const auto& optionalValues) {
            YT_VERIFY(LoadState_ != NDetail::EAttributeLoadState::Loaded);
            LoadState_ = NDetail::EAttributeLoadState::Loaded;
            ExistsOrError_ = optionalValues && (*optionalValues)[1].Type == EValueType::Null;
        };
        context->ScheduleLookup(
            table,
            Object_->GetKey(),
            std::array{
                // XXX(babenko): creation time is only needed to work around the bug in write ts
                &ObjectsTable.Fields.MetaCreationTime,
                &ObjectsTable.Fields.MetaRemovalTime,
            },
            std::move(handler));
        return;
    }

    const auto* typedParentsTable = Object_->GetTypeHandler()->GetParentsTable();
    EParentsTableMode parentsMode = typedParentsTable
        ? Object_->GetSession()->GetConfigsSnapshot().ObjectManager->GetParentsTableMode(Object_->GetType())
        : EParentsTableMode::NoSeparateTable;

    if (parentsMode >= EParentsTableMode::ReadSeparateTable) {
        auto handler = [this, parentKey = std::move(parentKey)] (const auto& optionalValues) {
            YT_VERIFY(LoadState_ != NDetail::EAttributeLoadState::Loaded);
            LoadState_ = NDetail::EAttributeLoadState::Loaded;
            if (!optionalValues) {
                ExistsOrError_ = false;
                return;
            }

            if (parentKey) {
                try {
                    auto oldParentKey = ValuesToKey(*optionalValues);
                    if (parentKey != oldParentKey) {
                        THROW_ERROR_EXCEPTION(
                            "Provided parent key %v is different from existing %v",
                            parentKey,
                            oldParentKey);
                    }
                } catch (const std::exception& ex) {
                    ExistsOrError_ = TError("Failed to check the parent key for %v",
                        Object_->GetDisplayName(/*keyOnly*/ true))
                        << ex;
                    return;
                }
            }

            ExistsOrError_ = true;
        };
        context->ScheduleLookup(
            typedParentsTable,
            Object_->GetKey(),
            Object_->GetTypeHandler()->GetParentKeyFields(),
            std::move(handler));
    } else {
        const auto* parentsTable = Object_->GetTypeHandler()->GetBootstrap()->GetParentsTable();
        if (!parentsTable) {
            return;
        }
        auto handler = [this, parentKey = std::move(parentKey)] (const auto& optionalValues) {
            YT_VERIFY(LoadState_ != NDetail::EAttributeLoadState::Loaded);
            LoadState_ = NDetail::EAttributeLoadState::Loaded;
            if (!optionalValues) {
                ExistsOrError_ = false;
                return;
            }

            if (parentKey) {
                try {
                    TString serializedParentKey;
                    FromUnversionedValue(&serializedParentKey, (*optionalValues)[0]);
                    auto oldParentKey = ParseObjectKey(
                        serializedParentKey,
                        Object_->GetTypeHandler()->GetParentKeyFields());
                    if (parentKey != oldParentKey) {
                        THROW_ERROR_EXCEPTION(
                            "Provided parent key %v is different from existing %v",
                            parentKey,
                            oldParentKey);
                    }
                } catch (const std::exception& ex) {
                    ExistsOrError_ = TError("Failed to check the parent key for %v",
                        Object_->GetDisplayName(/*keyOnly*/ true))
                        << ex;
                    return;
                }
            }

            ExistsOrError_ = true;
        };
        context->ScheduleLookup(
            parentsTable,
            TObjectKey(Object_->GetKey().ToString(), Object_->GetType()),
            std::array{&parentsTable->Fields.ParentId},
            std::move(handler));
    }
}

////////////////////////////////////////////////////////////////////////////////

TFinalizationChecker::TFinalizationChecker(TObject* object)
    : Object_(object)
{ }

void TFinalizationChecker::ScheduleCheck() const
{
    if (LoadState_ >= NDetail::EAttributeLoadState::Scheduled) {
        return;
    }
    LoadState_ = NDetail::EAttributeLoadState::Scheduled;
    Object_->FinalizationStartTime().ScheduleLoad();
    Object_->GetSession()->ScheduleLoad(
        [this] (ILoadContext* /*context*/) {
            LoadState_ = NDetail::EAttributeLoadState::Loading;
            if (Object_->DidExist() && Object_->FinalizationStartTime().Load() != 0) {
                Object_->SetState(EObjectState::Finalized);
            }
            LoadState_ = NDetail::EAttributeLoadState::Loaded;
        },
        ELoadPriority::View);
}

void TFinalizationChecker::FlushCheck() const
{
    if (LoadState_ == NDetail::EAttributeLoadState::Scheduled) {
        Object_->GetSession()->FlushLoads();
    }
}

////////////////////////////////////////////////////////////////////////////////

TObjectTombstoneChecker::TObjectTombstoneChecker(const TObject* object)
    : Object_(object)
{ }

void TObjectTombstoneChecker::ScheduleCheck() const
{
    auto* this_ = const_cast<TObjectTombstoneChecker*>(this);
    if (LoadState_ >= NDetail::EAttributeLoadState::Scheduled) {
        return;
    }
    LoadState_ = NDetail::EAttributeLoadState::Scheduled;
    Object_->GetSession()->ScheduleLoad(
        [this_] (ILoadContext* context) {
            this_->LoadFromDB(context);
        });
}

bool TObjectTombstoneChecker::Check(std::source_location location) const
{
    ScheduleCheck();
    if (LoadState_ == NDetail::EAttributeLoadState::Scheduled) {
        Object_->GetSession()->FlushLoads(location);
    }
    THROW_ERROR_EXCEPTION_IF(LoadState_ != NDetail::EAttributeLoadState::Loaded,
        "Expected %Qlv state of tombstone check, but got %Qlv for %v",
        NDetail::EAttributeLoadState::Loaded,
        LoadState_,
        Object_->GetDisplayName(/*keyOnly*/ true));
    return Tombstone_;
}

void TObjectTombstoneChecker::LoadFromDB(ILoadContext* /*context*/)
{
    switch (LoadState_) {
        case NDetail::EAttributeLoadState::None:
        case NDetail::EAttributeLoadState::Loaded:
            YT_ABORT();
        case NDetail::EAttributeLoadState::Scheduled:
            LoadState_ = NDetail::EAttributeLoadState::Loading;
            break;
        case NDetail::EAttributeLoadState::Loading:
            THROW_ERROR_EXCEPTION("Unexpected recursive call during tombstone check");
    }

    Object_->GetSession()->ScheduleLoad(
        [this] (ILoadContext* context) {
            TObjectKey typedKey(Object_->GetKey().ToString(), Object_->GetType());
            auto handler = [this] (const auto& optionalValues) {
                YT_VERIFY(LoadState_ != NDetail::EAttributeLoadState::Loaded);
                LoadState_ = NDetail::EAttributeLoadState::Loaded;
                Tombstone_ = optionalValues.has_value();
            };
            context->ScheduleLookup(
                &TombstonesTable,
                typedKey,
                // Not actually used, just for better diagnostics.
                std::array{&TombstonesTable.Fields.RemovalTime},
                std::move(handler));
        });
}

////////////////////////////////////////////////////////////////////////////////

TAttributeBase::TAttributeBase(TObject* owner)
    : Owner_(owner)
{
    Owner_->RegisterAttribute(this);
}

const TObject* TAttributeBase::GetOwner() const
{
    return Owner_;
}

TObject* TAttributeBase::GetOwner()
{
    return Owner_;
}

bool TAttributeBase::IsStoreScheduled() const
{
    return StoreScheduled_;
}

void TAttributeBase::DoScheduleLoad(ELoadPriority priority) const
{
    auto this_ = const_cast<TAttributeBase*>(this);
    if (this_->LoadScheduled_) {
        return;
    }
    this_->LoadScheduled_ = true;
    Owner_->GetSession()->ScheduleLoad(
        [this_] (ILoadContext* context) {
            this_->LoadScheduled_ = false;
            this_->LoadFromDB(context);
        },
        priority);
}

void TAttributeBase::DoScheduleStore() const
{
    auto this_ = const_cast<TAttributeBase*>(this);
    if (this_->StoreScheduled_) {
        return;
    }
    this_->StoreScheduled_ = true;
    Owner_->ScheduleStore();
    Owner_->GetSession()->ScheduleStore(
        [this_] (IStoreContext* context) {
            this_->StoreScheduled_ = false;
            this_->StoreToDB(context);
        });
}

void TAttributeBase::DoScheduleLock() const
{
    auto this_ = const_cast<TAttributeBase*>(this);
    if (this_->LockScheduled_) {
        return;
    }
    this_->LockScheduled_ = true;
    Owner_->ScheduleStore();
    Owner_->GetSession()->ScheduleStore(
        [this_] (IStoreContext* context) {
            this_->LockScheduled_ = false;
            this_->LockInDB(context);
        });
}

void TAttributeBase::LoadFromDB(ILoadContext* /*context*/)
{ }

void TAttributeBase::StoreToDB(IStoreContext* /*context*/)
{ }

void TAttributeBase::LockInDB(IStoreContext* /*context*/)
{ }

void TAttributeBase::OnObjectInitialization()
{ }

void TAttributeBase::PreloadObjectRemoval() const
{ }

void TAttributeBase::CheckObjectRemoval() const
{ }

void TAttributeBase::OnObjectRemovalStart()
{ }

void TAttributeBase::OnObjectRemovalFinish()
{ }

////////////////////////////////////////////////////////////////////////////////

TParentKeyAttribute::TParentKeyAttribute(TObject* owner, TObjectKey parentKey)
    : TAttributeBase(owner)
    , ParentKey_(std::move(parentKey))
{
    YT_VERIFY(Owner_->GetTypeHandler()->HasParent());
    if (!ParentKey_) {
        DoScheduleLoad(ELoadPriority::Parent);
    }
}

const TObjectKey& TParentKeyAttribute::GetKey(std::source_location location) const
{
    if (!ParentKey_ && !Missing_) {
        Owner_->GetSession()->FlushLoads(location);
        YT_VERIFY(ParentKey_ || Missing_);
    }

    if (Missing_) {
        THROW_ERROR_EXCEPTION(
            NClient::EErrorCode::NoSuchObject,
            "%v is missing",
            Owner_->GetDisplayName(/*keyOnly*/ true));
    }

    return ParentKey_;
}

void TParentKeyAttribute::ScheduleParentLoad()
{
    if (!ParentKey_ && !Missing_) {
        PreloadParentOnKeyLoad_ = true;
    } else if (ParentKey_) {
        PreloadParentObject();
    }
}

void TParentKeyAttribute::LoadFromDB(ILoadContext* context)
{
    YT_VERIFY(!ParentKey_);
    YT_VERIFY(!Missing_);

    const auto* typedParentsTable = Owner_->GetTypeHandler()->GetParentsTable();
    EParentsTableMode parentsMode = typedParentsTable
        ? Owner_->GetSession()->GetConfigsSnapshot().ObjectManager->GetParentsTableMode(
            Owner_->GetType())
        : EParentsTableMode::NoSeparateTable;

    if (parentsMode >= EParentsTableMode::ReadSeparateTable) {
        auto handler = [this] (const auto& optionalValues) {
            if (optionalValues) {
                YT_VERIFY(optionalValues->Size() == Owner_->GetTypeHandler()->GetParentKeyFields().size());
                try {
                    ParentKey_ = ValuesToKey(*optionalValues);
                    YT_LOG_DEBUG("Object parent resolved (ObjectKey: %v, ParentKey: %v)",
                        Owner_->GetKey(),
                        ParentKey_);
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Error loading parent key for %v",
                        Owner_->GetDisplayName(/*keyOnly*/ true))
                        << ex;
                }
                if (PreloadParentOnKeyLoad_) {
                    PreloadParentObject();
                }
            } else {
                Missing_ = true;
            }
        };
        context->ScheduleLookup(
            typedParentsTable,
            Owner_->GetKey(),
            Owner_->GetTypeHandler()->GetParentKeyFields(),
            std::move(handler));
        return;
    }

    const auto* parentsTable = GetOwner()->GetTypeHandler()->GetBootstrap()->GetParentsTable();
    THROW_ERROR_EXCEPTION_UNLESS(parentsTable,
        "Reading from unavailable common parents table for %v",
        Owner_->GetDisplayName(/*keyOnly*/ true));

    auto handler = [this] (const auto& optionalValues) {
        if (optionalValues) {
            YT_VERIFY(optionalValues->Size() == 1);
            try {
                TString serializedKey;
                FromUnversionedValue(&serializedKey, (*optionalValues)[0]);
                YT_LOG_DEBUG("Object parent resolved (ObjectKey: %v, ObjectType: %v, ParentKey: %v)",
                    Owner_->GetKey(),
                    NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(Owner_->GetType()),
                    serializedKey);
                ParentKey_ = ParseObjectKey(
                    serializedKey,
                    Owner_->GetTypeHandler()->GetParentKeyFields());
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error loading parent key for %v",
                    Owner_->GetDisplayName(/*keyOnly*/ true))
                    << ex;
            }
            if (PreloadParentOnKeyLoad_) {
                PreloadParentObject();
            }
        } else {
            Missing_ = true;
        }
    };
    context->ScheduleLookup(
        parentsTable,
        TObjectKey(Owner_->GetKey().ToString(), Owner_->GetType()),
        std::array{&parentsTable->Fields.ParentId},
        std::move(handler));
}

void TParentKeyAttribute::PreloadParentObject() const
{
    THROW_ERROR_EXCEPTION_IF(!ParentKey_,
        "Cannot load parent object for %v because parent key is missing",
        Owner_->GetDisplayName(/*keyOnly*/ true));

    Owner_->GetSession()->GetObject(Owner_->GetTypeHandler()->GetParentType(), ParentKey_);
}

////////////////////////////////////////////////////////////////////////////////

TChildrenAttributeBase::TChildrenAttributeBase(TObject* owner)
    : TAttributeBase(owner)
{ }

void TChildrenAttributeBase::ScheduleLoad() const
{
    DoScheduleLoad();
}

const THashSet<TObject*>& TChildrenAttributeBase::UntypedLoad(std::source_location location) const
{
    ScheduleLoad();
    if (!Children_) {
        Owner_->GetSession()->FlushLoads(location);
    }
    THROW_ERROR_EXCEPTION_UNLESS(Children_,
        "Expected children attribute to be loaded for %v",
        Owner_->GetDisplayName(/*keyOnly*/ true));
    return *Children_;
}

void TChildrenAttributeBase::DoAdd(TObject* child)
{
    AddedChildren_.insert(child);
    RemovedChildren_.erase(child);
    if (Children_) {
        Children_->insert(child);
    }
}

void TChildrenAttributeBase::DoRemove(TObject* child)
{
    RemovedChildren_.insert(child);
    AddedChildren_.erase(child);
    if (Children_) {
        Children_->erase(child);
    }
}

void TChildrenAttributeBase::OnObjectInitialization()
{
    Children_.emplace();
}

void TChildrenAttributeBase::LoadFromDB(ILoadContext* context)
{
    if (Children_) {
        return;
    }

    auto childrenType = GetChildrenType();
    auto* session = Owner_->GetSession();
    auto* childrenTypeHandler = session->GetTypeHandlerOrCrash(childrenType);
    const auto& childrenKeyFields = childrenTypeHandler->GetKeyFields();

    TStringBuilder builder = BuildSelectQuery(
        context->GetTablePath(childrenTypeHandler->GetTable()),
        childrenKeyFields,
        Owner_->GetKey(),
        childrenTypeHandler->GetParentKeyFields());

    builder.AppendFormat(" and is_null(%v)", FormatId(ObjectsTable.Fields.MetaRemovalTime.Name));

    context->ScheduleSelect(
        builder.Flush(),
        [=, this] (const IUnversionedRowsetPtr& rowset) {
            YT_VERIFY(!Children_);
            auto rows = rowset->GetRows();
            Children_.emplace();
            Children_->reserve(rows.Size());
            for (auto row : rows) {
                TObjectKey childKey;
                FromUnversionedRow(row, &childKey, childrenKeyFields.size());
                auto* child = Owner_->GetSession()->GetObject(childrenType, childKey, Owner_->GetKey());
                Children_->insert(child);
            }
            for (auto* object : AddedChildren_) {
                Children_->insert(object);
            }
            for (auto* object : RemovedChildren_) {
                Children_->erase(object);
            }
        });
}

////////////////////////////////////////////////////////////////////////////////

TScalarAttributeBase::TScalarAttributeBase(TObject* owner, const TScalarAttributeDescriptorBase* descriptor)
    : TAttributeBase(owner)
    , Descriptor_(descriptor)
{ }

void TScalarAttributeBase::ScheduleLoad() const
{
    if (LoadState_.Value != NDetail::EAttributeLoadState::None) {
        return;
    }
    LoadState_.Value = NDetail::EAttributeLoadState::Scheduled;

    DoScheduleLoad();
}

void TScalarAttributeBase::ScheduleLoadTimestamp() const
{
    if (LoadState_.Timestamp != NDetail::EAttributeLoadState::None) {
        return;
    }
    LoadState_.Timestamp = NDetail::EAttributeLoadState::Scheduled;

    DoScheduleLoad();
}

bool TScalarAttributeBase::HasChangesForStoringToDB() const
{
    return true;
}

void TScalarAttributeBase::UpdateSharedWriteLockFlag(bool sharedWrite)
{
    auto oldValue = SharedWrite_;
    SharedWrite_ = SharedWrite_.has_value()
        ? *SharedWrite_ && sharedWrite
        : sharedWrite;
    if (oldValue.has_value() && oldValue != SharedWrite_) {
        YT_LOG_DEBUG("Updating shared write flag (Type: %v, Key: %v, FieldName: %v, Old: %v, New: %v)",
            NClient::NObjects::GetGlobalObjectTypeRegistry()->GetTypeNameByValueOrCrash(Owner_->GetType()),
            Owner_->GetKey(),
            Descriptor_->Field->Name,
            oldValue,
            SharedWrite_);
    }
}

TTimestamp TScalarAttributeBase::LoadTimestamp(std::source_location location) const
{
    switch (Owner_->GetState()) {
        case EObjectState::Instantiated:
        case EObjectState::Finalizing:
        case EObjectState::Finalized:
        case EObjectState::Removing:
        case EObjectState::Removed:
            OnLoadTimestamp(location);
            return Timestamp_;

        case EObjectState::Creating:
        case EObjectState::Created:
        case EObjectState::CreatedRemoving:
        case EObjectState::CreatedRemoved: {
            return NullTimestamp;
        }

        case EObjectState::Unknown:
            YT_ABORT();
    }
}

void TScalarAttributeBase::BeforeStorePreload() const
{
    for (auto* observer : IndexedStoreObservers_) {
        observer->Preload();
    }
    for (auto* observer : PredicateStoreObservers_) {
        observer->Preload();
    }
}

void TScalarAttributeBase::Lock(NTableClient::ELockType lockType)
{
    if (NTableClient::GetStrongestLock(LockType_, lockType) != LockType_) {
        LockType_ = lockType;
        DoScheduleLock();
    }
}

const TScalarAttributeDescriptorBase* TScalarAttributeBase::GetDescriptor() const
{
    return Descriptor_;
}

void TScalarAttributeBase::ScheduleStore()
{
    DoScheduleStore();
}

void TScalarAttributeBase::OnLoad(std::source_location location) const
{
    ScheduleLoad();

    DoOnLoad(LoadState_.Value, location);
}

void TScalarAttributeBase::OnLoadTimestamp(std::source_location location) const
{
    ScheduleLoadTimestamp();

    DoOnLoad(LoadState_.Timestamp, location);
}

void TScalarAttributeBase::DoOnLoad(const NDetail::EAttributeLoadState& loadState, std::source_location location) const
{
    if (loadState == NDetail::EAttributeLoadState::Scheduled) {
        Owner_->GetSession()->FlushLoads(location);
    }

    THROW_ERROR_EXCEPTION_IF(loadState != NDetail::EAttributeLoadState::Loaded,
        "Unexpected state %Qlv of attribute %v for %v",
        loadState,
        Descriptor_->Field->Name,
        Owner_->GetDisplayName(/*keyOnly*/ true));

    if (!Owner_->DidExist() || Missing_) {
        THROW_ERROR_EXCEPTION(
            NClient::EErrorCode::NoSuchObject,
            "%v %v",
            Owner_->GetDisplayName(/*keyOnly*/ true),
            !Owner_->DidExist() ? "does not exist" : "is missing");
    }
}

void TScalarAttributeBase::OnStore(
    bool sharedWrite,
    std::source_location location)
{
    YT_VERIFY(!Owner_->IsRemoved());

    Owner_->ValidateNotFinalized(this, location);

    THROW_ERROR_EXCEPTION_IF(sharedWrite && (!IndexedStoreObservers_.empty() || !PredicateStoreObservers_.empty()),
        NClient::EErrorCode::InvalidRequestArguments,
        "%Qv indexed/predicate attribute %Qv cannot be updated with shared write lock",
        NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(Owner_->GetType()),
        Descriptor_->Field->Name);

    for (auto* observer : IndexedStoreObservers_) {
        observer->OnIndexedAttributeStore();
    }
    for (auto* observer : PredicateStoreObservers_) {
        observer->OnPredicateAttributeStore();
    }

    if (Descriptor_->ScheduleLoadOnStore) {
        ScheduleLoad();
    }

    UpdateSharedWriteLockFlag(sharedWrite);

    ScheduleStore();
}

void TScalarAttributeBase::LoadFromDB(ILoadContext* context)
{
    // State transitions for LoadState are explained in following table.
    // Ts  \ Val | None   | Scheduled | Loading | Loaded |
    // None      | ABORT  | UPDATE    | THROW   | RETURN |
    // Scheduled | UPDATE | UPDATE    | THROW   | UPDATE |
    // Loading   | THROW  | THROW     | THROW   | THROW  |
    // Loaded    | ABORT  | ABORT     | ABORT   | RETURN |
    using EState = NDetail::EAttributeLoadState;

    if (LoadState_.Value == EState::None && LoadState_.Timestamp == EState::None ||
        LoadState_.Value != EState::Loaded  && LoadState_.Timestamp == EState::Loaded)
    {
        YT_ABORT();
    } else if (LoadState_.Value == EState::Loading || LoadState_.Timestamp == EState::Loading) {
        THROW_ERROR_EXCEPTION(
            "Unexpected recursive call during scalar attribute load (LoadState - Value: %v, Timestamp %v)",
            LoadState_.Value,
            LoadState_.Timestamp);
    } else if (LoadState_.Value == EState::Loaded &&
        (LoadState_.Timestamp == EState::None || LoadState_.Timestamp == EState::Loaded))
    {
        return;
    } else {
        if (LoadState_.Value == EState::Scheduled) {
            LoadState_.Value = EState::Loading;
        }
        if (LoadState_.Timestamp == EState::Scheduled) {
            LoadState_.Timestamp = EState::Loading;
        }
    }

    auto* typeHandler = Owner_->GetTypeHandler();

    // NB: GetObjectTableKey will be retrieving object's parent and be raising
    // an exception in case the object does not exist. Let's prevent this from happening
    // by pre-checking object's existence.
    if (typeHandler->HasParent() && !Owner_->DidExist()) {
        LoadState_ = {
            .Value = EState::Loaded,
            .Timestamp = EState::Loaded,
        };
        Missing_ = true;
        return;
    }

    auto key = typeHandler->GetObjectTableKey(Owner_);
    const auto* table = typeHandler->GetTable();
    if (LoadState_.Timestamp == EState::Loading) {
        auto handler = [=, this] (const std::optional<TRange<TVersionedValue>>& optionalValues) {
            if (LoadState_.Timestamp == EState::Loaded) {
                return;
            }
            if (optionalValues) {
                YT_VERIFY(optionalValues->Size() == 1);
                try {
                    const auto& value = (*optionalValues)[0];
                    LoadOldValue(value, context);
                    Timestamp_ = value.Timestamp;
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Error loading value and timestamp of [%v.%v] for %v",
                        table->GetName(),
                        Descriptor_->Field->Name,
                        Owner_->GetDisplayName(/*keyOnly*/ true))
                        << ex;
                }
            } else {
                Missing_ = true;
            }
            LoadState_ = {
                .Value = EState::Loaded,
                .Timestamp = EState::Loaded,
            };
        };
        context->ScheduleVersionedLookup(
            table,
            key,
            std::array{Descriptor_->Field},
            std::move(handler));
    } else {
        YT_VERIFY(LoadState_.Value == EState::Loading);
        auto handler = [=, this] (const std::optional<TRange<TUnversionedValue>>& optionalValues) {
            if (LoadState_.Value == EState::Loaded) {
                return;
            }
            if (optionalValues) {
                YT_VERIFY(optionalValues->Size() == 1);
                try {
                    LoadOldValue((*optionalValues)[0], context);
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Error loading value of [%v.%v] for %v",
                        table->GetName(),
                        Descriptor_->Field->Name,
                        Owner_->GetDisplayName(/*keyOnly*/ true))
                        << ex;
                }
            } else {
                Missing_ = true;
            }
            LoadState_.Value = EState::Loaded;
        };
        context->ScheduleLookup(
            table,
            key,
            std::array{Descriptor_->Field},
            std::move(handler));
    }
}

void TScalarAttributeBase::StoreToDB(IStoreContext* context)
{
    if (Owner_->IsRemoved()) {
        return;
    }

    auto* typeHandler = Owner_->GetTypeHandler();
    auto key = typeHandler->GetObjectTableKey(Owner_);
    const auto* table = typeHandler->GetTable();

    if (typeHandler->SkipStoreWithoutChanges() && !HasChangesForStoringToDB()) {
        YT_LOG_DEBUG("Skipping storing scalar attribute value (TableName: %v, FieldName: %v)",
            table->GetName(),
            Descriptor_->Field->Name);
        return;
    }

    TUnversionedValue value;
    try {
        Descriptor_->RunFinalValueValidators(Owner_);
        StoreNewValue(&value, context);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error storing value of [%v.%v] for %v",
            table->GetName(),
            Descriptor_->Field->Name,
            Owner_->GetDisplayName(/*keyOnly*/ true))
            << ex;
    }

    YT_VERIFY(SharedWrite_);
    context->WriteRow(
        table,
        key,
        std::array{Descriptor_->Field},
        std::array{std::move(value)},
        *SharedWrite_);
}

void TScalarAttributeBase::LockInDB(IStoreContext* context)
{
    if (Owner_->IsRemoved()) {
        return;
    }
    if (LockType_ == ELockType::None) {
        return;
    }

    auto* typeHandler = Owner_->GetTypeHandler();
    auto key = typeHandler->GetObjectTableKey(Owner_);
    const auto* table = typeHandler->GetTable();

    context->LockRow(table, key, std::array{Descriptor_->Field}, LockType_);
}

void TScalarAttributeBase::OnObjectInitialization()
{
    SetDefaultValues();
    LoadState_ = {
        .Value = NDetail::EAttributeLoadState::Loaded,
        .Timestamp = NDetail::EAttributeLoadState::Loaded,
    };
    OnStore(/*sharedWrite*/ false, std::source_location::current());
}

void TScalarAttributeBase::RegisterStoreObserver(IIndexAttributeStoreObserver* observer, bool indexed) const
{
    if (indexed) {
        IndexedStoreObservers_.push_back(observer);
    } else {
        PredicateStoreObservers_.push_back(observer);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <>
TObjectKey::TKeyField TScalarAttribute<TInstant>::ToKeyField(
    const TInstant& value,
    const TString& suffix) const
{
    THROW_ERROR_EXCEPTION_UNLESS(suffix.empty(),
        "TInstant field cannot have a path suffix, but got %Qv",
        suffix);
    return value.MicroSeconds();
}

////////////////////////////////////////////////////////////////////////////////

TTimestampAttribute::TTimestampAttribute(TObject* owner, const TTimestampAttributeDescriptor* descriptor)
    : TScalarAttributeBase(owner, descriptor)
{ }

bool TTimestampAttribute::IsChanged(
    const NYPath::TYPath& path,
    std::source_location /*location*/) const
{
    THROW_ERROR_EXCEPTION_UNLESS(path.empty(),
        "Cannot collect changes at the non-empty path %v: timestamp attribute is not composite",
        path);
    return AttributeTouched_;
}

void TTimestampAttribute::Touch(bool sharedWrite)
{
    auto ownerState = Owner_->GetState();
    YT_VERIFY(ownerState != EObjectState::Removed && ownerState != EObjectState::CreatedRemoved);

    Owner_->ValidateExists();

    AttributeTouched_ = true;

    UpdateSharedWriteLockFlag(sharedWrite);

    ScheduleStore();
}

void TTimestampAttribute::SetDefaultValues()
{
    AttributeTouched_ = true;
    Timestamp_ = NTransactionClient::NullTimestamp;
}

void TTimestampAttribute::LoadOldValue(const TUnversionedValue& /*value*/, ILoadContext* /*context*/)
{ }

void TTimestampAttribute::StoreNewValue(TUnversionedValue* dbValue, IStoreContext* /*context*/)
{
    *dbValue = MakeUnversionedSentinelValue(EValueType::Null);
}

TObjectKey::TKeyField TTimestampAttribute::LoadAsKeyField(const TString& /*suffix*/) const
{
    THROW_ERROR_EXCEPTION("Timestamp attribute is not convertible to a key field");
}

TObjectKey::TKeyField TTimestampAttribute::LoadAsKeyFieldOld(const TString& /*suffix*/) const
{
    THROW_ERROR_EXCEPTION("Timestamp attribute is not convertible to a key field");
}

std::vector<TObjectKey::TKeyField> TTimestampAttribute::LoadAsKeyFields(
    const TString& /*suffix*/) const
{
    THROW_ERROR_EXCEPTION("Timestamp attribute is not convertible to a list of key fields");
}

std::vector<TObjectKey::TKeyField> TTimestampAttribute::LoadAsKeyFieldsOld(
    const TString& /*suffix*/) const
{
    THROW_ERROR_EXCEPTION("Timestamp attribute is not convertible to a list of key fields");
}

void TTimestampAttribute::StoreKeyField(TObjectKey::TKeyField, const TString& /*suffix*/)
{
    THROW_ERROR_EXCEPTION("Timestamp attribute cannot be stored from a key field");
}

void TTimestampAttribute::StoreKeyFields(
    TObjectKey::TKeyFields /*keyFields*/,
    const TString& /*suffix*/)
{
    THROW_ERROR_EXCEPTION("Timestamp attribute cannot be stored from key fields");
}

auto TTimestampAttribute::LoadAsProtos(bool /*old*/) const -> TConstProtosView
{
    return std::nullopt;
}

auto TTimestampAttribute::MutableLoadAsProtos() -> TProtosView
{
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

TAnyToManyAttributeBase::TAnyToManyAttributeBase(
    TObject* owner,
    const TAnyToManyAttributeDescriptorBase* descriptor)
    : TAttributeBase(owner)
    , Descriptor_(descriptor)
{ }

void TAnyToManyAttributeBase::ScheduleLoad() const
{
    DoScheduleLoad();
}

bool TAnyToManyAttributeBase::IsChanged() const
{
    return !AddedForeignObjects_.empty() || !RemovedForeignObjects_.empty();
}

const THashSet<TObject*>& TAnyToManyAttributeBase::UntypedLoad(std::source_location location, bool old) const
{
    ScheduleLoad();
    if (!ForeignObjects_) {
        Owner_->GetSession()->FlushLoads(location);
    }
    THROW_ERROR_EXCEPTION_UNLESS(ForeignObjects_ && OldForeignObjects_,
        "Expected foreign objects to be loaded for attribute(s) %v for %v",
        Descriptor_->PrimaryKeyFields,
        Owner_->GetDisplayName(/*keyOnly*/ true, location));
    return old ? *OldForeignObjects_ : *ForeignObjects_;
}

void TAnyToManyAttributeBase::DoAdd(TObject* many)
{
    AddedForeignObjects_.insert(many);
    RemovedForeignObjects_.erase(many);
    if (ForeignObjects_) {
        ForeignObjects_->insert(many);
    }
    DoScheduleStore();
}

void TAnyToManyAttributeBase::DoRemove(TObject* many)
{
    RemovedForeignObjects_.insert(many);
    AddedForeignObjects_.erase(many);
    if (ForeignObjects_) {
        ForeignObjects_->erase(many);
    }
    DoScheduleStore();
}

TObjectKey TAnyToManyAttributeBase::GetKey(TObject* object) const
{
    return Owner_->GetKey() + (Descriptor_->ForeignObjectTableKey
        ? object->GetTypeHandler()->GetObjectTableKey(object)
        : object->GetKey());
}

void TAnyToManyAttributeBase::OnObjectInitialization()
{
    ForeignObjects_.emplace();
    OldForeignObjects_.emplace();
}

void TAnyToManyAttributeBase::LoadFromDB(ILoadContext* context)
{
    if (ForeignObjects_) {
        return;
    }

    TStringBuilder builder = BuildSelectQuery(
        context->GetTablePath(Descriptor_->Table),
        Descriptor_->ForeignKeyFields,
        Owner_->GetKey(),
        Descriptor_->PrimaryKeyFields);

    context->ScheduleSelect(
        builder.Flush(),
        [this] (const IUnversionedRowsetPtr& rowset) {
            if (ForeignObjects_) {
                return;
            }
            auto rows = rowset->GetRows();
            ForeignObjects_.emplace();
            ForeignObjects_->reserve(rows.Size());
            for (auto row : rows) {
                YT_VERIFY(row.GetCount() == Descriptor_->ForeignKeyFields.size());
                TObjectKey foreignKey;
                FromUnversionedRow(row, &foreignKey, static_cast<size_t>(row.GetCount()));
                auto* foreignObject = Owner_->GetSession()->GetObject(
                    GetForeignObjectType(),
                    std::move(foreignKey));
                ScheduleLoadForeignObjectAttribute(foreignObject);
                ForeignObjects_->insert(foreignObject);
            }
            OldForeignObjects_.emplace(*ForeignObjects_);
            for (auto* object : AddedForeignObjects_) {
                ForeignObjects_->insert(object);
            }
            for (auto* object : RemovedForeignObjects_) {
                ForeignObjects_->erase(object);
            }
        });
}

void TAnyToManyAttributeBase::StoreToDB(IStoreContext* context)
{
    if ((AddedForeignObjects_ || RemovedForeignObjects_) && Owner_->DoesExist()) {
        auto* typeHandler = Owner_->GetTypeHandler();
        context->LockRow(
            typeHandler->GetTable(),
            typeHandler->GetObjectTableKey(Owner_),
            std::array{&ObjectsTable.Fields.ExistenceLock},
            ELockType::SharedStrong);
    }

    // NB: Only following states are possible for foreign object in a transaction:
    // added, removed, added+removed. In last case, `AddedForeignObjects_` and
    // `RemovedForeignObjects_` are empty. You can remove foreign object and then
    // add it with the same identifier. Therefore, removal is done before addition.
    for (auto* object : RemovedForeignObjects_) {
        context->DeleteRow(Descriptor_->Table, GetKey(object));
    }

    if (!Owner_->DoesExist() || Owner_->IsFinalized()) {
        YT_VERIFY(AddedForeignObjects_.empty());
    }
    for (auto* object : AddedForeignObjects_) {
        context->WriteRow(
            Descriptor_->Table,
            GetKey(object),
            std::array{&DummyField},
            std::array{MakeUnversionedSentinelValue(EValueType::Null)});
    }
}

////////////////////////////////////////////////////////////////////////////////

TAnnotationsAttribute::TAnnotationsAttribute(TObject* owner)
    : TAttributeBase(owner)
{ }

void TAnnotationsAttribute::ScheduleLoad(std::string_view key) const
{
    if (KeyToValue_.contains(key)) {
        return;
    }

    ScheduledLoadKeys_.emplace(key);
    DoScheduleLoad();
}

TYsonString TAnnotationsAttribute::Load(std::string_view key, std::source_location location) const
{
    ScheduleLoad(key);
    Owner_->GetSession()->FlushLoads(location);
    if (auto it = KeyToValue_.find(key); it != KeyToValue_.end()) {
        return it->second ? it->second : TYsonString();
    } else {
        THROW_ERROR_EXCEPTION("%v does not contain annotation with key %Qv",
            Owner_->GetDisplayName(/*keyOnly*/ true),
            key);
    }
}

bool TAnnotationsAttribute::Contains(std::string_view key, std::source_location location) const
{
    ScheduleLoad(key);
    Owner_->GetSession()->FlushLoads(location);
    auto it = KeyToValue_.find(key);
    return it != KeyToValue_.end() && it->second;
}

void TAnnotationsAttribute::ScheduleLoadTimestamp(std::string_view key) const
{
    if (KeyToTimestamp_.contains(key)) {
        return;
    }

    ScheduledLoadTimestampKeys_.emplace(key);
    DoScheduleLoad();
}

TTimestamp TAnnotationsAttribute::LoadTimestamp(std::string_view key, std::source_location location) const
{
    ScheduleLoadTimestamp(key);
    Owner_->GetSession()->FlushLoads(location);
    if (auto it = KeyToTimestamp_.find(key); it != KeyToTimestamp_.end()) {
        return it->second;
    } else {
        THROW_ERROR_EXCEPTION("%v does not contain annotation with key %Qv",
            Owner_->GetDisplayName(/*keyOnly*/ true),
            key);
    }
}

void TAnnotationsAttribute::ScheduleLoadAll() const
{
    if (ScheduledLoadAll_ >= EAnnotationsLoadAllState::KeyValues || LoadedAll_ >= EAnnotationsLoadAllState::KeyValues) {
        return;
    }
    ScheduledLoadAll_ = EAnnotationsLoadAllState::KeyValues;
    DoScheduleLoad();
}

std::vector<std::pair<std::string, TYsonString>> TAnnotationsAttribute::LoadAll(std::source_location location) const
{
    ScheduleLoadAll();
    Owner_->GetSession()->FlushLoads(location);
    YT_VERIFY(LoadedAll_ == EAnnotationsLoadAllState::KeyValues);
    std::vector<std::pair<std::string, TYsonString>> result;
    result.reserve(KeyToValue_.size());
    for (const auto& [key, value] : KeyToValue_) {
        if (value) {
            result.emplace_back(key, value);
        } else {
            // Key was deleted.
        }
    }
    return result;
}

void TAnnotationsAttribute::ScheduleLoadAllOldKeys() const
{
    if (ScheduledLoadAll_ >= EAnnotationsLoadAllState::Keys || LoadedAll_ >= EAnnotationsLoadAllState::Keys) {
        return;
    }
    ScheduledLoadAll_ = EAnnotationsLoadAllState::Keys;
    DoScheduleLoad();
}

const THashSet<std::string>& TAnnotationsAttribute::LoadAllOldKeys(std::source_location location) const
{
    ScheduleLoadAllOldKeys();
    Owner_->GetSession()->FlushLoads(location);
    YT_VERIFY(LoadedAll_ >= EAnnotationsLoadAllState::Keys);
    return OldKeys_;
}

void TAnnotationsAttribute::Store(
    std::string_view key,
    const TYsonString& value,
    std::source_location location)
{
    auto ownerState = Owner_->GetState();
    YT_VERIFY(ownerState != EObjectState::Removed && ownerState != EObjectState::CreatedRemoved);

    Owner_->ValidateExists(location);

    KeyToValue_[key] = value;
    ScheduledStoreKeys_.emplace(key);
    DoScheduleStore();
}

bool TAnnotationsAttribute::IsStoreScheduled(std::string_view key) const
{
    return ScheduledStoreKeys_.contains(key);
}

void TAnnotationsAttribute::LoadFromDB(ILoadContext* context)
{
    if (ScheduledLoadAll_ > LoadedAll_) {
        auto key = Owner_->GetKey().ToString();

        auto buildLoadAllQuery = [&] {
            TStringBuilder builder;
            builder.AppendString(
                FormatId(AnnotationsTable.Fields.Name.Name));
            if (ScheduledLoadAll_ == EAnnotationsLoadAllState::KeyValues) {
                builder.AppendString(Format(", %v",
                    FormatId(AnnotationsTable.Fields.Value.Name)));
            }
            builder.AppendString(Format(" from %v where %v = %v and %v = %v",
                FormatId(context->GetTablePath(&AnnotationsTable)),
                FormatId(AnnotationsTable.Fields.ObjectId.Name),
                FormatLiteralValue(key),
                FormatId(AnnotationsTable.Fields.ObjectType.Name),
                static_cast<int>(Owner_->GetType())));
            return builder.Flush();
        };

        context->ScheduleSelect(
            buildLoadAllQuery(),
            [this] (const IUnversionedRowsetPtr& rowset) {
                if (LoadedAll_ >= ScheduledLoadAll_) {
                    return;
                }
                auto rows = rowset->GetRows();
                for (auto row : rows) {
                    auto key = FromUnversionedValue<std::string>(row[0]);
                    if (ScheduledLoadAll_ == EAnnotationsLoadAllState::KeyValues) {
                        YT_VERIFY(row.GetCount() == 2);
                        auto value = FromUnversionedValue<TYsonString>(row[1]);
                        // Does not overwrite present value.
                        KeyToValue_.emplace(std::move(key), std::move(value));
                    }
                    OldKeys_.emplace(key);
                }
                LoadedAll_ = ScheduledLoadAll_;
                ScheduledLoadAll_ = EAnnotationsLoadAllState::None;
            });
    }

    auto versionedHandler = [this] (
        const THashSet<std::string>& scheduledKeys,
        std::string key,
        const std::optional<TRange<TVersionedValue>>& optionalValues)
    {
        if (!scheduledKeys.contains(key)) {
            return;
        }
        if (optionalValues) {
            YT_VERIFY(optionalValues->Size() == 1);
            const auto& value = (*optionalValues)[0];
            // Does not overwrite present value.
            KeyToValue_.emplace(key, FromUnversionedValue<TYsonString>(value));
            KeyToTimestamp_[key] = value.Timestamp;
        } else {
            KeyToValue_.emplace(key, TYsonString());
            KeyToTimestamp_[key] = NullTimestamp;
        }
        ScheduledLoadTimestampKeys_.erase(key);
        ScheduledLoadKeys_.erase(key);
    };
    for (const auto& attributeKey : ScheduledLoadTimestampKeys_) {
        auto key = TObjectKey(Owner_->GetKey().ToString(), Owner_->GetType(), attributeKey);
        context->ScheduleVersionedLookup(
            &AnnotationsTable,
            key,
            std::array{&AnnotationsTable.Fields.Value},
            std::bind_front(versionedHandler, ScheduledLoadTimestampKeys_, attributeKey));
    }

    auto notInTimestampRange = [this] (const auto& key) {
        return !ScheduledLoadTimestampKeys_.contains(key);
    };
    for (const auto& attributeKey : ScheduledLoadKeys_ | std::views::filter(notInTimestampRange)) {
        auto key = TObjectKey(Owner_->GetKey().ToString(), Owner_->GetType(), attributeKey);
        auto handler = [attributeKey, this] (const std::optional<TRange<TUnversionedValue>>& optionalValues) {
            if (!ScheduledLoadKeys_.contains(attributeKey)) {
                return;
            }
            if (optionalValues) {
                YT_VERIFY(optionalValues->Size() == 1);
                // Does not overwrite present value.
                KeyToValue_.emplace(attributeKey, FromUnversionedValue<TYsonString>((*optionalValues)[0]));
            } else {
                KeyToValue_.emplace(attributeKey, TYsonString());
            }
            ScheduledLoadKeys_.erase(attributeKey);
        };
        context->ScheduleLookup(
            &AnnotationsTable,
            key,
            std::array{&AnnotationsTable.Fields.Value},
            std::move(handler));
    }
}

void TAnnotationsAttribute::StoreToDB(IStoreContext* context)
{
    auto* typeHandler = Owner_->GetTypeHandler();
    if (Owner_->DoesExist()) {
        context->LockRow(
            typeHandler->GetTable(),
            typeHandler->GetObjectTableKey(Owner_),
            std::array{&ObjectsTable.Fields.ExistenceLock},
            ELockType::SharedStrong);
    }

    auto store = [&] (const std::string& attributeKey, const TYsonString& value) {
        // TODO(dgolear): Migrate to std::string.
        auto key = TObjectKey(Owner_->GetKey().ToString(), Owner_->GetType(), TString(attributeKey));
        if (value) {
            context->WriteRow(
                &AnnotationsTable,
                key,
                std::array{&AnnotationsTable.Fields.Value},
                ToUnversionedValues(context->GetRowBuffer(), value));
        } else {
            context->DeleteRow(&AnnotationsTable, key);
        }
    };

    if (Owner_->IsRemoved()) {
        for (const auto& key : LoadAllOldKeys()) {
            store(key, TYsonString());
        }
    } else {
        for (const auto& attributeKey : ScheduledStoreKeys_) {
            store(attributeKey, GetOrCrash(KeyToValue_, attributeKey));
        }
    }
}

void TAnnotationsAttribute::OnObjectInitialization()
{
    LoadedAll_ = EAnnotationsLoadAllState::KeyValues;
}

void TAnnotationsAttribute::PreloadObjectRemoval() const
{
    ScheduleLoadAllOldKeys();
}

void TAnnotationsAttribute::OnObjectRemovalFinish()
{
    DoScheduleStore();
}

////////////////////////////////////////////////////////////////////////////////

TIndexedAttributeDescriptor::TIndexedAttributeDescriptor(
    const TScalarAttributeDescriptorBase* descriptor,
    TYPath path)
    : AttributeDescriptor(descriptor)
    , Path(std::move(path))
{ }

TIndexAttribute::TIndexAttribute(
    const IScalarAttributeBaseHolder* holder,
    TString suffix,
    bool repeated)
    : Attribute(holder->GetAttributeForIndex())
    , Suffix(std::move(suffix))
    , Repeated(repeated)
{ }

////////////////////////////////////////////////////////////////////////////////

TScalarAttributeIndexDescriptor::TScalarAttributeIndexDescriptor(
    TString indexName,
    const TDBIndexTable* table,
    std::vector<TIndexedAttributeDescriptor> indexedAttributeDescriptors,
    std::vector<TIndexedAttributeDescriptor> predicateAttributeDescriptors,
    bool repeated,
    EIndexMode mode,
    std::optional<TPredicateInfo> predicateInfo)
    : IndexName(std::move(indexName))
    , Table(table)
    , IndexedAttributeDescriptors(std::move(indexedAttributeDescriptors))
    , PredicateAttributeDescriptors(std::move(predicateAttributeDescriptors))
    , Repeated(repeated)
    , Mode(mode)
{
    YT_VERIFY(!Repeated && IndexedAttributeDescriptors.size() >= 1 || Repeated && IndexedAttributeDescriptors.size() == 1);
    if (predicateInfo.has_value()) {
        auto& [expression, attributesToValidate] = *predicateInfo;
        YT_VERIFY(
            !PredicateAttributeDescriptors.empty() &&
            PredicateAttributeDescriptors.size() == attributesToValidate.size());
        ExtractAndValidateAttributes(expression, attributesToValidate);
        Predicate = NQuery::CreateFilterMatcher(std::move(expression), std::move(attributesToValidate));
    } else {
        Predicate = NQuery::CreateConstantFilterMatcher(true);
    }
}

const TDBField* TScalarAttributeIndexDescriptor::GetIndexFieldByAttribute(
    const TDBField* field,
    const TYPath& path) const
{
    for (size_t i = 0; i < IndexedAttributeDescriptors.size(); ++i) {
        if (IndexedAttributeDescriptors[i].AttributeDescriptor->Field == field &&
            IndexedAttributeDescriptors[i].Path == path)
        {
            return Table->IndexKey[i];
        }
    }
    return nullptr;
}

void TScalarAttributeIndexDescriptor::ExtractAndValidateAttributes(
    const TString& predicateExpression,
    const std::vector<TString>& attributesToValidate)
{
    THashSet<TString> attributes;
    try {
        NQuery::ExtractFilterAttributeReferences(predicateExpression, [&] (TString attribute) {
            attributes.insert(std::move(attribute));
        });
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Failed to extract attribute references from predicate (Predicate: %v)", predicateExpression);
        YT_ABORT();
    }
    YT_VERIFY(attributesToValidate.size() == attributes.size() &&
        std::all_of(attributesToValidate.begin(), attributesToValidate.end(), [&] (const auto& attribute) {
            return attributes.contains(attribute);
        }));
}

////////////////////////////////////////////////////////////////////////////////

TScalarAttributeIndexBase::TScalarAttributeIndexBase(
    const TScalarAttributeIndexDescriptor* descriptor,
    TObject* owner,
    std::vector<TIndexAttribute> indexedAttributes,
    std::vector<TIndexAttribute> predicateAttributes,
    bool unique)
    : IndexedAttributes_(std::move(indexedAttributes))
    , PredicateAttributes_(std::move(predicateAttributes))
    , Descriptor_(descriptor)
    , Owner_(owner)
    , Unique_(unique)
    , IndexMode_(Owner_->GetSession()->GetConfigsSnapshot().ObjectManager->TryGetIndexMode(
        Descriptor_->IndexName).value_or(Descriptor_->Mode))
{
    if (IndexMode_ == EIndexMode::Building || IndexMode_ == EIndexMode::Enabled) {
        Owner_->RegisterLifecycleObserver(this);
        for (const auto& indexedAttribute : IndexedAttributes_) {
            indexedAttribute.Attribute->RegisterStoreObserver(this, /*indexed*/ true);
        }
        for (const auto& predicateAttribute : PredicateAttributes_) {
            predicateAttribute.Attribute->RegisterStoreObserver(this, /*indexed*/ false);
        }
    }
}

void TScalarAttributeIndexBase::TouchIndex()
{
    ObjectTouched_ = true;
    ScheduleStore();
}

void TScalarAttributeIndexBase::OnObjectInitialization()
{
    ObjectCreated_ = true;
    ScheduleStore();
}

void TScalarAttributeIndexBase::PreloadObjectRemoval() const
{
    ScheduleLoad();
}

void TScalarAttributeIndexBase::CheckObjectRemoval() const
{ }

void TScalarAttributeIndexBase::OnObjectRemovalStart()
{ }

void TScalarAttributeIndexBase::OnObjectRemovalFinish()
{
    ObjectRemoved_ = true;
    ScheduleStore();
}

void TScalarAttributeIndexBase::Preload() const
{
    ScheduleLoad();
}

void TScalarAttributeIndexBase::OnIndexedAttributeStore()
{
    IndexedAttributesUpdated_ = true;
    ScheduleStore();
}

void TScalarAttributeIndexBase::OnPredicateAttributeStore()
{
    PredicateAttributesUpdated_ = true;
    ScheduleStore();
}

void TScalarAttributeIndexBase::ScheduleLoad() const
{
    for (const auto& indexedAttribute : IndexedAttributes_) {
        indexedAttribute.Attribute->ScheduleLoad();
    }
    for (const auto& predicateAttribute : PredicateAttributes_) {
        predicateAttribute.Attribute->ScheduleLoad();
    }
}

void TScalarAttributeIndexBase::ScheduleStore()
{
    if (StoreScheduled_) {
        return;
    }

    ScheduleLoad();
    StoreScheduled_ = true;

    // Wrap the following loads and stores into the ScheduleStore to ensure they will be
    // run after the transaction flush has been started:
    // - Indexed attribute loads will be finished before the following operations,
    //   which helps to prevent recursion during indexed attribute load: one call from
    //   the previous `ScheduleLoad` and another from the following `GetIndexKeysToAdd`.
    // - `GetIndexKeysToAdd` will be constant because no changes in a transaction happen
    //   after the transaction flush. So these keys can be preloaded for each object
    //   in the session and loaded together efficiently within one read phase.
    Owner_->GetSession()->ScheduleStore(
        [this] (IStoreContext*) {
            Owner_->GetSession()->ScheduleLoad(
                [this] (ILoadContext* context) {
                    // Pointer must be valid until transaction destruction.
                    auto* indexSession = Owner_->GetSession()->GetOrCreateIndexSession(Descriptor_->IndexName);
                    auto indexKeysToAdd = GetIndexKeysToAdd();
                    if (Unique_) {
                        for (const auto& indexKey : indexKeysToAdd) {
                            auto handler = [=] (const auto& optionalValues) {
                                indexSession->TryAddLoadedKey(indexKey, /*exists*/ optionalValues.has_value());
                            };
                            context->ScheduleLookup(
                                this->Descriptor_->Table,
                                indexKey,
                                this->Descriptor_->Table->IndexKey,
                                std::move(handler));
                        }
                    }
                    Owner_->GetSession()->ScheduleStore(
                        [this, indexSession, indexKeysToAdd = std::move(indexKeysToAdd)] (IStoreContext* context) {
                            YT_VERIFY(!AlreadyStored_);
                            YT_VERIFY(StoreScheduled_);
                            this->StoreScheduled_ = false;

                            auto* typeHandler = Owner_->GetTypeHandler();
                            auto objectKey = typeHandler->GetObjectTableKey(Owner_);

                            auto indexKeysToRemove = GetIndexKeysToRemove();

                            if (!indexKeysToAdd.empty() || !indexKeysToRemove.empty()) {
                                // Indexing field update and object remove (equivalent to write to meta.removal_time)
                                // do not intersect in their writesets, so can be committed concurrently and
                                // must be blocked manually.
                                if (Owner_->DoesExist()) {
                                    context->LockRow(
                                        typeHandler->GetTable(),
                                        objectKey,
                                        std::array{&ObjectsTable.Fields.ExistenceLock},
                                        ELockType::SharedStrong);
                                }

                                for (const auto& indexKey : indexKeysToRemove) {
                                    if (Unique_) {
                                        indexSession->RemoveUniqueKey(indexKey);
                                        context->DeleteRow(Descriptor_->Table, indexKey);
                                    } else {
                                        context->DeleteRow(Descriptor_->Table, indexKey + objectKey);
                                    }
                                }
                                for (const auto& indexKey : indexKeysToAdd) {
                                    if (Unique_) {
                                        indexSession->AddUniqueKeyOrThrow(indexKey, Descriptor_->IndexName);
                                        auto row = ToUnversionedRow(objectKey, context->GetRowBuffer());
                                        context->WriteRow(
                                            Descriptor_->Table,
                                            indexKey,
                                            Descriptor_->Table->ObjectTableKey,
                                            TRange(row.begin(), row.end()));
                                    } else {
                                        context->WriteRow(
                                            this->Descriptor_->Table,
                                            indexKey + objectKey,
                                            std::array{&DummyField},
                                            std::array{MakeUnversionedSentinelValue(EValueType::Null)});
                                    }
                                }
                            }
                            this->AlreadyStored_ = true;
                        });
                });
        });

}

bool TScalarAttributeIndexBase::DoesPredicateMatch(bool old) const
{
    std::vector<NYson::TYsonString> attributeYsons;
    attributeYsons.reserve(PredicateAttributes_.size());
    for (const auto& predicateAttribute : PredicateAttributes_) {
        if (predicateAttribute.Repeated) {
            auto oldValue = old
                ? predicateAttribute.Attribute->LoadAsKeyFieldsOld(predicateAttribute.Suffix)
                : predicateAttribute.Attribute->LoadAsKeyFields(predicateAttribute.Suffix);

            attributeYsons.push_back(NYTree::BuildYsonStringFluently().DoListFor(
                oldValue,
                [] (NYTree::TFluentList fluent, const auto& keyField) {
                    Visit(keyField, [&fluent] (const auto& field) {
                        fluent.Item().Value(field);
                    });
                }));
        } else {
            auto oldValue = old
                ? predicateAttribute.Attribute->LoadAsKeyFieldOld(predicateAttribute.Suffix)
                : predicateAttribute.Attribute->LoadAsKeyField(predicateAttribute.Suffix);

            attributeYsons.push_back(Visit(oldValue, [] (const auto& field) {
                return NYTree::BuildYsonStringFluently().Value(field);
            }));
        }
    }

    std::vector<NYT::NOrm::NQuery::TNonOwningAttributePayload> matchArgument(attributeYsons.begin(), attributeYsons.end());
    return Descriptor_->Predicate->Match(matchArgument)
        .ValueOrThrow();
}

bool TScalarAttributeIndexBase::OldPredicateValue() const
{
    if (!OldPredicateValue_.has_value()) {
        OldPredicateValue_ = DoesPredicateMatch(/*old*/ true);
    }
    return *OldPredicateValue_;
}

bool TScalarAttributeIndexBase::NewPredicateValue() const
{
    if (!NewPredicateValue_.has_value()) {
        NewPredicateValue_ = DoesPredicateMatch(/*old*/ false);
    }
    return *NewPredicateValue_;
}

////////////////////////////////////////////////////////////////////////////////

TScalarAttributeIndex::TScalarAttributeIndex(
    const TScalarAttributeIndexDescriptor* descriptor,
    TObject* owner,
    std::vector<TIndexAttribute> indexedAttributes,
    std::vector<TIndexAttribute> predicateAttributes,
    bool unique)
    : TScalarAttributeIndexBase(descriptor, owner, std::move(indexedAttributes), std::move(predicateAttributes), unique)
{
    YT_VERIFY(!Descriptor_->Repeated);
    YT_VERIFY(std::all_of(IndexedAttributes_.begin(), IndexedAttributes_.end(), [] (const auto& attribute) {
        return !attribute.Repeated;
    }));
}

TObjectKey TScalarAttributeIndex::GetIndexKey() const
{
    TObjectKey::TKeyFields keyFields;
    keyFields.reserve(IndexedAttributes_.size());
    for (const auto& indexedAttribute : IndexedAttributes_) {
        keyFields.push_back(indexedAttribute.Attribute->LoadAsKeyField(indexedAttribute.Suffix));
    }
    return TObjectKey(keyFields);
}

TObjectKey TScalarAttributeIndex::GetIndexKeyOld() const
{
    TObjectKey::TKeyFields keyFields;
    keyFields.reserve(IndexedAttributes_.size());
    for (const auto& indexedAttribute : IndexedAttributes_) {
        keyFields.push_back(indexedAttribute.Attribute->LoadAsKeyFieldOld(indexedAttribute.Suffix));
    }
    return TObjectKey(keyFields);
}

std::vector<TObjectKey> TScalarAttributeIndex::GetIndexKeysToRemove() const
{
    if (ObjectCreated_) {
        return {};
    }
    auto oldValue = GetIndexKeyOld();
    std::vector<TObjectKey> result;

    if (ObjectTouched_ ||
        IndexMode_ == EIndexMode::Building ||
        OldPredicateValue() && (
            ObjectRemoved_ ||
            IndexedAttributesUpdated_ && oldValue != GetIndexKey() ||
            PredicateAttributesUpdated_ && !NewPredicateValue()))
    {
        result.push_back(std::move(oldValue));
    }
    return result;
}

std::vector<TObjectKey> TScalarAttributeIndex::GetIndexKeysToAdd() const
{
    if (ObjectRemoved_ || !NewPredicateValue()) {
        return {};
    }
    auto newValue = GetIndexKey();
    std::vector<TObjectKey> result;

    if (ObjectTouched_ ||
        ObjectCreated_ ||
        IndexMode_ == EIndexMode::Building ||
        IndexedAttributesUpdated_ && newValue != GetIndexKeyOld() ||
        PredicateAttributesUpdated_  && !OldPredicateValue())
    {
        result.push_back(std::move(newValue));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TRepeatedScalarAttributeIndex::TRepeatedScalarAttributeIndex(
    const TScalarAttributeIndexDescriptor* descriptor,
    TObject* owner,
    std::vector<TIndexAttribute> indexedAttributes,
    std::vector<TIndexAttribute> predicateAttributes)
    : TScalarAttributeIndexBase(
        descriptor,
        owner,
        std::move(indexedAttributes),
        std::move(predicateAttributes),
        /*unique*/ false)
{
    YT_VERIFY(IndexedAttributes_.size() == 1);
    YT_VERIFY(Descriptor_->Repeated);
    YT_VERIFY(IndexedAttributes_[0].Repeated);
}

std::vector<TObjectKey> TRepeatedScalarAttributeIndex::GetIndexKeysToRemove() const
{
    if (ObjectCreated_) {
        return {};
    }

    THashSet<TObjectKey> itemsToRemove;
    const auto& indexedAttribute = IndexedAttributes_[0];
    const auto oldValue = indexedAttribute.Attribute->LoadAsKeyFieldsOld(indexedAttribute.Suffix);
    auto oldPredicateValue = OldPredicateValue();
    if (ObjectTouched_ ||
        IndexMode_ == EIndexMode::Building ||
        oldPredicateValue && (
            ObjectRemoved_ ||
            PredicateAttributesUpdated_ && !NewPredicateValue()))
    {
        itemsToRemove.reserve(oldValue.size());
        for (const auto& item : oldValue) {
            itemsToRemove.insert(TObjectKey({item}));
        }
    } else if (oldPredicateValue && IndexedAttributesUpdated_) {
        const auto newValue = indexedAttribute.Attribute->LoadAsKeyFields(indexedAttribute.Suffix);
        THashSet<TObjectKey> newValueSet;
        newValueSet.reserve(newValue.size());
        for (const auto& item : newValue) {
            newValueSet.insert(TObjectKey({item}));
        }
        for (const auto& item : oldValue) {
            const auto& itemValue = TObjectKey({item});
            if (newValueSet.find(itemValue) == newValueSet.end()) {
                itemsToRemove.insert(itemValue);
            }
        }
    }

    return {std::make_move_iterator(itemsToRemove.begin()), std::make_move_iterator(itemsToRemove.end())};
}

std::vector<TObjectKey> TRepeatedScalarAttributeIndex::GetIndexKeysToAdd() const
{
    if (ObjectRemoved_ || !NewPredicateValue()) {
        return {};
    }

    THashSet<TObjectKey> itemsToAdd;
    const auto& indexedAttribute = IndexedAttributes_[0];
    const auto newValue = indexedAttribute.Attribute->LoadAsKeyFields(indexedAttribute.Suffix);
    if (ObjectTouched_ ||
        ObjectCreated_ ||
        IndexMode_ == EIndexMode::Building ||
        PredicateAttributesUpdated_ && !OldPredicateValue())
    {
        for (const auto& item : newValue) {
            itemsToAdd.insert(TObjectKey({item}));
        }
    } else if (IndexedAttributesUpdated_) {
        const auto oldValue = indexedAttribute.Attribute->LoadAsKeyFieldsOld(indexedAttribute.Suffix);
        THashSet<TObjectKey> oldValueSet;
        for (const auto& item : oldValue) {
            oldValueSet.insert(TObjectKey({item}));
        }
        for (const auto& item : newValue) {
            const auto& itemValue = TObjectKey({item});
            if (oldValueSet.find(itemValue) == oldValueSet.end()) {
                itemsToAdd.insert(itemValue);
            }
        }
    }

    return {std::make_move_iterator(itemsToAdd.begin()), std::make_move_iterator(itemsToAdd.end())};
}

////////////////////////////////////////////////////////////////////////////////

TViewAttributeBase::TViewAttributeBase(const TObject* object)
    : Owner_(object)
{ }

TResolveAttributeResult TViewAttributeBase::ResolveViewAttribute(
    const TAttributeSchema* schema,
    const NYPath::TYPath& path,
    bool many) const
{
    if (auto it = ResolveResults_.find(path); it != ResolveResults_.end()) {
        return it->second;
    }

    auto adjustedPath = path;
    if (many) {
        if (!path.empty()) {
            THROW_ERROR_EXCEPTION_UNLESS(path.StartsWith("/*"),
                "Expected viewed path to start with \"/*\" for views on multiple objects, got %Qv",
                adjustedPath);
            adjustedPath = adjustedPath.substr(2);
        }
    }

    auto viewResolveResult = ResolveAttribute(
        Owner_
            ->GetTypeHandler()
            ->GetBootstrap()
            ->GetObjectManager()
            ->GetTypeHandlerOrThrow(schema->GetViewObjectType()),
        adjustedPath,
        /*callback*/ {},
        /*validateProtoSchemaCompliance*/ false);
    auto* viewAttribute = viewResolveResult.Attribute;
    THROW_ERROR_EXCEPTION_IF(viewAttribute->IsView(),
        "View %Qv cannot fetch another view %Qv",
        schema->GetPath(),
        viewAttribute->GetPath());

    THROW_ERROR_EXCEPTION_UNLESS(
        viewAttribute->TryAsComposite() || viewAttribute->TryAsScalar()->HasValueGetter(),
        "View %Qv cannot fetch attribute %Qv",
        schema->GetPath(),
        viewAttribute->GetPath());
    THROW_ERROR_EXCEPTION_IF(
        viewAttribute->TryAsComposite() && !viewResolveResult.SuffixPath.empty(),
        "View %Qv cannot fetch suffix path %Qv of composite attribute %Qv",
        schema->GetPath(),
        viewResolveResult.SuffixPath,
        viewAttribute->GetPath());

    ResolveResults_[path] = viewResolveResult;
    return viewResolveResult;
}

TResolveAttributeResult TManyToOneViewAttribute::ResolveViewAttribute(
    const TAttributeSchema* schema,
    const NYPath::TYPath& path) const
{
    return TViewAttributeBase::ResolveViewAttribute(schema, path, /*many*/ false);
}

TResolveAttributeResult TManyToManyInlineViewAttribute::ResolveViewAttribute(
    const TAttributeSchema* schema,
    const NYPath::TYPath& path) const
{
    return TViewAttributeBase::ResolveViewAttribute(schema, path, /*many*/ true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
