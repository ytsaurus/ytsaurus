#include "object.h"

#include "attribute_schema.h"
#include "db_config.h"
#include "db_schema.h"
#include "fqid.h"
#include "type_handler.h"
#include "private.h"

#include <yt/yt/orm/server/master/bootstrap.h>
#include <yt/yt/orm/server/master/config.h>

#include <yt/yt/orm/server/access_control/access_control_manager.h>

#include <yt/yt_proto/yt/orm/data_model/generic.pb.h>

#include <yt/yt/orm/client/objects/registry.h>

namespace NYT::NOrm::NServer::NObjects {

using namespace NClient::NObjects;

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeDescriptor<TObject, TInstant> TObject::CreationTimeDescriptor{
    &ObjectsTable.Fields.MetaCreationTime,
    [] (TObject* object) { return &object->CreationTime(); }
};

const TScalarAttributeDescriptor<TObject, NYTree::IMapNodePtr> TObject::LabelsDescriptor{
    &ObjectsTable.Fields.Labels,
    [] (TObject* object) { return &object->Labels(); }
};

const TScalarAttributeDescriptor<TObject, bool> TObject::InheritAclDescriptor{
    &ObjectsTable.Fields.MetaInheritAcl,
    [] (TObject* object) { return &object->InheritAcl(); }
};

const TScalarAttributeDescriptor<TObject, bool> TObject::ExistenceLockDescriptor{
    &ObjectsTable.Fields.ExistenceLock,
    [] (TObject* object) { return &object->ExistenceLock(); }
};

TObject::TObject(
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : CreationTime_(this, &CreationTimeDescriptor)
    , Labels_(this, &LabelsDescriptor)
    , InheritAcl_(this, &InheritAclDescriptor)
    , ExistenceLock_(this, &ExistenceLockDescriptor)
    , TypeHandler_(typeHandler)
    , Session_(session)
    , Annotations_(typeHandler->GetBootstrap()->GetDBConfig().EnableAnnotations
        ? std::optional<TAnnotationsAttribute>(this)
        : std::nullopt)
    , ExistenceChecker_(this)
    , FinalizationChecker_(typeHandler->GetBootstrap()->GetDBConfig().EnableFinalizers
        ? std::optional<TFinalizationChecker>(this)
        : std::nullopt)
    , TombstoneChecker_(typeHandler->GetBootstrap()->GetDBConfig().EnableTombstones
        ? std::optional<TObjectTombstoneChecker>(this)
        : std::nullopt)
{ }

void TObject::InitializeCreating(TObject* predecessor)
{
    Predecessor_ = predecessor;
    SetState(EObjectState::Creating);
}

void TObject::InitializeInstantiated(TObjectKey parentKey)
{
    SetState(EObjectState::Instantiated);
    ExistenceChecker_.ScheduleCheck(std::move(parentKey));
    if (FinalizationChecker_) {
        FinalizationChecker_->ScheduleCheck();
    }
}

TObjectKey TObject::GetParentKey(std::source_location /*location*/) const
{
    return {};
}

TParentKeyAttribute* TObject::GetParentKeyAttribute()
{
    return nullptr;
}

TString TObject::GetDisplayName(bool keyOnly, std::source_location location) const
{
    TString name;
    auto typeName = GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(GetType());
    if (GetTypeHandler()->IsObjectNameSupported() && !keyOnly) {
        try {
            name = GetName(location);
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(ex, "Suppressing object name load error (Key: %v)",
                GetKey());
        }
    }
    if (name) {
        return Format("%v %Qv (key %Qv)", typeName, name, GetKey());
    } else {
        return Format("%v %Qv", typeName, GetKey());
    }
}

TString TObject::GetFqid(std::source_location location) const
{
    return TFqid(GetTypeHandler()->GetBootstrap())
        .Type(GetType())
        .Key(GetKey())
        .Uuid(GetUuid(location))
        .Serialize();
}

IObjectTypeHandler* TObject::GetTypeHandler() const
{
    return TypeHandler_;
}

ISession* TObject::GetSession() const
{
    return Session_;
}

EObjectState TObject::GetState() const
{
    if (FinalizationChecker_) {
        FinalizationChecker_->FlushCheck();
    }
    return State_;
}

void TObject::SetState(EObjectState state)
{
    State_ = state;
}

const TAnnotationsAttribute& TObject::Annotations() const noexcept
{
    if (Y_UNLIKELY(!Annotations_)) {
        YT_ABORT();
    }
    return *Annotations_;
}

TAnnotationsAttribute& TObject::Annotations() noexcept
{
    if (Y_UNLIKELY(!Annotations_)) {
        YT_ABORT();
    }
    return *Annotations_;
}

void TObject::PrepareUuidValidation(std::string_view expected) const
{
    if (expected.empty()) {
        return;
    }

    ScheduleUuidLoad();
}

void TObject::ValidateUuid(std::string_view expected, std::source_location location) const
{
    if (expected.empty()) {
        return;
    }

    const auto& myUuid = GetUuid(location);
    if (expected != myUuid) {
        THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments,
            "Requested uuid %v does not match object uuid %v",
            expected,
            myUuid);
    }
}

void TObject::ScheduleMetaResponseLoad() const
{
    ScheduleUuidLoad();
    GetTypeHandler()->GetMetaAttributeSchema()->PreloadMetaResponseAttributes(this);
}

void TObject::ScheduleAccessControlLoad() const
{
    InheritAcl_.ScheduleLoad();
    ScheduleAclLoad();
    auto* typeHandler = GetTypeHandler();
    YT_VERIFY(typeHandler);
    if (auto* schema = typeHandler->GetSchemaObject(this)) {
        schema->ScheduleAccessControlLoad();
    }
    typeHandler->ScheduleAccessControlParentKeyLoad(this);
}

bool TObject::DoesExist(std::source_location location) const
{
    switch (GetState()) {
        case EObjectState::Instantiated:
            return ExistenceChecker_.Check(/*parentKey*/ {}, location);
        case EObjectState::Creating:
        case EObjectState::Created:
        case EObjectState::CreatedRemoving:
        case EObjectState::Finalizing:
        case EObjectState::Finalized:
        case EObjectState::Removing:
            return true;
        case EObjectState::Removed:
        case EObjectState::CreatedRemoved:
            return false;
        case EObjectState::Unknown:
            YT_ABORT();
        }
}

bool TObject::DidExist(std::source_location location) const
{
    switch (GetState()) {
        case EObjectState::Instantiated:
            return ExistenceChecker_.Check(/*parentKey*/ {}, location);
        case EObjectState::Finalizing:
        case EObjectState::Finalized:
        case EObjectState::Removing:
        case EObjectState::Removed:
            return true;
        case EObjectState::Creating:
        case EObjectState::Created:
        case EObjectState::CreatedRemoving:
        case EObjectState::CreatedRemoved:
            return false;
        case EObjectState::Unknown:
            YT_ABORT();
        }
}

void TObject::ValidateExists(std::source_location location) const
{
    if (!DoesExist(location)) {
        THROW_ERROR_EXCEPTION(NClient::EErrorCode::NoSuchObject,
            "%v does not exist",
            GetDisplayName())
            << TErrorAttribute("location", NYT::ToString(location));
    }
}

void TObject::ValidateNotFinalized(
    const IPersistentAttribute* updatedAttribute,
    std::source_location location) const
{
    ValidateExists(location);
    if (IsFinalized() &&
        updatedAttribute != &FinalizationStartTime() &&
        updatedAttribute != &Finalizers())
    {
        THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidObjectState,
            "Trying to update finalizing %v",
            GetDisplayName())
            << TErrorAttribute("location", NYT::ToString(location));
    }
}

void TObject::ScheduleTombstoneCheck()
{
    THROW_ERROR_EXCEPTION_UNLESS(TombstoneChecker_.has_value(),
        "Tombstones are disabled");
    TombstoneChecker_->ScheduleCheck();
}

bool TObject::IsTombstone(std::source_location location) const
{
    THROW_ERROR_EXCEPTION_UNLESS(TombstoneChecker_.has_value(),
        "Tombstones are disabled");
    return TombstoneChecker_->Check(location);
}

bool TObject::RemovalStarted() const
{
    return IsFinalizing() || IsFinalized() || IsRemoving() || IsRemoved();
}

bool TObject::IsFinalized() const
{
    return GetState() == EObjectState::Finalized;
}

bool TObject::IsRemoved() const
{
    auto state = GetState();
    return state == EObjectState::Removed || state == EObjectState::CreatedRemoved;
}

bool TObject::IsFinalizing() const
{
    return GetState() == EObjectState::Finalizing;
}

bool TObject::IsRemoving() const
{
    auto state = GetState();
    return state == EObjectState::Removing || state == EObjectState::CreatedRemoving;
}

void TObject::RegisterAttribute(IPersistentAttribute* attribute)
{
    RegisterLifecycleObserver(attribute);
}

void TObject::RegisterLifecycleObserver(IObjectLifecycleObserver* observer)
{
    LifecycleObservers_.push_back(observer);
}

void TObject::ScheduleStore()
{
    StoreScheduled_ = true;
}

bool TObject::IsStoreScheduled() const
{
    return StoreScheduled_;
}

TScalarAttribute<ui64>& TObject::FinalizationStartTime() noexcept
{
    YT_ABORT();
}

const TScalarAttribute<ui64>& TObject::FinalizationStartTime() const noexcept
{
    YT_ABORT();
}

TObject::TFinalizersAttribute& TObject::Finalizers() noexcept
{
    YT_ABORT();
}

const TObject::TFinalizersAttribute& TObject::Finalizers() const noexcept
{
    YT_ABORT();
}

bool TObject::HasActiveFinalizers() const
{
    return FinalizationChecker_.has_value() &&
        std::ranges::any_of(Finalizers().Load(), [] (const auto& finalizer) {
            return finalizer.second.completion_timestamp() == 0;
        });
}

int TObject::CountActiveFinalizers() const
{
    if (!FinalizationChecker_) {
        return 0;
    }
    return std::ranges::count_if(Finalizers().Load(), [] (const auto& finalizer) {
        return finalizer.second.completion_timestamp() == 0;
    });
}

bool TObject::IsFinalizerActive(TStringBuf finalizerName) const
{
    if (!FinalizationChecker_) {
        return false;
    }
    auto it = Finalizers().Load().find(finalizerName);
    return it != Finalizers().Load().end() && it->second.completion_timestamp() == 0;
}

void TObject::AddFinalizer(TStringBuf finalizerName, std::optional<std::string> ownerId)
{
    THROW_ERROR_EXCEPTION_UNLESS(FinalizationChecker_.has_value(),
        "Finalization is not enabled");
    auto& finalizers = *Finalizers().MutableLoad();
    THROW_ERROR_EXCEPTION_IF(finalizers.contains(finalizerName),
        NOrm::NClient::EErrorCode::InvalidRequestArguments,
        "Finalizer %Qv already exists for %v",
        finalizerName,
        GetDisplayName());

    NDataModel::TFinalizer protoFinalizer;
    // TODO(babenko): switch to std::string
    protoFinalizer.set_user_id(TString(ownerId
        ? *ownerId
        : NAccessControl::GetAuthenticatedUserIdentity().User));

    finalizers.emplace(finalizerName, protoFinalizer);
}

void TObject::CompleteFinalizer(TStringBuf finalizerName)
{
    THROW_ERROR_EXCEPTION_UNLESS(FinalizationChecker_.has_value(),
        "Finalization is not enabled");
    auto& finalizers = *Finalizers().MutableLoad();
    THROW_ERROR_EXCEPTION_UNLESS(finalizers.contains(finalizerName),
        NOrm::NClient::EErrorCode::InvalidRequestArguments,
        "No such finalizer %Qv for %v",
        finalizerName,
        GetDisplayName());

    auto& finalizer = finalizers[finalizerName];

    THROW_ERROR_EXCEPTION_UNLESS(finalizer.completion_timestamp() == 0,
        NOrm::NClient::EErrorCode::InvalidRequestArguments,
        "Finalizer %Qv is already completed for %v",
        finalizerName,
        GetDisplayName());

    finalizer.set_completion_timestamp(GetSession()->GetOwner()->GetStartTimestamp());
}

TScalarAttributeIndexBase* TObject::GetIndexOrThrow(const TString& indexName) const
{
    if (auto it = IndexPerName_.find(indexName); it == IndexPerName_.end()) {
        THROW_ERROR_EXCEPTION(NClient::EErrorCode::NoSuchIndex,
            "No such index %Qv", indexName);
    } else {
        return it->second.get();
    }
}

void TObject::RegisterScalarAttributeIndex(
    TString indexName,
    std::unique_ptr<TScalarAttributeIndexBase> index)
{
    EmplaceOrCrash(IndexPerName_, std::move(indexName), std::move(index));
}

void TObject::TouchRevisionTracker(const NYPath::TYPath& trackerPath)
{
    TouchedRevisionTrackers_.insert(trackerPath);
}

void TObject::ReconcileRevisionTrackers()
{
    for (const auto& trackerPath : TouchedRevisionTrackers_) {
        TypeHandler_->DoHandleRevisionTrackerUpdate(this, trackerPath);
    }
}

void TObject::MigrateAttribute(int migration)
{
    AttributeMigrations_.insert(migration);
}

void TObject::ForceMigrateAttribute(int migration)
{
    AttributeMigrations_.insert(migration);
    ForcedAttributeMigrations_.insert(migration);
}

void TObject::PrepareAttributeMigrations()
{
    if (AttributeMigrations_.empty()) {
        return;
    }
    TypeHandler_->DoPrepareAttributeMigrations(this, AttributeMigrations_, ForcedAttributeMigrations_);
}

void TObject::FinalizeAttributeMigrations()
{
    if (AttributeMigrations_.empty()) {
        return;
    }
    TypeHandler_->DoFinalizeAttributeMigrations(this, AttributeMigrations_, ForcedAttributeMigrations_);
    AttributeMigrations_.clear();
    ForcedAttributeMigrations_.clear();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
