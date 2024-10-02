#include "reference_attribute.h"

#include "object.h"
#include "session.h"
#include "type_handler.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

TReferenceAttributeBase::TReferenceAttributeBase(TObject* owner)
{
    owner->RegisterAttribute(this);
}

TObject* TReferenceAttributeBase::GetOwner() const
{
    return DoGetOwner();
}

void TReferenceAttributeBase::ScheduleLoad(bool forPartialStore) const
{
    GetKeyStorageDriver().ScheduleLoad(forPartialStore);
}

bool TReferenceAttributeBase::IsStoreScheduled() const
{
    return ReconcileState_ == EOS_SCHEDULED || GetKeyStorageDriver().IsStoreScheduled();
}

bool TReferenceAttributeBase::IsChanged() const
{
    return GetKeyStorageDriver().IsChanged();
}

void TReferenceAttributeBase::Lock(NTableClient::ELockType lockType)
{
    GetKeyStorageDriver().Lock(lockType);
}

void TReferenceAttributeBase::OnObjectInitialization()
{ }

void TReferenceAttributeBase::PreloadObjectRemoval() const
{
    ScheduleLoad();
    GetOwner()->GetSession()->ScheduleLoad(
        [this] (ILoadContext*) {
            BeforeLoad();
            PreloadForeignKeyUpdate();
        },
        ELoadPriority::View);
}

void TReferenceAttributeBase::OnObjectRemovalFinish()
{ }

void TReferenceAttributeBase::BeforeLoad() const
{
    // Cache operations are not reentrant; this could throw off the state machine.
    THROW_ERROR_EXCEPTION_IF(CacheIsBusy(), "Called BeforeLoad() on a busy cache");

    switch (CacheState_) {
        case ECS_UNLOADED:
            LoadCache();
            CacheState_ = ECS_CURRENT;
            break;
        case ECS_CURRENT:
            break;
        case ECS_DIRTY:
            ApplyKeyChanges();
            CacheState_ = ECS_CURRENT;
            break;
        case ECS_TAINTED:
            ApplyKeyChanges();
            break;
    }
}

void TReferenceAttributeBase::BeforeKeyStore() const
{
    if (CacheIsBusy()) {
        return; // Ignore self-induced notification.
    }

    switch (CacheState_) {
        case ECS_UNLOADED:
            LoadCache(); // Load previous foreigns.
            CacheState_ = ECS_DIRTY;
            break;
        case ECS_CURRENT:
            CacheState_ = ECS_DIRTY;
            break;
        case ECS_DIRTY:
        case ECS_TAINTED:
            break;
    }

    // Prevent races if key fields are in multiple lock groups.
    // Not calling Lock() to preserve const correctness.
    GetKeyStorageDriver().Lock(NTableClient::ELockType::SharedStrong);
    ScheduleReconcileOnCommit();
}

void TReferenceAttributeBase::BeforeKeyMutableLoad() const
{
    if (CacheIsBusy()) {
        return; // Ignore self-induced notification.
    }

    switch (CacheState_) {
        case ECS_UNLOADED:
            LoadCache(); // Load previous foreigns.
            CacheState_ = ECS_TAINTED;
            break;
        case ECS_CURRENT:
        case ECS_DIRTY:
            CacheState_ = ECS_TAINTED;
            break;
        case ECS_TAINTED:
            break;
    }

    GetKeyStorageDriver().Lock(NTableClient::ELockType::SharedStrong);
    ScheduleReconcileOnCommit();
}

void TReferenceAttributeBase::BeforeStore() const
{
    // Recursive stores should not happen.
    THROW_ERROR_EXCEPTION_IF(CacheIsBusy(), "Called BeforeStore() on a busy cache");

    switch (CacheState_) {
        case ECS_UNLOADED:
            LoadCache(); // Load previous foreigns.
            CacheState_ = ECS_CURRENT;
            break;
        case ECS_CURRENT:
        case ECS_DIRTY: // Total store will clean it out.
        case ECS_TAINTED: // Same. Don't need to reload here.
            break;
    }

    GetOwner()->ScheduleStore();
    CacheIsBusy_ = true;
}

void TReferenceAttributeBase::BeforePartialStore() const
{
    // Add/Remove is being called on a single foreign in a multiref. We don't need to activate the
    // cache for that. In the case of a large tabular ref the performance hit can be prohibitive.

    // Recursive stores should not happen.
    THROW_ERROR_EXCEPTION_IF(CacheIsBusy(), "Called BeforePartialStore() on a busy cache");

    switch (CacheState_) {
        case ECS_UNLOADED:
        case ECS_CURRENT:
            break;
        case ECS_DIRTY:
            ApplyKeyChanges();
            CacheState_ = ECS_CURRENT;
            break;
        case ECS_TAINTED:
            ApplyKeyChanges();
            break;
    }

    GetOwner()->ScheduleStore();
    CacheIsBusy_ = true;
}

void TReferenceAttributeBase::AfterStore() const
{
    // Before/After mismatch.
    THROW_ERROR_EXCEPTION_IF(!CacheIsBusy(), "Called AfterStore() on a non-busy cache");
    CacheIsBusy_ = false;
}

void TReferenceAttributeBase::Reconcile() const
{
    // We don't commit during a cache update.
    THROW_ERROR_EXCEPTION_IF(CacheIsBusy(), "Called Reconcile() on a busy cache");

    switch (CacheState_) {
        case ECS_UNLOADED:
            YT_ABORT(); // We loaded in ScheduleReconcileOnCommit.
        case ECS_CURRENT:
            break;
        case ECS_DIRTY:
            ApplyKeyChanges();
            CacheState_ = ECS_CURRENT;
            break;
        case ECS_TAINTED:
            ApplyKeyChanges();
            break;
    }
}

void TReferenceAttributeBase::ScheduleReconcileOnCommit() const
{
    if (ReconcileState_ == EOS_SCHEDULED) {
        return;
    }
    ReconcileState_ = EOS_SCHEDULED;

    PreloadForeignKeyUpdate(); // Preload old foreigns.

    GetOwner()->GetSession()->ScheduleFinalize(
        [this] {
            YT_VERIFY(ReconcileState_ == EOS_SCHEDULED);
            if (CacheState_ == ECS_CURRENT) { // Already reconciled.
                ReconcileState_ = EOS_COMPLETED;
                return;
            }
            ReconcileState_ = EOS_RUNNING;
            PreloadForeignKeyUpdate(); // Preload new foreigns.
            GetOwner()->GetSession()->ScheduleFinalize(
                [this] {
                    YT_VERIFY(ReconcileState_ == EOS_RUNNING);
                    Reconcile();
                    // The backreference should not cause another store because of the
                    // busy cache state.
                    ReconcileState_ = EOS_COMPLETED;
                });
        });
}

bool TReferenceAttributeBase::CacheIsActive() const
{
    return CacheState_ != ECS_UNLOADED;
}

bool TReferenceAttributeBase::CacheIsBusy() const
{
    return CacheIsBusy_;
}

TObjectKey TReferenceAttributeBase::GetForeignStorageKey(TObject* foreign) const
{
    if (foreign == nullptr) {
        return TObjectKey();
    } else if (Settings().StoreParentKey) {
        return foreign->GetTypeHandler()->GetObjectTableKey(foreign);
    } else {
        return foreign->GetKey();
    }
}

std::pair<TObjectKey, TObjectKey> TReferenceAttributeBase::SplitForeignStorageKey(
    TObjectKey key,
    TObjectTypeValue foreignType) const
{
    if (Settings().StoreParentKey) {
        return GetOwner()
            ->GetSession()
            ->GetTypeHandlerOrThrow(foreignType)
            ->SplitObjectTableKey(key);
    } else {
        return std::pair(key, TObjectKey());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
