#pragma once

#include "persistence.h"

#include <yt/yt/orm/server/access_control/public.h>

#include <yt/yt/core/misc/property.h>

#include <library/cpp/yt/small_containers/compact_vector.h>
#include <library/cpp/yt/small_containers/compact_set.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NOrm::NDataModel {
    class TFinalizer;
}

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

// Do not use this class in multiple inheritance, because system relies heavily on
// comparison of pointer to TObject's derived and pointer to TObject.
class TObject
{
public:
    TObject(
        IObjectTypeHandler* typeHandler,
        ISession* session);
    virtual ~TObject() = default;

    void InitializeCreating(TObject* predecessor);
    void InitializeInstantiated(TObjectKey parentKey = {});

    // The unique identifier of this object within its type.
    // Not unique vs. other object types and other clusters.
    // May be different from the key in the DBTable (e.g., the table key may be prefixed by
    // the key of the parent object).
    virtual TObjectKey GetKey() const = 0;

    // The unique identifier of this object's parent, if any.
    virtual TObjectKey GetParentKey(std::source_location location = std::source_location::current()) const;
    virtual TParentKeyAttribute* GetParentKeyAttribute();

    virtual TObjectTypeValue GetType() const = 0;

    TString GetFqid(std::source_location location = std::source_location::current()) const;

    IObjectTypeHandler* GetTypeHandler() const;
    ISession* GetSession() const;

    EObjectState GetState() const;
    void SetState(EObjectState state);

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TControlAttributeObserver, ControlAttributeObserver);

    using TLifecycleObserverList = TCompactVector<IObjectLifecycleObserver*, 16>;
    DEFINE_BYREF_RO_PROPERTY(TLifecycleObserverList, LifecycleObservers);

    static const TScalarAttributeDescriptor<TObject, TInstant> CreationTimeDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TInstant>, CreationTime);

    // Labels map node cannot be null (see YP-2297 for details).
    static const TScalarAttributeDescriptor<TObject, NYTree::IMapNodePtr> LabelsDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<NYTree::IMapNodePtr>, Labels);

    static const TScalarAttributeDescriptor<TObject, bool> InheritAclDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<bool>, InheritAcl);

    using TTouchHistoryEventsSet = TCompactSet<TString, 4>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TTouchHistoryEventsSet, ControlTouchEventsToSave);

    // Overwrites default settings for object's references removal.
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(std::optional<bool>, AllowRemovalWithNonEmptyReferences);

    static const TScalarAttributeDescriptor<TObject, bool> ExistenceLockDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<bool>, ExistenceLock);

    DECLARE_BYREF_RW_PROPERTY(TAnnotationsAttribute, Annotations);

    DEFINE_BYVAL_RO_PROPERTY(TObject*, Predecessor);

    virtual TScalarAttribute<ui64>& FinalizationStartTime() noexcept;
    virtual const TScalarAttribute<ui64>& FinalizationStartTime() const noexcept;
    using TFinalizersAttribute = TScalarAttribute<THashMap<TString, NDataModel::TFinalizer>>;
    virtual TFinalizersAttribute& Finalizers() noexcept;
    virtual const TFinalizersAttribute& Finalizers() const noexcept;

    bool HasActiveFinalizers() const;
    bool IsFinalizerActive(TStringBuf finalizerName) const;
    int CountActiveFinalizers() const;
    void AddFinalizer(TStringBuf finalizerName, std::optional<std::string> ownerId = std::nullopt);
    void CompleteFinalizer(TStringBuf finalizerName);

    virtual void ScheduleUuidLoad() const = 0;
    void PrepareUuidValidation(std::string_view expected) const;
    void ValidateUuid(std::string_view expected, std::source_location location = std::source_location::current()) const;

    virtual void ScheduleMetaResponseLoad() const;

    virtual TObjectId GetUuid(std::source_location location = std::source_location::current()) const = 0;
    virtual TString GetName(std::source_location location = std::source_location::current()) const = 0;
    // Name load error is saved within the attribute model, such that any following load returns the error.
    // Stack unwinding may happen due to this error. During the unwinding #GetDisplayName function may be called.
    // Error suppression allows to build reasonable display name in such cases not hiding the original error.
    TString GetDisplayName(bool keyOnly = false, std::source_location location = std::source_location::current()) const;

    void ScheduleAccessControlLoad() const;

    bool DoesExist(std::source_location location = std::source_location::current()) const;
    bool DidExist(std::source_location location = std::source_location::current()) const;
    void ValidateExists(std::source_location location = std::source_location::current()) const;
    void ValidateNotFinalized(
        const IPersistentAttribute* updatedAttribute = nullptr,
        std::source_location location = std::source_location::current()) const;

    void ScheduleTombstoneCheck();
    bool IsTombstone(std::source_location location = std::source_location::current()) const;

    void ScheduleStore();
    bool IsStoreScheduled() const;

    bool RemovalStarted() const;
    bool IsFinalized() const;
    bool IsRemoved() const;

    template <class T>
    void ValidateAs() const;
    template <class T>
    const T* As() const;
    template <class T>
    T* As();

    // Dynamic interface adapts ORM to different access control models with the same fields layout.
    virtual NAccessControl::TAccessControlList LoadOldAcl(
        std::source_location location = std::source_location::current()) const = 0;
    virtual NAccessControl::TAccessControlList LoadAcl(
        std::source_location location = std::source_location::current()) const = 0;
    virtual void AddAce(
        const NAccessControl::TAccessControlEntry& ace,
        std::source_location location = std::source_location::current()) = 0;
    virtual void ScheduleAclLoad() const = 0;

    TScalarAttributeIndexBase* GetIndexOrThrow(const TString& indexName) const;

    void TouchRevisionTracker(const NYPath::TYPath& trackerPath);

    void ReconcileRevisionTrackers();

    void PrepareAttributeMigrations();

    void FinalizeAttributeMigrations();

    void MigrateAttribute(int migration);

    void ForceMigrateAttribute(int migration);

protected:
    void RegisterScalarAttributeIndex(
        TString indexName,
        std::unique_ptr<TScalarAttributeIndexBase> index);

private:
    friend class TAttributeBase;
    friend class TScalarAttributeIndexBase;
    friend class TReferenceAttributeBase;

    IObjectTypeHandler* const TypeHandler_;
    ISession* const Session_;

    EObjectState State_ = EObjectState::Unknown;

    std::optional<TAnnotationsAttribute> Annotations_;

    bool StoreScheduled_ = false;

    TObjectExistenceChecker ExistenceChecker_;
    std::optional<TFinalizationChecker> FinalizationChecker_;
    std::optional<TObjectTombstoneChecker> TombstoneChecker_;

    THashMap<TString, std::unique_ptr<TScalarAttributeIndexBase>> IndexPerName_;

    THashSet<NYPath::TYPath> TouchedRevisionTrackers_;

    TBitSet<int> AttributeMigrations_;
    TBitSet<int> ForcedAttributeMigrations_;

    void RegisterAttribute(IPersistentAttribute* attribute);
    void RegisterLifecycleObserver(IObjectLifecycleObserver* observer);

    bool IsFinalizing() const;
    bool IsRemoving() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

#define OBJECT_INL_H_
#include "object-inl.h"
#undef OBJECT_INL_H_

#define PERSISTENCE_INL_H_
#include "persistence-inl.h"
#undef PERSISTENCE_INL_H_
