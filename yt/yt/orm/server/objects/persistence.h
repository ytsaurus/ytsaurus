#pragma once

#include "public.h"

#include <yt/yt/orm/client/objects/key.h>

#include <yt/yt/orm/library/query/public.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/misc/range.h>

#include <yt/yt/core/yson/writer.h>

#include <yt/yt/ytlib/table_client/row_merger.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

#include <optional>
#include <source_location>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

// Order of underlying values matters. The greater value is, the more state is loaded.
DEFINE_ENUM(EAnnotationsLoadAllState,
    ((None)      (0))
    ((Keys)      (1))
    ((KeyValues) (2))
);

DEFINE_ENUM(EAttributeLoadState,
    ((None)      (0))
    ((Scheduled) (1))
    ((Loading)   (2))
    ((Loaded)    (3))
);

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

struct IObjectLifecycleObserver
{
    virtual ~IObjectLifecycleObserver() = default;

    virtual void OnObjectInitialization() = 0;

    virtual void PreloadObjectRemoval() const = 0;
    virtual void CheckObjectRemoval() const = 0;
    virtual void OnObjectRemovalStart() = 0;
    virtual void OnObjectRemovalFinish() = 0;
};

struct IPersistentAttribute
    : public IObjectLifecycleObserver
{ };

struct IIndexAttributeStoreObserver
{
    virtual ~IIndexAttributeStoreObserver() = default;

    virtual void Preload() const = 0;

    virtual void OnIndexedAttributeStore() = 0;
    virtual void OnPredicateAttributeStore() = 0;
};

class TControlAttributeObserver
{
public:
    void OnCall(TStringBuf method);

    bool IsCalled() const;
    bool IsCalled(TStringBuf method) const;

private:
    THashSet<TString> MethodsCalled_;
};

class TObjectExistenceChecker
{
public:
    explicit TObjectExistenceChecker(TObject* object);

    TObject* GetObject() const;
    void ScheduleCheck(TObjectKey parentKey = {}) const;
    bool Check(
        TObjectKey parentKey = {},
        std::source_location location = std::source_location::current()) const;

private:
    TObject* const Object_;

    NDetail::EAttributeLoadState LoadState_ = NDetail::EAttributeLoadState::None;
    bool ParentCheckScheduled_ = false;
    TErrorOr<bool> ExistsOrError_;

    void LoadFromDB(ILoadContext* context, TObjectKey parentKey);
};

class TFinalizationChecker
{
public:
    explicit TFinalizationChecker(TObject* object);

    void ScheduleCheck() const;
    void FlushCheck() const;

private:
    TObject* const Object_;
    mutable NDetail::EAttributeLoadState LoadState_ = NDetail::EAttributeLoadState::None;
};

class TObjectTombstoneChecker
{
public:
    explicit TObjectTombstoneChecker(const TObject* object);

    void ScheduleCheck() const;
    bool Check(std::source_location location = std::source_location::current()) const;

private:
    const TObject* const Object_;

    mutable NDetail::EAttributeLoadState LoadState_ = NDetail::EAttributeLoadState::None;
    bool Tombstone_ = false;

    void LoadFromDB(ILoadContext* context);
};

class TAttributeBase
    : public IPersistentAttribute
    , private TNonCopyable
{
public:
    explicit TAttributeBase(TObject* owner);

    const TObject* GetOwner() const;
    TObject* GetOwner();

    virtual bool IsStoreScheduled() const;

protected:
    TObject* const Owner_;

    void DoScheduleLoad(ELoadPriority priority = ELoadPriority::Default) const;
    void DoScheduleStore() const;
    void DoScheduleLock() const;

    virtual void LoadFromDB(ILoadContext* context);
    virtual void StoreToDB(IStoreContext* context);
    virtual void LockInDB(IStoreContext* context);

    // IPersistentAttribute implementation.
    void OnObjectInitialization() override;

    void PreloadObjectRemoval() const override;
    void CheckObjectRemoval() const override;
    void OnObjectRemovalStart() override;
    void OnObjectRemovalFinish() override;

private:
    bool LoadScheduled_ = false;
    bool StoreScheduled_ = false;
    bool LockScheduled_ = false;
};

class TParentKeyAttribute
    : public TAttributeBase
{
public:
    TParentKeyAttribute(TObject* owner, TObjectKey parentKey);

    const TObjectKey& GetKey(std::source_location location = std::source_location::current()) const;

    void ScheduleParentLoad();

private:
    TObjectKey ParentKey_;
    bool Missing_ = false;
    bool PreloadParentOnKeyLoad_ = false;

    // TAttributeBase implementation.
    void LoadFromDB(ILoadContext* context) override;
    void PreloadParentObject() const;
};

template <class T>
class TParentAttribute
{
public:
    explicit TParentAttribute(TObject* owner);

    T* Load(std::source_location location = std::source_location::current()) const;
    operator T*() const;

private:
    TObject* const Owner_;
};

class TChildrenAttributeBase
    : public TAttributeBase
{
public:
    explicit TChildrenAttributeBase(TObject* owner);

    void ScheduleLoad() const;
    const THashSet<TObject*>& UntypedLoad(std::source_location location = std::source_location::current()) const;

    virtual TObjectTypeValue GetChildrenType() const = 0;

protected:
    friend struct TChildrenAttributeHelper;

    std::optional<THashSet<TObject*>> Children_;
    THashSet<TObject*> AddedChildren_;
    THashSet<TObject*> RemovedChildren_;

    void DoAdd(TObject* child);
    void DoRemove(TObject* child);

    // IPersistent implementation.
    void OnObjectInitialization() override;
    void LoadFromDB(ILoadContext* context) override;
};

template <class T>
class TChildrenAttribute
    : public TChildrenAttributeBase
{
public:
    explicit TChildrenAttribute(TObject* owner);

    std::vector<T*> Load(std::source_location location = std::source_location::current()) const;

private:
    TObjectTypeValue GetChildrenType() const override;
};

class TAttributeObservers
{
public:
    using TObserver = std::function<void(TObject*, /*appliedSharedWrite*/ bool)>;
    using TObservers = std::vector<TObserver>;

    const TObservers& Get() const;

    void Add(TObserver observer) const;

private:
    mutable TObservers Observers_;

    friend class TAttributeSchema;
    friend class TScalarAttributeSchemaBuilder;
};

struct TScalarAttributeDescriptorBase
{
    explicit TScalarAttributeDescriptorBase(
        const TDBField* field,
        bool alwaysScheduleLoadOnStore = false);

    virtual TScalarAttributeBase* AttributeBaseGetter(TObject* /*object*/) const;

    virtual void RunFinalValueValidators(const TObject* /*object*/) const
    { }

    const TDBField* Field;
    const bool ScheduleLoadOnStore;

    TAttributeObservers BeforeStoreObservers;
    TAttributeObservers BeforeMutableLoadObservers;
};

class TScalarAttributeBase
    : public TAttributeBase
{
public:
    TScalarAttributeBase(
        TObject* owner,
        const TScalarAttributeDescriptorBase* descriptor);

    void ScheduleLoad() const;

    void ScheduleLoadTimestamp() const;
    TTimestamp LoadTimestamp(std::source_location location = std::source_location::current()) const;

    // Allows to load data for observers (e.g. data for indexes)
    // in the same read phase with other loads before store.
    void BeforeStorePreload() const;

    virtual bool IsChanged(
        const NYPath::TYPath& path = "",
        std::source_location location = std::source_location::current()) const = 0;

    void Lock(NTableClient::ELockType lockType);

    virtual TObjectKey::TKeyField LoadAsKeyField(const TString& suffix) const = 0;
    virtual TObjectKey::TKeyField LoadAsKeyFieldOld(const TString& suffix) const = 0;
    virtual std::vector<TObjectKey::TKeyField> LoadAsKeyFields(const TString& suffix) const = 0;
    virtual std::vector<TObjectKey::TKeyField> LoadAsKeyFieldsOld(const TString& suffix) const = 0;

    virtual void StoreKeyField(TObjectKey::TKeyField keyField, const TString& suffix) = 0;
    virtual void StoreKeyFields(TObjectKey::TKeyFields keyFields, const TString& suffix) = 0;

    const TScalarAttributeDescriptorBase* GetDescriptor() const;

    using TConstProtosView = std::optional<TCompactVector<const google::protobuf::Message*, 1>>;
    virtual TConstProtosView LoadAsProtos(bool old) const = 0;

    using TProtosView = std::optional<TCompactVector<google::protobuf::Message*, 1>>;
    virtual TProtosView MutableLoadAsProtos() = 0;

protected:
    const TScalarAttributeDescriptorBase* const Descriptor_;

    struct TLoadState
    {
        NDetail::EAttributeLoadState Value;
        NDetail::EAttributeLoadState Timestamp;
    };

    mutable TLoadState LoadState_ = {
        .Value = NDetail::EAttributeLoadState::None,
        .Timestamp = NDetail::EAttributeLoadState::None
    };
    bool Missing_ = false;
    TTimestamp Timestamp_ = NTransactionClient::NullTimestamp;
    mutable std::vector<IIndexAttributeStoreObserver*> IndexedStoreObservers_;
    mutable std::vector<IIndexAttributeStoreObserver*> PredicateStoreObservers_;
    NTableClient::ELockType LockType_ = NTableClient::ELockType::None;

    void ScheduleStore();

    void OnLoad(std::source_location location) const;
    void OnLoadTimestamp(std::source_location location) const;
    void OnStore(bool sharedWrite, std::source_location location);

    // TAttributeBase implementation.
    void LoadFromDB(ILoadContext* context) override;
    void StoreToDB(IStoreContext* context) override;
    void LockInDB(IStoreContext* context) override;

    // IPersistentAttribute implementation.
    void OnObjectInitialization() override;

    // NB: used to check whether we had changes before storing to DB.
    virtual bool HasChangesForStoringToDB() const;

    void UpdateSharedWriteLockFlag(bool sharedWrite);

private:
    std::optional<bool> SharedWrite_;

    void DoOnLoad(const NDetail::EAttributeLoadState& loadState, std::source_location location) const;

    virtual void SetDefaultValues() = 0;

    virtual void LoadOldValue(const NTableClient::TUnversionedValue& value, ILoadContext* context) = 0;
    virtual void StoreNewValue(NTableClient::TUnversionedValue* dbValue, IStoreContext* context) = 0;

    friend class TScalarAttributeIndexBase;

    void RegisterStoreObserver(IIndexAttributeStoreObserver* observer, bool fromPredicate) const;
};

// NB: ORM-master code relies on static storage duration of TScalarAttributeDescriptor objects.
template <class TTypedObject, class TTypedValue>
class TScalarAttributeDescriptor
    : public TScalarAttributeDescriptorBase
{
public:
    using TInitializer = std::function<void(TTransaction*, TTypedObject*, TTypedValue*)>;

    TScalarAttributeDescriptor(
        const TDBField* field,
        std::function<TScalarAttribute<TTypedValue>*(TTypedObject*)> attributeGetter,
        bool alwaysScheduleLoadOnStore = false)
        : TScalarAttributeDescriptorBase(field, alwaysScheduleLoadOnStore)
        , AttributeGetter_(std::move(attributeGetter))
    { }

    const TScalarAttributeDescriptor& SetInitializer(TInitializer initializer) const;

    const TInitializer& GetInitializer() const;

    const TScalarAttributeDescriptor& AddValidator(
        std::function<void(TTransaction*, const TTypedObject*, const TTypedValue&,
        const TTypedValue&)> validator) const;

    const TScalarAttributeDescriptor& AddValidator(
        std::function<void(TTransaction*, const TTypedObject*, const TTypedValue&)> validator) const;

    TScalarAttribute<TTypedValue>* AttributeGetter(TTypedObject* typedObject) const;

    const TScalarAttribute<TTypedValue>* AttributeGetter(const TTypedObject* typedObject) const;

    TScalarAttributeBase* AttributeBaseGetter(TObject* object) const override;

    bool DoesNeedOldValue() const;

    void RunValidators(
        const TObject* object,
        const TTypedValue& currentValue,
        const TTypedValue* newValue) const;

    void RunFinalValueValidators(const TObject* object) const override;

private:
    const std::function<TScalarAttribute<TTypedValue>*(TTypedObject*)> AttributeGetter_;
    mutable TInitializer Initializer_;
    mutable std::vector<std::function<void(TTransaction*, const TTypedObject*, const TTypedValue&, const TTypedValue&)>> OldNewValueValidators_;
    mutable std::vector<std::function<void(TTransaction*, const TTypedObject*, const TTypedValue&)>> NewValueValidators_;
};

struct IScalarAttributeBaseHolder
{
    virtual const TScalarAttributeBase* GetAttributeForIndex() const = 0;
};

template <class T>
class TScalarAttribute
    : public TScalarAttributeBase
    , public IScalarAttributeBaseHolder
{
public:
    using TValue = T;

    TScalarAttribute(
        TObject* owner,
        const TScalarAttributeDescriptorBase* descriptor);

    const T& Load(std::source_location location = std::source_location::current()) const;
    operator const T&() const;
    const T& LoadOld(std::source_location location = std::source_location::current()) const;

    // NB: Schedules store to the underlying storage.
    T* MutableLoad(
        std::optional<bool> sharedWrite = std::nullopt,
        std::source_location location = std::source_location::current());

    TObjectKey::TKeyField LoadAsKeyField(const TString& suffix) const override;
    TObjectKey::TKeyField LoadAsKeyFieldOld(const TString& suffix) const override;
    std::vector<TObjectKey::TKeyField> LoadAsKeyFields(const TString& suffix) const override;
    std::vector<TObjectKey::TKeyField> LoadAsKeyFieldsOld(const TString& suffix) const override;

    void StoreKeyField(TObjectKey::TKeyField keyField, const TString& suffix) override;
    void StoreKeyFields(TObjectKey::TKeyFields keyFields, const TString& suffix) override;

    TConstProtosView LoadAsProtos(bool old) const override;
    TProtosView MutableLoadAsProtos() override;

    //! Throws if the attribute is not composite and the path is not empty.
    bool IsChanged(
        const NYPath::TYPath& path = "",
        std::source_location location = std::source_location::current()) const override;

    bool IsChangedWithFilter(
        const NYPath::TYPath& path = "",
        const NYPath::TYPath& schemaPath = "",
        std::function<bool(const NYPath::TYPath&)> byPathFilter = nullptr,
        std::source_location location = std::source_location::current()) const;

    void Store(
        T value,
        std::optional<bool> sharedWrite = std::nullopt,
        std::source_location location = std::source_location::current());

    // NB: Default value may differ from T{}.
    void StoreDefault(
        std::optional<bool> sharedWrite = std::nullopt,
        std::source_location location = std::source_location::current());

    // IScalarAttributeBaseHolder implementation.
    const TScalarAttributeBase* GetAttributeForIndex() const override;

private:
    std::optional<T> NewValue_;

    bool LoadedFromDB_ = false;
    std::optional<T> OldValue_;

    bool HasChangesForStoringToDB() const override;

    void SetDefaultValues() override;
    void LoadOldValue(const NTableClient::TUnversionedValue& value, ILoadContext* context) override;
    void StoreNewValue(NTableClient::TUnversionedValue* dbValue, IStoreContext* context) override;

    TObjectKey::TKeyField ToKeyField(const T& value, const TString& suffix) const;
    std::vector<TObjectKey::TKeyField> ToKeyFields(const T& value, const TString& suffix) const;
};

template <>
TObjectKey::TKeyField TScalarAttribute<TInstant>::ToKeyField(
    const TInstant& value,
    const TString& suffix) const;

class TTimestampAttribute
    : public TScalarAttributeBase
{
public:
    TTimestampAttribute(
        TObject* owner,
        const TTimestampAttributeDescriptor* descriptor);

    void Touch(bool sharedWrite = false);

    bool IsChanged(
        const NYPath::TYPath& path = "",
        std::source_location location = std::source_location::current()) const override;

    TObjectKey::TKeyField LoadAsKeyField(const TString& suffix) const override;
    TObjectKey::TKeyField LoadAsKeyFieldOld(const TString& suffix) const override;
    std::vector<TObjectKey::TKeyField> LoadAsKeyFields(const TString& suffix) const override;
    std::vector<TObjectKey::TKeyField> LoadAsKeyFieldsOld(const TString& suffix) const override;

    void StoreKeyField(TObjectKey::TKeyField keyField, const TString& suffix) override;
    void StoreKeyFields(TObjectKey::TKeyFields keyFields, const TString& suffix) override;

    TConstProtosView LoadAsProtos(bool old) const override;
    TProtosView MutableLoadAsProtos() override;

private:
    bool AttributeTouched_ = false;

    void SetDefaultValues() override;
    void LoadOldValue(const NTableClient::TUnversionedValue& value, ILoadContext* context) override;
    void StoreNewValue(NTableClient::TUnversionedValue* dbValue, IStoreContext* context) override;

    using TScalarAttributeBase::ScheduleLoad;
};

template <class TThis, class TThat>
class TOneToOneAttributeDescriptor
    : public TScalarAttributeDescriptorBase
{
public:
    TOneToOneAttributeDescriptor(
        const TDBField* field,
        std::function<TOneToOneAttribute<TThis, TThat>*(TThis*)> forwardAttributeGetter,
        std::function<TOneToOneAttribute<TThat, TThis>*(TThat*)> inverseAttributeGetter,
        bool forbidNonEmptyRemoval,
        bool nullable = true)
        : TScalarAttributeDescriptorBase(field)
        , ForbidNonEmptyRemoval(forbidNonEmptyRemoval)
        , Nullable(nullable)
        , ForwardAttributeGetter_(std::move(forwardAttributeGetter))
        , InverseAttributeGetter_(std::move(inverseAttributeGetter))
    { }

    bool ForbidNonEmptyRemoval;
    bool Nullable;

    TOneToOneAttribute<TThis, TThat>* ForwardAttributeGetter(TThis* thisObject) const;

    const TOneToOneAttribute<TThis, TThat>* ForwardAttributeGetter(const TThis* thisObject) const;

    TOneToOneAttribute<TThat, TThis>* InverseAttributeGetter(TThat* thatObject) const;

    const TOneToOneAttribute<TThat, TThis>* InverseAttributeGetter(const TThat* thatObject) const;

private:
    std::function<TOneToOneAttribute<TThis, TThat>*(TThis*)> ForwardAttributeGetter_;
    std::function<TOneToOneAttribute<TThat, TThis>*(TThat*)> InverseAttributeGetter_;
};

template <class TThis, class TThat>
class TOneToOneAttribute
    : public TAttributeBase
    , public IScalarAttributeBaseHolder
{
public:
    using TDescriptor = TOneToOneAttributeDescriptor<TThis, TThat>;

    TOneToOneAttribute(
        TObject* owner,
        const TDescriptor* descriptor);

    void ScheduleLoad() const;

    TObjectPlugin<TThat>* Load(std::source_location location = std::source_location::current()) const;
    operator TObjectPlugin<TThat>*() const;
    TObjectPlugin<TThat>* LoadOld(std::source_location location = std::source_location::current()) const;

    void Store(TThat* value, std::source_location location = std::source_location::current());

    void Lock(NTableClient::ELockType lockType);

    void ScheduleLoadTimestamp() const;
    TTimestamp LoadTimestamp(std::source_location location = std::source_location::current()) const;

    bool IsChanged(std::source_location location = std::source_location::current()) const;

    // TAttributeBase implementation.
    bool IsStoreScheduled() const override;

    // IScalarAttributeBaseHolder implementation.
    const TScalarAttributeBase* GetAttributeForIndex() const override;

private:
    template <class TThis_, class TThat_>
    friend class TOneToOneAttribute;

    const TDescriptor* const Descriptor_;
    const TScalarAttributeDescriptor<TThis, TObjectId> UnderlyingDescriptor_;

    TScalarAttribute<TObjectId> Underlying_;

    TObjectPlugin<TThat>* IdToThat(const TObjectId& id) const;

    void PreloadObjectRemoval() const override;
    void CheckObjectRemoval() const override;
    void OnObjectRemovalStart() override;
};

template <class TMany, class TOne>
class TManyToOneAttributeDescriptor
    : public TScalarAttributeDescriptorBase
{
public:
    TManyToOneAttributeDescriptor(
        const TDBField* field,
        std::function<TManyToOneAttribute<TMany, TOne>*(TMany*)> forwardAttributeGetter,
        std::function<TOneToManyAttribute<TOne, TMany>*(TOne*)> inverseAttributeGetter,
        bool forbidNonEmptyRemoval,
        bool nullable = true)
        : TScalarAttributeDescriptorBase(field)
        , ForbidNonEmptyRemoval(forbidNonEmptyRemoval)
        , Nullable(nullable)
        , ForwardAttributeGetter_(std::move(forwardAttributeGetter))
        , InverseAttributeGetter_(std::move(inverseAttributeGetter))
    { }

    TManyToOneAttribute<TMany, TOne>* ForwardAttributeGetter(TMany* many) const;

    const TManyToOneAttribute<TMany, TOne>* ForwardAttributeGetter(const TMany* many) const;

    TOneToManyAttribute<TOne, TMany>* InverseAttributeGetter(TOne* one) const;

    const TOneToManyAttribute<TOne, TMany>* InverseAttributeGetter(const TOne* one) const;

    bool ForbidNonEmptyRemoval;
    bool Nullable;

private:
    std::function<TManyToOneAttribute<TMany, TOne>*(TMany*)> ForwardAttributeGetter_;
    std::function<TOneToManyAttribute<TOne, TMany>*(TOne*)> InverseAttributeGetter_;
};

template <class TMany, class TOne>
class TManyToOneAttribute
    : public TAttributeBase
    , public IScalarAttributeBaseHolder
{
public:
    using TDescriptor = TManyToOneAttributeDescriptor<TMany, TOne>;

    TManyToOneAttribute(
        TObject* owner,
        const TDescriptor* descriptor);

    void ScheduleLoad() const;

    TObjectKey LoadKey(std::source_location location = std::source_location::current()) const;
    TObjectKey LoadKeyOld(std::source_location location = std::source_location::current()) const;
    TObjectPlugin<TOne>* Load(std::source_location location = std::source_location::current()) const;
    operator TOne*() const;
    TObjectPlugin<TOne>* LoadOld(std::source_location location = std::source_location::current()) const;

    void Lock(NTableClient::ELockType lockType);

    void ScheduleLoadTimestamp() const;
    TTimestamp LoadTimestamp(std::source_location location = std::source_location::current()) const;

    bool IsChanged(std::source_location location = std::source_location::current()) const;

    // TAttributeBase implementation.
    bool IsStoreScheduled() const override;

    // IScalarAttributeBaseHolder implementation.
    const TScalarAttributeBase* GetAttributeForIndex() const override;

private:
    template <class TOne_, class TMany_>
    friend class TOneToManyAttribute;

    static_assert(std::tuple_size_v<typename TObjectKeyTraits<TOne>::TTypes> == 1);
    using TOneKeyFirstField = std::tuple_element_t<0, typename TObjectKeyTraits<TOne>::TTypes>;

    const TDescriptor* const Descriptor_;
    const TScalarAttributeDescriptor<TMany, TOneKeyFirstField> UnderlyingDescriptor_;

    TScalarAttribute<TOneKeyFirstField> Underlying_;

    TObjectPlugin<TOne> * KeyFirstFieldToOne(const TOneKeyFirstField& keyFirstField) const;
    void Store(TOne* one, std::source_location location = std::source_location::current());

    void PreloadObjectRemoval() const override;
    void CheckObjectRemoval() const override;
    void OnObjectRemovalStart() override;
};

////////////////////////////////////////////////////////////////////////////////

struct TAnyToManyAttributeDescriptorBase
{
    TAnyToManyAttributeDescriptorBase(
        const TDBTable* table,
        std::initializer_list<const TDBField*> primaryKeyFields,
        std::initializer_list<const TDBField*> foreignKeyFields,
        bool foreignObjectTableKey)
        : Table(table)
        , PrimaryKeyFields(primaryKeyFields)
        , ForeignKeyFields(foreignKeyFields)
        , ForeignObjectTableKey(foreignObjectTableKey)
    { }

    const TDBTable* Table;
    const TDBFields PrimaryKeyFields;
    const TDBFields ForeignKeyFields;
    bool ForeignObjectTableKey;
};

class TAnyToManyAttributeBase
    : public TAttributeBase
{
public:
    TAnyToManyAttributeBase(
        TObject* owner,
        const TAnyToManyAttributeDescriptorBase* descriptor);

    void ScheduleLoad() const;

    bool IsChanged() const;

protected:
    const TAnyToManyAttributeDescriptorBase* const Descriptor_;

    std::optional<THashSet<TObject*>> ForeignObjects_;
    std::optional<const THashSet<TObject*>> OldForeignObjects_;
    THashSet<TObject*> AddedForeignObjects_;
    THashSet<TObject*> RemovedForeignObjects_;

    const THashSet<TObject*>& UntypedLoad(
        std::source_location location = std::source_location::current(),
        bool old = false) const;

    template<typename TTypedObject>
    std::vector<TObjectPlugin<TTypedObject>*> TypedLoad(
        std::source_location location = std::source_location::current(),
        bool old = false) const;

    void DoAdd(TObject* many);
    void DoRemove(TObject* many);

    virtual TObjectTypeValue GetForeignObjectType() const = 0;
    virtual void ScheduleLoadForeignObjectAttribute(TObject* foreign) const = 0;

    // TAttributeBase implementation.
    void LoadFromDB(ILoadContext* context) final;
    void StoreToDB(IStoreContext* context) final;

    // IPersistentAttribute implementation.
    void OnObjectInitialization() final;

private:
    TObjectKey GetKey(TObject* object) const;
};

////////////////////////////////////////////////////////////////////////////////

template <class TOne, class TMany>
class TOneToManyAttributeDescriptor
    : public TAnyToManyAttributeDescriptorBase
{
public:
    TOneToManyAttributeDescriptor(
        const TDBTable* table,
        std::initializer_list<const TDBField*> primaryKeyFields,
        std::initializer_list<const TDBField*> foreignKeyFields,
        std::function<TOneToManyAttribute<TOne, TMany>*(TOne*)> forwardAttributeGetter,
        std::function<TManyToOneAttribute<TMany, TOne>*(TMany*)> inverseAttributeGetter,
        bool foreignObjectTableKey,
        bool forbidNonEmptyRemoval)
        : TAnyToManyAttributeDescriptorBase(table, primaryKeyFields, foreignKeyFields, foreignObjectTableKey)
        , ForbidNonEmptyRemoval(forbidNonEmptyRemoval)
        , ForwardAttributeGetter_(std::move(forwardAttributeGetter))
        , InverseAttributeGetter_(std::move(inverseAttributeGetter))
    { }

    TOneToManyAttribute<TOne, TMany>* ForwardAttributeGetter(TOne* one) const;

    const TOneToManyAttribute<TOne, TMany>* ForwardAttributeGetter(const TOne* one) const;

    TManyToOneAttribute<TMany, TOne>* InverseAttributeGetter(TMany* many) const;

    const TManyToOneAttribute<TMany, TOne>* InverseAttributeGetter(const TMany* many) const;

    bool ForbidNonEmptyRemoval;

private:
    std::function<TOneToManyAttribute<TOne, TMany>*(TOne*)> ForwardAttributeGetter_;
    std::function<TManyToOneAttribute<TMany, TOne>*(TMany*)> InverseAttributeGetter_;
};

template <class TOne, class TMany>
class TOneToManyAttribute
    : public TAnyToManyAttributeBase
{
public:
    using TDescriptor = TOneToManyAttributeDescriptor<TOne, TMany>;

    TOneToManyAttribute(
        TOne* owner,
        const TDescriptor* descriptor);

    std::vector<TObjectPlugin<TMany>*> Load(std::source_location location = std::source_location::current()) const;
    std::vector<TObjectPlugin<TMany>*> LoadOld(std::source_location location = std::source_location::current()) const;

    void Add(TObjectPlugin<TMany>* many, std::source_location location = std::source_location::current());
    void Remove(TObjectPlugin<TMany>* many, std::source_location location = std::source_location::current());
    void Clear(std::source_location location = std::source_location::current());

private:
    TOne* const TypedOwner_;
    const TDescriptor* const TypedDescriptor_;

    // TAnyToManyAttributeBase implementation.
    TObjectTypeValue GetForeignObjectType() const override;
    void ScheduleLoadForeignObjectAttribute(TObject* many) const override;

    // IPersistentAttribute implementation.
    void PreloadObjectRemoval() const override;
    void CheckObjectRemoval() const override;
    void OnObjectRemovalStart() override;
};

////////////////////////////////////////////////////////////////////////////////

template <class TOwner, class TForeign>
class TManyToManyInlineAttributeDescriptor
    : public TScalarAttributeDescriptorBase
{
public:
    TManyToManyInlineAttributeDescriptor(
        const TDBField* field,
        std::function<TManyToManyInlineAttribute<TOwner, TForeign>*(TOwner*)> forwardAttributeGetter,
        std::function<TManyToManyTabularAttribute<TForeign, TOwner>*(TForeign*)> inverseAttributeGetter,
        bool forbidNonEmptyRemoval)
        : TScalarAttributeDescriptorBase(field)
        , ForbidNonEmptyRemoval(forbidNonEmptyRemoval)
        , ForwardAttributeGetter_(std::move(forwardAttributeGetter))
        , InverseAttributeGetter_(std::move(inverseAttributeGetter))
    { }

    TManyToManyInlineAttribute<TOwner, TForeign>* ForwardAttributeGetter(TOwner* owner) const;

    const TManyToManyInlineAttribute<TOwner, TForeign>* ForwardAttributeGetter(const TOwner* owner) const;

    TManyToManyTabularAttribute<TForeign, TOwner>* InverseAttributeGetter(TForeign* foreign) const;

    const TManyToManyTabularAttribute<TForeign, TOwner>* InverseAttributeGetter(const TForeign* foreign) const;

    bool ForbidNonEmptyRemoval;

private:
    std::function<TManyToManyInlineAttribute<TOwner, TForeign>*(TOwner*)> ForwardAttributeGetter_;
    std::function<TManyToManyTabularAttribute<TForeign, TOwner>*(TForeign*)> InverseAttributeGetter_;
};

template <class TOwner, class TForeign>
class TManyToManyInlineAttribute
    : public TAttributeBase
    , public IScalarAttributeBaseHolder
{
public:
    using TDescriptor = TManyToManyInlineAttributeDescriptor<TOwner, TForeign>;

    TManyToManyInlineAttribute(
        TObject* owner,
        const TDescriptor* descriptor);

    void Lock(NTableClient::ELockType lockType);

    void ScheduleLoad() const;

    std::vector<TObjectKey> LoadKeys(std::source_location location = std::source_location::current()) const;
    std::vector<TObjectKey> LoadKeysOld(std::source_location location = std::source_location::current()) const;
    std::vector<TObjectPlugin<TForeign>*> Load(std::source_location location = std::source_location::current()) const;
    std::vector<TObjectPlugin<TForeign>*> LoadOld(std::source_location location = std::source_location::current()) const;

    void ScheduleLoadTimestamp() const;
    TTimestamp LoadTimestamp(std::source_location location = std::source_location::current()) const;

    bool IsChanged(std::source_location location = std::source_location::current()) const;

    // TAttributeBase implementation.
    bool IsStoreScheduled() const override;

    void Store(std::vector<TForeign*> foreign, std::source_location location = std::source_location::current());
    void Add(TForeign* foreign, std::source_location location = std::source_location::current());
    void Remove(TForeign* foreign, std::source_location location = std::source_location::current());

    // IScalarAttributeBaseHolder implementation.
    const TScalarAttributeBase* GetAttributeForIndex() const override;

private:
    static_assert(std::tuple_size_v<typename TObjectKeyTraits<TForeign>::TTypes> == 1);
    using TForeignKeyFirstField = std::tuple_element_t<0, typename TObjectKeyTraits<TForeign>::TTypes>;

    const TDescriptor* const Descriptor_;
    const TScalarAttributeDescriptor<TOwner, std::vector<TForeignKeyFirstField>> UnderlyingDescriptor_;

    TScalarAttribute<std::vector<TForeignKeyFirstField>> Underlying_;

    std::vector<TObjectPlugin<TForeign>*> KeyFirstFieldToForeigns(
        const std::vector<TForeignKeyFirstField>& keyFirstFields) const;

    TForeignKeyFirstField ForeignToKeyFirstField(const TForeign* foreign) const;

    // IPersistentAttribute implementation.
    void PreloadObjectRemoval() const override;
    void CheckObjectRemoval() const override;
    void OnObjectRemovalStart() override;
};

////////////////////////////////////////////////////////////////////////////////

template <class TOwner, class TForeign>
class TManyToManyTabularAttributeDescriptor
    : public TAnyToManyAttributeDescriptorBase
{
public:
    TManyToManyTabularAttributeDescriptor(
        const TDBTable* table,
        std::initializer_list<const TDBField*> primaryKeyFields,
        std::initializer_list<const TDBField*> foreignKeyFields,
        std::function<TManyToManyTabularAttribute<TOwner, TForeign>*(TOwner*)> forwardAttributeGetter,
        std::function<TManyToManyInlineAttribute<TForeign, TOwner>*(TForeign*)> inverseAttributeGetter,
        bool foreignObjectTableKey,
        bool forbidNonEmptyRemoval)
        : TAnyToManyAttributeDescriptorBase(table, primaryKeyFields, foreignKeyFields, foreignObjectTableKey)
        , ForbidNonEmptyRemoval(forbidNonEmptyRemoval)
        , ForwardAttributeGetter_(std::move(forwardAttributeGetter))
        , InverseAttributeGetter_(std::move(inverseAttributeGetter))
    { }

    TManyToManyTabularAttribute<TOwner, TForeign>* ForwardAttributeGetter(TOwner* owner) const;

    const TManyToManyTabularAttribute<TOwner, TForeign>* ForwardAttributeGetter(const TOwner* owner) const;

    TManyToManyInlineAttribute<TForeign, TOwner>* InverseAttributeGetter(TForeign* foreign) const;

    const TManyToManyInlineAttribute<TForeign, TOwner>* InverseAttributeGetter(const TForeign* foreign) const;

    bool ForbidNonEmptyRemoval;

private:
    std::function<TManyToManyTabularAttribute<TOwner, TForeign>*(TOwner*)> ForwardAttributeGetter_;
    std::function<TManyToManyInlineAttribute<TForeign, TOwner>*(TForeign*)> InverseAttributeGetter_;
};

template <class TOwner, class TForeign>
class TManyToManyTabularAttribute
    : public TAnyToManyAttributeBase
{
public:
    using TDescriptor = TManyToManyTabularAttributeDescriptor<TOwner, TForeign>;

    TManyToManyTabularAttribute(
        TOwner* owner,
        const TDescriptor* descriptor);

    std::vector<TObjectPlugin<TForeign>*> Load(std::source_location location = std::source_location::current()) const;
    std::vector<TObjectPlugin<TForeign>*> LoadOld(std::source_location location = std::source_location::current()) const;

    void Clear(std::source_location location = std::source_location::current());

private:
    template <class TOwner_, class TForeign_>
    friend class TManyToManyInlineAttribute;

    TOwner* const TypedOwner_;
    const TDescriptor* const TypedDescriptor_;

    void DoAdd(TForeign* foreign);
    void DoRemove(TForeign* foreign);

    // TAnyToManyAttributeBase implementation.
    TObjectTypeValue GetForeignObjectType() const override;
    void ScheduleLoadForeignObjectAttribute(TObject* foreign) const override;

    // IPersistentAttribute implementation.
    void PreloadObjectRemoval() const override;
    void CheckObjectRemoval() const override;
    void OnObjectRemovalStart() override;
};

////////////////////////////////////////////////////////////////////////////////

class TAnnotationsAttribute
    : public TAttributeBase
{
public:
    explicit TAnnotationsAttribute(TObject* owner);

    void ScheduleLoad(std::string_view key) const;
    NYson::TYsonString Load(std::string_view key, std::source_location location = std::source_location::current()) const;

    bool Contains(std::string_view key, std::source_location location = std::source_location::current()) const;

    void ScheduleLoadAll() const;
    std::vector<std::pair<std::string, NYson::TYsonString>> LoadAll(
        std::source_location location = std::source_location::current()) const;
    void ScheduleLoadAllOldKeys() const;
    const THashSet<std::string>& LoadAllOldKeys(std::source_location location = std::source_location::current()) const;

    void ScheduleLoadTimestamp(std::string_view key) const;
    TTimestamp LoadTimestamp(std::string_view key, std::source_location location = std::source_location::current()) const;

    void Store(
        std::string_view key,
        const NYson::TYsonString& value,
        std::source_location location = std::source_location::current());
    bool IsStoreScheduled(std::string_view key) const;

    // Needed to overcome warning about hiding overloaded virtual function.
    using TAttributeBase::IsStoreScheduled;

private:
    mutable THashSet<std::string> ScheduledLoadKeys_;
    // Null values indicate tombstones.
    THashMap<std::string, NYson::TYsonString, THash<std::string>, TEqualTo<>> KeyToValue_;

    mutable THashSet<std::string> ScheduledLoadTimestampKeys_;
    // NullTimestamp indicate missing keys.
    THashMap<std::string, TTimestamp, THash<std::string>, TEqualTo<>> KeyToTimestamp_;

    THashSet<std::string, THash<std::string>, TEqualTo<>> ScheduledStoreKeys_;

    THashSet<std::string> OldKeys_;

    // All-keys machinery.
    using EAnnotationsLoadAllState = NDetail::EAnnotationsLoadAllState;
    mutable EAnnotationsLoadAllState ScheduledLoadAll_ = EAnnotationsLoadAllState::None;
    EAnnotationsLoadAllState LoadedAll_ = EAnnotationsLoadAllState::None;

    void ScheduleLoadAll(EAnnotationsLoadAllState state) const;

    // TAttributeBase implementation.
    void LoadFromDB(ILoadContext* context) override;
    void StoreToDB(IStoreContext* context) override;

    // IPersistentAttribute implementation.
    void OnObjectInitialization() override;
    void PreloadObjectRemoval() const override;
    void OnObjectRemovalFinish() override;
};

////////////////////////////////////////////////////////////////////////////////

struct TIndexedAttributeDescriptor
{
    TIndexedAttributeDescriptor(
        const TScalarAttributeDescriptorBase* attributeDescriptor,
        NYPath::TYPath path);

    const TScalarAttributeDescriptorBase* AttributeDescriptor;
    NYPath::TYPath Path;
};

struct TPredicateInfo
{
    TString Expression;
    std::vector<TString> AttributesToValidate;
};

class TScalarAttributeIndexDescriptor
{
public:
    TScalarAttributeIndexDescriptor(
        TString indexName,
        const TDBIndexTable* table,
        std::vector<TIndexedAttributeDescriptor> indexedAttributeDescriptors,
        std::vector<TIndexedAttributeDescriptor> predicateAttributeDescriptors,
        bool repeated,
        EIndexMode mode,
        std::optional<TPredicateInfo> predicateInfo = std::nullopt);

    const TDBField* GetIndexFieldByAttribute(
        const TDBField* field,
        const NYPath::TYPath& path) const;

    const TString IndexName;
    const TDBIndexTable* Table;
    const std::vector<TIndexedAttributeDescriptor> IndexedAttributeDescriptors;
    const std::vector<TIndexedAttributeDescriptor> PredicateAttributeDescriptors;
    const bool Repeated;
    const EIndexMode Mode;
    NQuery::IFilterMatcherPtr Predicate;

private:
    void ExtractAndValidateAttributes(
        const TString& predicateExpression,
        const std::vector<TString>& attributesToValidate);
};

////////////////////////////////////////////////////////////////////////////////

struct TIndexAttribute
{
    TIndexAttribute(
        const IScalarAttributeBaseHolder* attribute,
        TString suffix,
        bool repeated);

    const TScalarAttributeBase* Attribute;
    const TString Suffix;
    const bool Repeated;
};

class TScalarAttributeIndexBase
    : public IObjectLifecycleObserver
    , public IIndexAttributeStoreObserver
{
public:
    TScalarAttributeIndexBase(
        const TScalarAttributeIndexDescriptor* descriptor,
        TObject* owner,
        std::vector<TIndexAttribute> indexedAttributes,
        std::vector<TIndexAttribute> predicateAttributes,
        bool unique);
    void TouchIndex();

protected:
    bool ObjectCreated_ = false;
    bool IndexedAttributesUpdated_ = false;
    bool PredicateAttributesUpdated_ = false;
    bool ObjectRemoved_ = false;
    bool ObjectTouched_ = false;

    const std::vector<TIndexAttribute> IndexedAttributes_;
    const std::vector<TIndexAttribute> PredicateAttributes_;
    const TScalarAttributeIndexDescriptor* const Descriptor_;
    TObject* const Owner_;
    const bool Unique_;
    const EIndexMode IndexMode_;

    bool DoesPredicateMatch(bool old) const;
    bool OldPredicateValue() const;
    bool NewPredicateValue() const; //  NB: this function must be called after transaction flush, since it caches result

private:
    bool StoreScheduled_ = false;
    bool AlreadyStored_ = false;

    mutable std::optional<bool> OldPredicateValue_;
    mutable std::optional<bool> NewPredicateValue_;

    // IObjectLifecycleObserver implementation.
    void OnObjectInitialization() override;
    void PreloadObjectRemoval() const override;
    void CheckObjectRemoval() const override;
    void OnObjectRemovalStart() override;
    void OnObjectRemovalFinish() override;

    // IIndexAttributeStoreObserver implementation.
    void Preload() const override;
    void OnIndexedAttributeStore() override;
    void OnPredicateAttributeStore() override;

    void ScheduleStore();
    void ScheduleLoad() const;

    virtual std::vector<TObjectKey> GetIndexKeysToAdd() const = 0;
    virtual std::vector<TObjectKey> GetIndexKeysToRemove() const = 0;
};

class TScalarAttributeIndex
    : public TScalarAttributeIndexBase
{
public:
    TScalarAttributeIndex(
        const TScalarAttributeIndexDescriptor* descriptor,
        TObject* owner,
        std::vector<TIndexAttribute> indexedAttributes,
        std::vector<TIndexAttribute> predicateAttributes,
        bool unique);

private:
    TObjectKey GetIndexKey() const;
    TObjectKey GetIndexKeyOld() const;
    std::vector<TObjectKey> GetIndexKeysToAdd() const override;
    std::vector<TObjectKey> GetIndexKeysToRemove() const override;
};

class TRepeatedScalarAttributeIndex
    : public TScalarAttributeIndexBase
{
public:
    TRepeatedScalarAttributeIndex(
        const TScalarAttributeIndexDescriptor* descriptor,
        TObject* owner,
        std::vector<TIndexAttribute> indexedAttributes,
        std::vector<TIndexAttribute> predicateAttributes);

private:
    std::vector<TObjectKey> GetIndexKeysToAdd() const override;
    std::vector<TObjectKey> GetIndexKeysToRemove() const override;
};

////////////////////////////////////////////////////////////////////////////////

class TManyToOneViewAttribute;

template <class TMany, typename TOne>
struct TManyToOneViewAttributeDescriptor
{
    TManyToOneViewAttributeDescriptor(
        const TManyToOneAttributeDescriptor<TMany, TOne>& referenceDescriptor,
        std::function<const TManyToOneViewAttribute*(const TMany*)> viewAttributeGetter)
        : ReferenceDescriptor(referenceDescriptor)
        , ViewAttributeGetter(std::move(viewAttributeGetter))
    { }

    const TManyToOneAttributeDescriptor<TMany, TOne>& ReferenceDescriptor;
    const std::function<const TManyToOneViewAttribute*(const TMany*)> ViewAttributeGetter;
};

class TManyToManyInlineViewAttribute;

template <class TOwner, class TForeign>
struct TManyToManyInlineViewAttributeDescriptor
{
    TManyToManyInlineViewAttributeDescriptor(
        const TManyToManyInlineAttributeDescriptor<TOwner, TForeign>& referenceDescriptor,
        std::function<const TManyToManyInlineViewAttribute*(const TOwner*)> viewAttributeGetter)
        : ReferenceDescriptor(referenceDescriptor)
        , ViewAttributeGetter(std::move(viewAttributeGetter))
    { }

    const TManyToManyInlineAttributeDescriptor<TOwner, TForeign>& ReferenceDescriptor;
    const std::function<const TManyToManyInlineViewAttribute*(const TOwner*)> ViewAttributeGetter;
};

class TViewAttributeBase
{
public:
    TViewAttributeBase(const TObject* object);

    TResolveAttributeResult ResolveViewAttribute(
        const TAttributeSchema* schema,
        const NYPath::TYPath& path,
        bool many) const;

private:
    const TObject* const Owner_;
    mutable THashMap<NYPath::TYPath, TResolveAttributeResult> ResolveResults_;
};

class TManyToOneViewAttribute
    : public TViewAttributeBase
{
public:
    using TViewAttributeBase::TViewAttributeBase;

    TResolveAttributeResult ResolveViewAttribute(const TAttributeSchema* schema, const NYPath::TYPath& path) const;
};

class TManyToManyInlineViewAttribute
    : public TViewAttributeBase
{
public:
    using TViewAttributeBase::TViewAttributeBase;

    TResolveAttributeResult ResolveViewAttribute(const TAttributeSchema* schema, const NYPath::TYPath& path) const;
};

////////////////////////////////////////////////////////////////////////////////

template <class TOwner, class TForeign>
class TOneTransitiveAttribute;

template <class TOwner, class TForeign>
class TOneTransitiveAttributeDescriptor
    : public TScalarAttributeDescriptorBase
{
public:
    TOneTransitiveAttributeDescriptor(
        const TDBField* field,
        std::function<TOneTransitiveAttribute<TOwner, TForeign>*(TOwner*)> forwardAttributeGetter,
        std::function<void(TOwner*)> initialForeignLoader)
        : TScalarAttributeDescriptorBase(field)
        , ForwardAttributeGetter_(std::move(forwardAttributeGetter))
        , InitialForeignLoader_(std::move(initialForeignLoader))
    { }

    TOneTransitiveAttribute<TOwner, TForeign>* ForwardAttributeGetter(TOwner* owner) const;

    TOneTransitiveAttribute<TOwner, TForeign>* ForwardAttributeGetter(const TOwner* owner) const;

    void InitialForeignLoader(TOwner* owner) const;

private:
    std::function<TOneTransitiveAttribute<TOwner, TForeign>*(TOwner*)> ForwardAttributeGetter_;
    std::function<void(TOwner*)> InitialForeignLoader_;
};

template <class TOwner, class TForeign>
class TOneTransitiveAttribute
    : public TAttributeBase
    , public IScalarAttributeBaseHolder
{
public:
    using TDescriptor = TOneTransitiveAttributeDescriptor<TOwner, TForeign>;

    TOneTransitiveAttribute(TOwner* owner, const TDescriptor* descriptor);

    void ScheduleLoad() const;
    TObjectPlugin<TForeign>* Load(std::source_location location = std::source_location::current()) const;
    TObjectKey LoadKey(std::source_location location = std::source_location::current()) const;

    void ScheduleLoadTimestamp() const;
    TTimestamp LoadTimestamp(std::source_location location = std::source_location::current()) const;

    bool IsStoreScheduled() const override;

    const TScalarAttributeBase* GetAttributeForIndex() const override;

    void StoreInitial(TForeign* foreign);

private:
    using TForeignKeyFirstField = std::tuple_element_t<0, typename TObjectKeyTraits<TForeign>::TTypes>;

    TOwner* const TypedOwner_;

    const TDescriptor* const Descriptor_;
    const TScalarAttributeDescriptor<TOwner, TForeignKeyFirstField> UnderlyingDescriptor_;

    TScalarAttribute<TForeignKeyFirstField> Underlying_;

    TForeign* IdToForeign(const TForeignKeyFirstField& id) const;

    void OnObjectInitialization() override;

    void Store(TForeign* foreign);
};

////////////////////////////////////////////////////////////////////////////////

// NB: ORM-master code relies on static storage duration of TScalarAggregatedAttributeDescriptor objects.
template <class TTypedObject, class TTypedValue>
class TScalarAggregatedAttributeDescriptor
    : public TScalarAttributeDescriptorBase
{
public:
    TScalarAggregatedAttributeDescriptor(
        const TDBField* field,
        std::function<TScalarAggregatedAttribute<TTypedValue>*(TTypedObject*)> attributeGetter)
        : TScalarAttributeDescriptorBase(field)
        , AttributeGetter_(std::move(attributeGetter))
    { }

    const TScalarAggregatedAttributeDescriptor& AddValidator(
        std::function<void(TTransaction*, const TTypedObject*, const TTypedValue&)> validator) const;

    TScalarAggregatedAttribute<TTypedValue>* AttributeGetter(TTypedObject* typedObject) const;

    const TScalarAggregatedAttribute<TTypedValue>* AttributeGetter(const TTypedObject* typedObject) const;

    TScalarAttributeBase* AttributeBaseGetter(TObject* object) const override;

    void RunFinalValueValidators(const TObject* object) const override;

private:
    const std::function<TScalarAggregatedAttribute<TTypedValue>*(TTypedObject*)> AttributeGetter_;
    mutable std::vector<std::function<void(TTransaction*, const TTypedObject*, const TTypedValue&)>> NewValueValidators_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TTypedValue>
class TScalarAggregatedAttribute
    : public TScalarAttributeBase
{
public:
    TScalarAggregatedAttribute(
        TObject* owner,
        const TScalarAttributeDescriptorBase* descriptor);

    const TTypedValue& Load(std::source_location location = std::source_location::current()) const;
    const TTypedValue& LoadOld(std::source_location location = std::source_location::current()) const;

    void Store(
        TTypedValue value,
        EAggregateMode aggregateMode,
        std::optional<bool> sharedWrite = std::nullopt,
        std::source_location location = std::source_location::current());

    void StoreDefault(
        EAggregateMode aggregateMode,
        std::optional<bool> sharedWrite = std::nullopt,
        std::source_location location = std::source_location::current());

    TObjectKey::TKeyField LoadAsKeyField(const TString& suffix) const override;
    TObjectKey::TKeyField LoadAsKeyFieldOld(const TString& suffix) const override;
    std::vector<TObjectKey::TKeyField> LoadAsKeyFields(const TString& suffix) const override;
    std::vector<TObjectKey::TKeyField> LoadAsKeyFieldsOld(const TString& suffix) const override;

    void StoreKeyField(TObjectKey::TKeyField keyField, const TString& suffix) override;
    void StoreKeyFields(TObjectKey::TKeyFields keyFields, const TString& suffix) override;

    TConstProtosView LoadAsProtos(bool old) const override;
    TProtosView MutableLoadAsProtos() override;

private:
    const int ColumnId_;

    struct TRowBufferTag
    { };
    const NTableClient::TRowBufferPtr RowBuffer_;

    // Use lazy initialization because of expensive evaluator copying and situations where a merger is not needed.
    // Can not use TLazyIntrusivePtr since TUnversionedRowMerger is not refcounted class.
    class TMergerHolder
    {
    private:
        using TFactory = std::function<NTableClient::TUnversionedRowMerger()>;

    public:
        TMergerHolder(TFactory factory);

        NTableClient::TUnversionedRowMerger& GetUnderlying();

    private:
        const TFactory Factory_;
        std::optional<NTableClient::TUnversionedRowMerger> Merger_;
    };

    mutable TMergerHolder MergerHolder_;

    struct TAggregate
    {
        TTypedValue TypedValue;
        NTableClient::TUnversionedRow UnversionedRow;
    };

    // Attribute accumulates changes, merges them if necessary and saves them.

    // If you make several Stores within the same StoreContext, without calling Load() (Current = ∅), then after the
    // context flush, the aggregates will be empty. It turns out that if we do not save the aggregated values of the
    // previous context anywhere before the flush, we will not be able to return the correct Load() result in the next
    // one. ShouldTakeIntoAccountOnLoad is used as such storage.

    mutable std::optional<TAggregate> Old_;
    mutable std::optional<TAggregate> Current_;

    mutable std::optional<NTableClient::TUnversionedRow> DeltaForCurrent_;
    mutable std::optional<NTableClient::TUnversionedRow> DeltaForDb_;

    mutable std::optional<TAggregate> Override_;

    mutable std::vector<TTypedValue> Pending_;

    void Aggregate(TTypedValue&& value, bool sharedWrite, std::source_location location);
    void Override(TTypedValue&& value, bool sharedWrite, std::source_location location);

    void SetDefaultValues() override;

    void LoadOldValue(const NTableClient::TUnversionedValue& value, ILoadContext* context) override;
    void StoreNewValue(NTableClient::TUnversionedValue* dbValue, IStoreContext* context) override;

    NTableClient::TMutableUnversionedRow ValueToRow(NTableClient::TUnversionedValue value, bool captureValue) const;
    [[nodiscard]] NTableClient::TMutableUnversionedRow MergePending() const;
    NTableClient::TMutableUnversionedRow MergeRows(
        NTableClient::TUnversionedRow lhsRow,
        NTableClient::TUnversionedRow rhsRow) const;

    TAggregate MakeAggregate(TTypedValue typedValue) const;
    TAggregate MakeAggregate(NTableClient::TMutableUnversionedRow unversionedRow) const;

    void OnObjectInitialization() override;

    bool IsChanged(
        const NYPath::TYPath& path = "",
        std::source_location location = std::source_location::current()) const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
