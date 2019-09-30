#pragma once

#include "public.h"

#include <yt/client/table_client/versioned_row.h>
#include <yt/client/table_client/unversioned_row.h>

#include <yt/client/api/public.h>
#include <yt/client/api/client.h>

#include <yt/core/misc/optional.h>
#include <yt/core/misc/range.h>

#include <yt/core/yson/writer.h>

#include <yt/core/net/public.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

TRange<NYT::NTableClient::TUnversionedValue> CaptureCompositeObjectKey(
    const TObject* object,
    const NYT::NTableClient::TRowBufferPtr& rowBuffer);

////////////////////////////////////////////////////////////////////////////////

struct TDBField
{
    TString Name;
    NTableClient::EValueType Type;
};

struct TDBTable
{
    TDBTable(const TString& name)
        : Name(name)
    { }

    TString Name;
    std::vector<const TDBField*> Key;
};

////////////////////////////////////////////////////////////////////////////////

struct ILoadContext
{
    virtual ~ILoadContext() = default;

    virtual const NYT::NTableClient::TRowBufferPtr& GetRowBuffer() = 0;
    virtual TString GetTablePath(const TDBTable* table) = 0;

    virtual void ScheduleLookup(
        const TDBTable* table,
        TRange<NYT::NTableClient::TUnversionedValue> key,
        TRange<const TDBField*> fields,
        std::function<void(const std::optional<TRange<NYT::NTableClient::TVersionedValue>>&)> handler) = 0;

    virtual void ScheduleSelect(
        const TString& query,
        std::function<void(const NYT::NApi::IUnversionedRowsetPtr&)> handler) = 0;
};

struct IStoreContext
{
    virtual ~IStoreContext() = default;

    virtual const NYT::NTableClient::TRowBufferPtr& GetRowBuffer() = 0;

    virtual void WriteRow(
        const TDBTable* table,
        TRange<NYT::NTableClient::TUnversionedValue> key,
        TRange<const TDBField*> fields,
        TRange<NYT::NTableClient::TUnversionedValue> values) = 0;

    virtual void DeleteRow(
        const TDBTable* table,
        TRange<NYT::NTableClient::TUnversionedValue> key) = 0;
};

struct IPersistentAttribute
{
    virtual void OnObjectCreated() = 0;
    virtual void OnObjectRemoved() = 0;
};

struct ISession
{
    virtual ~ISession() = default;

    virtual IObjectTypeHandler* GetTypeHandler(EObjectType type) = 0;

    virtual TObject* CreateObject(EObjectType type, const TObjectId& id, const TObjectId& parentId) = 0;
    virtual TObject* GetObject(EObjectType type, const TObjectId& id, const TObjectId& parentId = {}) = 0;
    virtual void RemoveObject(TObject* object) = 0;

    static constexpr int ParentLoadPriority = 0;
    static constexpr int DefaultLoadPriority = 1;
    static constexpr int LoadPriorityCount = 2;
    using TLoadCallback = std::function<void(ILoadContext*)>;
    virtual void ScheduleLoad(TLoadCallback callback, int priority = DefaultLoadPriority) = 0;

    using TStoreCallback = std::function<void(IStoreContext*)>;
    virtual void ScheduleStore(TStoreCallback callback) = 0;

    virtual void FlushLoads() = 0;
};

class TObjectExistenceChecker
{
public:
    explicit TObjectExistenceChecker(TObject* object);

    TObject* GetObject() const;
    void ScheduleCheck() const;
    bool Check() const;

private:
    TObject* const Object_;

    bool CheckScheduled_ = false;
    bool Checked_ = false;
    bool Exists_ = false;

    void LoadFromDB(ILoadContext* context);
    void StoreToDB(IStoreContext* context);
};

class TObjectTombstoneChecker
{
public:
    explicit TObjectTombstoneChecker(TObject* object);

    void ScheduleCheck() const;
    bool Check() const;

private:
    TObject* const Object_;

    bool CheckScheduled_ = false;
    bool Checked_ = false;
    bool Tombstone_ = false;

    void LoadFromDB(ILoadContext* context);
    void StoreToDB(IStoreContext* context);
};

class TAttributeBase
    : public IPersistentAttribute
    , private TNonCopyable
{
public:
    explicit TAttributeBase(TObject* owner);

    TObject* GetOwner() const;

    bool IsStoreScheduled() const;

protected:
    TObject* const Owner_;

    void DoScheduleLoad(int priority = ISession::DefaultLoadPriority) const;
    void DoScheduleStore() const;

    virtual void LoadFromDB(ILoadContext* context);
    virtual void StoreToDB(IStoreContext* context);

    // IPersistentAttribute implementation
    virtual void OnObjectCreated() override;
    virtual void OnObjectRemoved() override;

private:
    bool LoadScheduled_ = false;
    bool StoreScheduled_ = false;
};

class TParentIdAttribute
    : public TAttributeBase
{
public:
    TParentIdAttribute(TObject* owner, const TObjectId& parentId);

    const TObjectId& GetId() const;

private:
    const bool NeedsParentId_;
    TObjectId ParentId_;
    bool Missing_ = false;


    // IPersistentAttribute implementation
    virtual void LoadFromDB(ILoadContext* context) override;
};

template <class T>
class TParentAttribute
{
public:
    explicit TParentAttribute(TObject* owner);

    T* Load() const;
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
    const THashSet<TObject*>& UntypedLoad() const;

protected:
    friend struct TChildrenAttributeHelper;

    std::optional<THashSet<TObject*>> Children_;
    THashSet<TObject*> AddedChildren_;
    THashSet<TObject*> RemovedChildren_;

    void DoAdd(TObject* child);
    void DoRemove(TObject* child);

    virtual EObjectType GetChildrenType() const = 0;

    // IPersistent implementation.
    virtual void LoadFromDB(ILoadContext* context) override;
};

template <class T>
class TChildrenAttribute
    : public TChildrenAttributeBase
{
public:
    explicit TChildrenAttribute(TObject* owner);

    std::vector<T*> Load() const;

private:
    virtual EObjectType GetChildrenType() const override;
};

struct TScalarAttributeSchemaBase
{
    explicit TScalarAttributeSchemaBase(const TDBField* field)
        : Field(field)
    { }

    const TDBField* Field;
};

class TScalarAttributeBase
    : public TAttributeBase
{
public:
    TScalarAttributeBase(
        TObject* owner,
        const TScalarAttributeSchemaBase* schema);

    void ScheduleLoad() const;

    void ScheduleLoadTimestamp() const;
    TTimestamp LoadTimestamp() const;

protected:
    const TScalarAttributeSchemaBase* const Schema_;

    mutable bool LoadScheduled_ = false;
    bool StoreScheduled_ = false;
    bool Missing_ = false;
    mutable bool Loaded_ = false;
    mutable std::optional<TTimestamp> Timestamp_;


    void ScheduleStore();

    void OnLoad() const;
    void OnStore();

    // IPersistentAttribute implementation
    virtual void LoadFromDB(ILoadContext* context) override;
    virtual void StoreToDB(IStoreContext* context) override;
    virtual void OnObjectCreated() override;

    virtual void SetDefaultValues() = 0;
    virtual void LoadOldValue(const NTableClient::TVersionedValue& value, ILoadContext* context) = 0;
    virtual void StoreNewValue(NTableClient::TUnversionedValue* dbValue, IStoreContext* context) = 0;
};

template <class TTypedObject, class TTypedValue>
struct TScalarAttributeSchema
    : public TScalarAttributeSchemaBase
{
    explicit TScalarAttributeSchema(
        const TDBField* field,
        std::function<TScalarAttribute<TTypedValue>*(TTypedObject*)> attributeGetter)
        : TScalarAttributeSchemaBase(field)
        , AttributeGetter(std::move(attributeGetter))
    { }

    TScalarAttributeSchema SetInitializer(std::function<void(TTransaction*, TTypedObject*, TTypedValue*)> initializer) const
    {
        auto result = *this;
        result.Initializer = std::move(initializer);
        return result;
    }

    TScalarAttributeSchema SetValidator(std::function<void(TTransaction*, TTypedObject*, const TTypedValue&, const TTypedValue&)> validator) const
    {
        auto result = *this;
        result.OldNewValueValidator = std::move(validator);
        return result;
    }

    TScalarAttributeSchema SetValidator(std::function<void(TTransaction*, TTypedObject*, const TTypedValue&)> validator) const
    {
        auto result = *this;
        result.NewValueValidator = std::move(validator);
        return result;
    }


    std::function<TScalarAttribute<TTypedValue>*(TTypedObject*)> AttributeGetter;
    std::function<void(TTransaction*, TTypedObject*, TTypedValue*)> Initializer;
    std::function<void(TTransaction*, TTypedObject*, const TTypedValue&, const TTypedValue&)> OldNewValueValidator;
    std::function<void(TTransaction*, TTypedObject*, const TTypedValue&)> NewValueValidator;
};

template <class T>
class TScalarAttribute
    : public TScalarAttributeBase
{
public:
    using TValue = T;

    TScalarAttribute(
        TObject* owner,
        const TScalarAttributeSchemaBase* schema);

    const T& Load() const;
    operator const T&() const;
    const T& LoadOld() const;

    bool IsChanged() const;

    void Store(const T& value);
    TScalarAttribute& operator = (const T& value);
    void Store(T&& value);
    TScalarAttribute& operator = (T&& value);

    T* Get();
    T* operator->();

private:
    std::optional<T> NewValue_;
    std::optional<T> OldValue_;


    virtual void SetDefaultValues() override;
    virtual void LoadOldValue(const NTableClient::TVersionedValue& value, ILoadContext* context) override;
    virtual void StoreNewValue(NTableClient::TUnversionedValue* dbValue, IStoreContext* context) override;
};

using TTimestampAttributeSchema = TScalarAttributeSchemaBase;

class TTimestampAttribute
    : public TScalarAttributeBase
{
public:
    TTimestampAttribute(
        TObject* owner,
        const TTimestampAttributeSchema* schema);

    TTimestamp Load() const;
    operator TTimestamp() const;

    void Touch();

private:
    TTimestamp Timestamp_ = NYT::NTransactionClient::NullTimestamp;


    virtual void SetDefaultValues() override;
    virtual void LoadOldValue(const NTableClient::TVersionedValue& value, ILoadContext* context) override;
    virtual void StoreNewValue(NTableClient::TUnversionedValue* dbValue, IStoreContext* context) override;
};

template <class TMany, class TOne>
struct TManyToOneAttributeSchema
{
    TManyToOneAttributeSchema(
        const TDBField* field,
        std::function<TManyToOneAttribute<TMany, TOne>*(TMany*)> forwardAttributeGetter,
        std::function<TOneToManyAttribute<TOne, TMany>*(TOne*)> inverseAttributeGetter)
        : Field(field)
        , ForwardAttributeGetter(std::move(forwardAttributeGetter))
        , InverseAttributeGetter(std::move(inverseAttributeGetter))
    { }

    const TDBField* Field;
    std::function<TManyToOneAttribute<TMany, TOne>*(TMany*)> ForwardAttributeGetter;
    std::function<TOneToManyAttribute<TOne, TMany>*(TOne*)> InverseAttributeGetter;
    bool Nullable = true;

    TManyToOneAttributeSchema SetNullable(bool value) const
    {
        auto result = *this;
        result.Nullable = value;
        return result;
    }
};

template <class TMany, class TOne>
class TManyToOneAttribute
    : public TAttributeBase
{
public:
    TManyToOneAttribute(
        TObject* owner,
        const TManyToOneAttributeSchema<TMany, TOne>* schema);

    void ScheduleLoad() const;

    TOne* Load() const;
    operator TOne*() const;
    TOne* LoadOld() const;

    void ScheduleLoadTimestamp() const;
    TTimestamp LoadTimestamp() const;

    bool IsChanged() const;

private:
    template <class TOne_, class TMany_>
    friend class TOneToManyAttribute;

    const TManyToOneAttributeSchema<TMany, TOne>* const Schema_;
    const TScalarAttributeSchema<TMany, TObjectId> UnderlyingSchema_;

    TScalarAttribute<TObjectId> Underlying_;

    TOne* IdToOne(const TObjectId& id) const;
    void Store(TOne* value);

    virtual void OnObjectRemoved() override;
};

struct TOneToManyAttributeSchemaBase
{
    TOneToManyAttributeSchemaBase(
        const TDBTable* table,
        const TDBField* primaryKeyField,
        const TDBField* foreignKeyField)
        : Table(table)
        , PrimaryKeyField(primaryKeyField)
        , ForeignKeyField(foreignKeyField)
    { }

    const TDBTable* Table;
    const TDBField* PrimaryKeyField;
    const TDBField* ForeignKeyField;
};

class TOneToManyAttributeBase
    : public TAttributeBase
{
public:
    TOneToManyAttributeBase(
        TObject* owner,
        const TOneToManyAttributeSchemaBase* schema);

    void ScheduleLoad() const;

protected:
    const TOneToManyAttributeSchemaBase* const Schema_;

    std::optional<THashSet<TObject*>> ForeignObjects_;
    THashSet<TObject*> AddedForeignObjects_;
    THashSet<TObject*> RemovedForeignObjects_;

    const THashSet<TObject*>& UntypedLoad() const;

    void DoAdd(TObject* many);
    void DoRemove(TObject* many);

    virtual EObjectType GetForeignObjectType() const = 0;

    // IPersistent implementation.
    virtual void LoadFromDB(ILoadContext* context) override;
    virtual void StoreToDB(IStoreContext* context) override;
};

template <class TOne, class TMany>
struct TOneToManyAttributeSchema
    : public TOneToManyAttributeSchemaBase
{
    TOneToManyAttributeSchema(
        const TDBTable* table,
        const TDBField* primaryKeyField,
        const TDBField* foreignKeyField,
        std::function<TOneToManyAttribute<TOne, TMany>*(TOne*)> forwardAttributeGetter,
        std::function<TManyToOneAttribute<TMany, TOne>*(TMany*)> inverseAttributeGetter)
        : TOneToManyAttributeSchemaBase(table, primaryKeyField, foreignKeyField)
        , ForwardAttributeGetter(std::move(forwardAttributeGetter))
        , InverseAttributeGetter(std::move(inverseAttributeGetter))
    { }

    std::function<TOneToManyAttribute<TOne, TMany>*(TOne*)> ForwardAttributeGetter;
    std::function<TManyToOneAttribute<TMany, TOne>*(TMany*)> InverseAttributeGetter;
};

template <class TOne, class TMany>
class TOneToManyAttribute
    : public TOneToManyAttributeBase
{
public:
    TOneToManyAttribute(
        TOne* owner,
        const TOneToManyAttributeSchema<TOne, TMany>* schema);

    std::vector<TMany*> Load() const;

    void Add(TMany* many);
    void Remove(TMany* many);

private:
    TOne* const TypedOwner_;
    const TOneToManyAttributeSchema<TOne, TMany>* const TypedSchema_;

    virtual EObjectType GetForeignObjectType() const;
    virtual void OnObjectRemoved() override;
};

class TAnnotationsAttribute
    : public TAttributeBase
{
public:
    explicit TAnnotationsAttribute(TObject* owner);

    void ScheduleLoad(const TString& key) const;
    NYT::NYson::TYsonString Load(const TString& key) const;

    void ScheduleLoadAll() const;
    std::vector<std::pair<TString, NYT::NYson::TYsonString>> LoadAll() const;

    void ScheduleLoadTimestamp(const TString& key) const;
    TTimestamp LoadTimestamp(const TString& key) const;

    void Store(const TString& key, const NYT::NYson::TYsonString& value);
    bool IsStoreScheduled(const TString& key) const;

private:
    // Per-key machinery.
    mutable THashSet<TString> ScheduledLoadKeys_;
    THashSet<TString> ScheduledStoreKeys_;
    // Null values indicate tombstones.
    THashMap<TString, NYT::NYson::TYsonString> KeyToValue_;
    // Null values indicate missing keys.
    THashMap<TString, TTimestamp> KeyToTimestamp_;

    // All-keys machinery.
    mutable bool ScheduledLoadAll_ = false;
    bool LoadedAll_ = false;
    THashMap<TString, NYT::NYson::TYsonString> AllKeysToValue_;

    // IPersistentAttribute implementation
    virtual void LoadFromDB(ILoadContext* context) override;
    virtual void StoreToDB(IStoreContext* context) override;
    virtual void OnObjectCreated() override;
    virtual void OnObjectRemoved() override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
