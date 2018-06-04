#pragma once

#include "public.h"

#include <yt/ytlib/table_client/versioned_row.h>
#include <yt/ytlib/table_client/unversioned_row.h>

#include <yt/ytlib/api/public.h>

#include <yt/core/misc/nullable.h>
#include <yt/core/misc/range.h>

#include <yt/core/yson/writer.h>

#include <yt/core/net/public.h>

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////
// TODO(babenko): move to yt

template <class T>
struct TIsScalarPersistentType
{
    static constexpr bool Value =
        std::is_same<T, TGuid>::value ||
        std::is_same<T, TString>::value ||
        std::is_same<T, i64>::value ||
        std::is_same<T, ui64>::value ||
        std::is_same<T, TInstant>::value;
};

void ToDBValue(NYT::NTableClient::TUnversionedValue* dbValue, const TGuid& value, const NYT::NTableClient::TRowBufferPtr& rowBuffer, int id = 0);
void FromDBValue(TGuid* value, const NYT::NTableClient::TUnversionedValue& dbValue);

void ToDBValue(NYT::NTableClient::TUnversionedValue* dbValue, const TString& value, const NYT::NTableClient::TRowBufferPtr& rowBuffer, int id = 0);
void FromDBValue(TString* value, const NYT::NTableClient::TUnversionedValue& dbValue);

void ToDBValue(NYT::NTableClient::TUnversionedValue* dbValue, bool value, const NYT::NTableClient::TRowBufferPtr& rowBuffer, int id = 0);
void FromDBValue(bool* value, const NYT::NTableClient::TUnversionedValue& dbValue);

void ToDBValue(NYT::NTableClient::TUnversionedValue* dbValue, const NYT::NYson::TYsonString& value, const NYT::NTableClient::TRowBufferPtr& rowBuffer, int id = 0);
void FromDBValue(NYT::NYson::TYsonString* value, const NYT::NTableClient::TUnversionedValue& dbValue);

void ToDBValue(NYT::NTableClient::TUnversionedValue* dbValue, i64 value, const NYT::NTableClient::TRowBufferPtr& rowBuffer, int id = 0);
void FromDBValue(i64* value, const NYT::NTableClient::TUnversionedValue& dbValue);

void ToDBValue(NYT::NTableClient::TUnversionedValue* dbValue, ui64 value, const NYT::NTableClient::TRowBufferPtr& rowBuffer, int id = 0);
void FromDBValue(ui64* value, const NYT::NTableClient::TUnversionedValue& dbValue);

void ToDBValue(NYT::NTableClient::TUnversionedValue* dbValue, ui32 value, const NYT::NTableClient::TRowBufferPtr& rowBuffer, int id = 0);
void FromDBValue(ui32* value, const NYT::NTableClient::TUnversionedValue& dbValue);

void ToDBValue(NYT::NTableClient::TUnversionedValue* dbValue, ui16 value, const NYT::NTableClient::TRowBufferPtr& rowBuffer, int id = 0);
void FromDBValue(ui16* value, const NYT::NTableClient::TUnversionedValue& dbValue);

void ToDBValue(NYT::NTableClient::TUnversionedValue* dbValue, double value, const NYT::NTableClient::TRowBufferPtr& rowBuffer, int id = 0);
void FromDBValue(double* value, const NYT::NTableClient::TUnversionedValue& dbValue);

void ToDBValue(NYT::NTableClient::TUnversionedValue* dbValue, TInstant value, const NYT::NTableClient::TRowBufferPtr& rowBuffer, int id = 0);
void FromDBValue(TInstant* value, const NYT::NTableClient::TUnversionedValue& dbValue);

void ToDBValue(NYT::NTableClient::TUnversionedValue* dbValue, const NYT::NYTree::IMapNodePtr& value, const NYT::NTableClient::TRowBufferPtr& rowBuffer, int id = 0);
void FromDBValue(NYT::NYTree::IMapNodePtr* value, const NYT::NTableClient::TUnversionedValue& dbValue);

void ToDBValue(NYT::NTableClient::TUnversionedValue* dbValue, const NYT::NNet::TIP6Address& value, const NYT::NTableClient::TRowBufferPtr& rowBuffer, int id = 0);
void FromDBValue(NYT::NNet::TIP6Address* value, const NYT::NTableClient::TUnversionedValue& dbValue);

template <class T>
void ToDBValue(
    NYT::NTableClient::TUnversionedValue* dbValue,
    T value,
    const NYT::NTableClient::TRowBufferPtr& rowBuffer,
    int id = 0,
    typename std::enable_if<TEnumTraits<T>::IsEnum, void>::type* = nullptr);
template <class T>
void FromDBValue(
    T* value,
    const NYT::NTableClient::TUnversionedValue& dbValue,
    typename std::enable_if<TEnumTraits<T>::IsEnum, void>::type* = nullptr);

template <class T>
NYT::NTableClient::TUnversionedValue ToDBValue(const T& value, const NYT::NTableClient::TRowBufferPtr& rowBuffer, int id = 0);
template <class T>
T FromDBValue(const NYT::NTableClient::TUnversionedValue& dbValue);

template <class T>
void ToDBValue(
    NYT::NTableClient::TUnversionedValue* dbValue,
    const T& value,
    const NYT::NTableClient::TRowBufferPtr& rowBuffer,
    int id = 0,
    typename std::enable_if<std::is_convertible<T*, ::google::protobuf::Message*>::value, void>::type* = nullptr);
template <class T>
void FromDBValue(
    T* value,
    const NYT::NTableClient::TUnversionedValue& dbValue,
    typename std::enable_if<std::is_convertible<T*, ::google::protobuf::Message*>::value, void>::type* = nullptr);

template <class T>
void ToDBValue(
    NYT::NTableClient::TUnversionedValue* dbValue,
    const TNullable<T>& value,
    const NYT::NTableClient::TRowBufferPtr& rowBuffer,
    int id = 0);
template <class T>
void FromDBValue(
    TNullable<T>* value,
    const NYT::NTableClient::TUnversionedValue& dbValue);

template <class T>
void ToDBValue(
    NYT::NTableClient::TUnversionedValue* dbValue,
    const std::vector<T>& values,
    const NYT::NTableClient::TRowBufferPtr& rowBuffer,
    int id = 0);
template <class T>
void FromDBValue(
    std::vector<T>* values,
    const NYT::NTableClient::TUnversionedValue& dbValue,
    typename std::enable_if<std::is_convertible<T*, ::google::protobuf::Message*>::value, void>::type* = nullptr);
template <class T>
void FromDBValue(
    std::vector<T>* values,
    const NYT::NTableClient::TUnversionedValue& dbValue,
    typename std::enable_if<TIsScalarPersistentType<T>::Value, void>::type* = nullptr);

template <class... Ts>
auto ToDBValues(
    const NYT::NTableClient::TRowBufferPtr& rowBuffer,
    const Ts& ... values)
    -> std::array<NYT::NTableClient::TUnversionedValue, sizeof...(Ts)>;

template <class... Ts>
void FromDBRow(
    NYT::NTableClient::TUnversionedRow row,
    Ts*... values);

void DBValueToYson(const NYT::NTableClient::TUnversionedValue& dbValue, NYT::NYson::IYsonConsumer* consumer);
NYT::NYson::TYsonString DBValueToYson(const NYT::NTableClient::TUnversionedValue& dbValue);

TRange<NYT::NTableClient::TUnversionedValue> CaptureCompositeObjectKey(
    const TObject* object,
    const NYT::NTableClient::TRowBufferPtr& rowBuffer);

////////////////////////////////////////////////////////////////////////////////

struct TDBField
{
    TString Name;
};

struct TDBTable
{
    explicit TDBTable(const TString& name)
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
        const TRange<NYT::NTableClient::TUnversionedValue>& key,
        const TRange<const TDBField*>& fields,
        std::function<void(const TNullable<TRange<NYT::NTableClient::TVersionedValue>>&)> handler) = 0;

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
        const TRange<NYT::NTableClient::TUnversionedValue>& key,
        const TRange<const TDBField*>& fields,
        const TRange<NYT::NTableClient::TUnversionedValue>& values) = 0;

    virtual void DeleteRow(
        const TDBTable* table,
        const TRange<NYT::NTableClient::TUnversionedValue>& key) = 0;
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
    bool Exists_;

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

protected:
    TObject* const Owner_;

    void ThrowObjectMissing() const;

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

    TNullable<THashSet<TObject*>> Children_;
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

protected:
    const TScalarAttributeSchemaBase* const Schema_;

    mutable bool LoadScheduled_ = false;
    bool StoreScheduled_ = false;
    bool Missing_ = false;
    mutable bool Loaded_ = false;


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
        std::function<TScalarAttribute<TTypedValue>*(TTypedObject*)> attributeGetter = nullptr)
        : TScalarAttributeSchemaBase(field)
        , AttributeGetter(std::move(attributeGetter))
    { }

    TScalarAttributeSchema SetInitializer(std::function<void(const TTransactionPtr&, TTypedObject*, TTypedValue*)> initializer) const
    {
        auto result = *this;
        result.Initializer = std::move(initializer);
        return result;
    }

    TScalarAttributeSchema SetValidator(std::function<void(const TTypedValue&, const TTypedValue&)> validator) const
    {
        auto result = *this;
        result.OldNewValueValidator = std::move(validator);
        return result;
    }

    TScalarAttributeSchema SetValidator(std::function<void(const TTypedValue&)> validator) const
    {
        auto result = *this;
        result.NewValueValidator = std::move(validator);
        return result;
    }


    std::function<TScalarAttribute<TTypedValue>*(TTypedObject*)> AttributeGetter;
    std::function<void(const TTransactionPtr&, TTypedObject*, TTypedValue*)> Initializer;
    std::function<void(const TTypedValue&, const TTypedValue&)> OldNewValueValidator;
    std::function<void(const TTypedValue&)> NewValueValidator;
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
    TNullable<T> NewValue_;
    TNullable<T> OldValue_;


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

    TNullable<THashSet<TObject*>> ForeignObjects_;
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
    TNullable<NYT::NYson::TYsonString> Load(const TString& key) const;

    void ScheduleLoadAll() const;
    std::vector<std::pair<TString, NYT::NYson::TYsonString>> LoadAll() const;

    void Store(const TString& key, const TNullable<NYT::NYson::TYsonString>& value);

private:
    mutable THashSet<TString> ScheduledLoadKeys_;
    mutable bool ScheduledLoadAll_ = false;
    THashSet<TString> ScheduledStoreKeys_;
    bool LoadedAll_ = false;
    THashMap<TString, TNullable<NYT::NYson::TYsonString>> KeyToValue_;

    // IPersistentAttribute implementation
    virtual void LoadFromDB(ILoadContext* context) override;
    virtual void StoreToDB(IStoreContext* context) override;
    virtual void OnObjectCreated() override;
    virtual void OnObjectRemoved() override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP
