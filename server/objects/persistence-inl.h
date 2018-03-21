#pragma once
#ifndef PERSISTENCE_INL_H_
#error "Direct inclusion of this file is not allowed, include persistence.h"
#endif

#include <yt/ytlib/table_client/row_buffer.h>

#include <yt/core/yson/protobuf_interop.h>

#include <array>

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void ToDbValue(
    NYT::NTableClient::TUnversionedValue* dbValue,
    T value,
    const NYT::NTableClient::TRowBufferPtr& rowBuffer,
    int id,
    typename std::enable_if<TEnumTraits<T>::IsEnum, void>::type*)
{
    ToDbValue(dbValue, static_cast<i64>(value), rowBuffer, id);
}

template <class T>
void FromDbValue(
    T* value,
    const NYT::NTableClient::TUnversionedValue& dbValue,
    typename std::enable_if<TEnumTraits<T>::IsEnum, void>::type*)
{
    i64 rawValue;
    FromDbValue(&rawValue, dbValue);
    *value = static_cast<T>(rawValue);
}

////////////////////////////////////////////////////////////////////////////////

void ToDbValueImpl(
    NYT::NTableClient::TUnversionedValue* dbValue,
    const google::protobuf::Message& value,
    const NYT::NYson::TProtobufMessageType* type,
    const NYT::NTableClient::TRowBufferPtr& rowBuffer,
    int id);

template <class T>
void ToDbValue(
    NYT::NTableClient::TUnversionedValue* dbValue,
    const T& value,
    const NYT::NTableClient::TRowBufferPtr& rowBuffer,
    int id,
    typename std::enable_if<std::is_convertible<T*, google::protobuf::Message*>::value, void>::type*)
{
    ToDbValueImpl(
        dbValue,
        value,
        NYT::NYson::ReflectProtobufMessageType<T>(),
        rowBuffer,
        id);
}

void FromDbValueImpl(
    google::protobuf::Message* value,
    const NYT::NYson::TProtobufMessageType* type,
    const NYT::NTableClient::TUnversionedValue& dbValue);

template <class T>
void FromDbValue(
    T* value,
    const NYT::NTableClient::TUnversionedValue& dbValue,
    typename std::enable_if<std::is_convertible<T*, google::protobuf::Message*>::value, void>::type*)
{
    FromDbValueImpl(
        value,
        NYT::NYson::ReflectProtobufMessageType<T>(),
        dbValue);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void ToDbValue(
    NYT::NTableClient::TUnversionedValue* dbValue,
    const TNullable<T>& value,
    const NYT::NTableClient::TRowBufferPtr& rowBuffer,
    int id)
{
    if (value) {
        ToDbValue(dbValue, *value, rowBuffer, id);
    } else {
        *dbValue = NYT::NTableClient::MakeUnversionedSentinelValue(NYT::NTableClient::EValueType::Null, id);
    }
}

template <class T>
void FromDbValue(
    TNullable<T>* value,
    const NYT::NTableClient::TUnversionedValue& dbValue)
{
    if (dbValue.Type == NYT::NTableClient::EValueType::Null) {
        *value = Null;
    } else {
        value->Emplace();
        FromDbValue(value->GetPtr(), dbValue);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ToDbValueImpl(
    NYT::NTableClient::TUnversionedValue* dbValue,
    const std::function<bool(NYT::NTableClient::TUnversionedValue*)> producer,
    const NYT::NTableClient::TRowBufferPtr& rowBuffer,
    int id);

template <class T>
void ToDbValue(
    NYT::NTableClient::TUnversionedValue* dbValue,
    const std::vector<T>& values,
    const NYT::NTableClient::TRowBufferPtr& rowBuffer,
    int id)
{
    size_t index = 0;
    ToDbValueImpl(
        dbValue,
        [&] (NYT::NTableClient::TUnversionedValue* itemValue) mutable -> bool {
            if (index == values.size()) {
                return false;
            }
            ToDbValue(itemValue, values[index++], rowBuffer);
            return true;
        },
        rowBuffer,
        id);
}

void FromDbValueImpl(
    std::function<google::protobuf::Message*()> appender,
    const NYT::NYson::TProtobufMessageType* type,
    const NYT::NTableClient::TUnversionedValue& dbValue);

template <class T>
void FromDbValue(
    std::vector<T>* values,
    const NYT::NTableClient::TUnversionedValue& dbValue,
    typename std::enable_if<std::is_convertible<T*, google::protobuf::Message*>::value, void>::type*)
{
    values->clear();
    FromDbValueImpl(
        [&] {
            values->emplace_back();
            return &values->back();
        },
        NYT::NYson::ReflectProtobufMessageType<T>(),
        dbValue);
}

void FromDbValueImpl(
    std::function<void(const NYT::NTableClient::TUnversionedValue&)> appender,
    const NYT::NTableClient::TUnversionedValue& dbValue);

template <class T>
void FromDbValue(
    std::vector<T>* values,
    const NYT::NTableClient::TUnversionedValue& dbValue,
    typename std::enable_if<TIsScalarPersistentType<T>::Value, void>::type*)
{
    values->clear();
    FromDbValueImpl(
        [&] (const NYT::NTableClient::TUnversionedValue& itemValue) {
            values->emplace_back();
            FromDbValue(&values->back(), itemValue);
        },
        dbValue);
}

template <size_t Index, class... Ts>
struct TToDbValuesTraits;

template <size_t Index>
struct TToDbValuesTraits<Index>
{
    template <class V>
    static void Do(V*, const NYT::NTableClient::TRowBufferPtr&)
    { }
};

template <size_t Index, class T, class... Ts>
struct TToDbValuesTraits<Index, T, Ts...>
{
    template <class V>
    static void Do(V* array, const NYT::NTableClient::TRowBufferPtr& rowBuffer, const T& head, const Ts&... tail)
    {
        ToDbValue(&(*array)[Index], head, rowBuffer);
        TToDbValuesTraits<Index + 1, Ts...>::Do(array, rowBuffer, tail...);
    }
};

template <class... Ts>
auto ToDbValues(
    const NYT::NTableClient::TRowBufferPtr& rowBuffer,
    const Ts& ... values)
    -> std::array<NYT::NTableClient::TUnversionedValue, sizeof...(Ts)>
{
    std::array<NYT::NTableClient::TUnversionedValue, sizeof...(Ts)> array;
    TToDbValuesTraits<0, Ts...>::Do(&array, rowBuffer, values...);
    return array;
}

template <size_t Index, class... Ts>
struct TFromDbRowTraits;

template <size_t Index>
struct TFromDbRowTraits<Index>
{
    static void Do(NYT::NTableClient::TUnversionedRow)
    { }
};

template <size_t Index, class T, class... Ts>
struct TFromDbRowTraits<Index, T, Ts...>
{
    static void Do(NYT::NTableClient::TUnversionedRow row, T* head, Ts*... tail)
    {
        FromDbValue(head, row[Index]);
        TFromDbRowTraits<Index + 1, Ts...>::Do(row , tail...);
    }
};

template <class... Ts>
void FromDbRow(
    NYT::NTableClient::TUnversionedRow row,
    Ts*... values)
{
    if (row.GetCount() != sizeof...(Ts)) {
        THROW_ERROR_EXCEPTION("Invalid number of values in row: expected %v, got %v",
            sizeof...(Ts),
            row.GetCount());
    }
    TFromDbRowTraits<0, Ts...>::Do(row, values...);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
NYT::NTableClient::TUnversionedValue ToDbValue(const T& value, const NYT::NTableClient::TRowBufferPtr& rowBuffer, int id)
{
    NYT::NTableClient::TUnversionedValue dbValue;
    ToDbValue(&dbValue, value, rowBuffer, id);
    return dbValue;
}

template <class T>
T FromDbValue(const NYT::NTableClient::TUnversionedValue& dbValue)
{
    T value;
    FromDbValue(&value, dbValue);
    return value;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TParentAttribute<T>::TParentAttribute(TObject* owner)
    : Owner_(owner)
{ }

template <class T>
T* TParentAttribute<T>::Load() const
{
    auto* session = Owner_->GetSession();
    const auto& id = Owner_->GetParentId();
    return session->GetObject(T::Type, id)->template As<T>();
}

template <class T>
TParentAttribute<T>::operator T*() const
{
    return Load();
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TChildrenAttribute<T>::TChildrenAttribute(TObject* owner)
    : TChildrenAttributeBase(owner)
{ }

template <class T>
std::vector<T*> TChildrenAttribute<T>::Load() const
{
    const auto& untypedResult = UntypedLoad();
    std::vector<T*> result;
    result.reserve(untypedResult.size());
    for (auto* untypedObject : untypedResult) {
        result.push_back(untypedObject->template As<T>());
    }
    return result;
}

template <class T>
EObjectType TChildrenAttribute<T>::GetChildrenType() const
{
    return T::Type;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TScalarAttribute<T>::TScalarAttribute(TObject* owner, const TScalarAttributeSchemaBase* schema)
    : TScalarAttributeBase(owner, schema)
{ }

template <class T>
const T& TScalarAttribute<T>::Load() const
{
    if (NewValue_) {
        return *NewValue_;
    }
    OnLoad();
    return *OldValue_;
}

template <class T>
TScalarAttribute<T>::operator const T&() const
{
    return Load();
}

template <class T>
const T& TScalarAttribute<T>::LoadOld() const
{
    OnLoad();
    return *OldValue_;
}

template <class T>
bool TScalarAttribute<T>::IsChanged() const
{
    return NewValue_ && LoadOld() != *NewValue_;
}

template <class T>
void TScalarAttribute<T>::Store(const T& value)
{
    OnStore();
    NewValue_.Assign(value);
}

template <class T>
TScalarAttribute<T>& TScalarAttribute<T>::operator=(const T& value)
{
    Store(value);
    return *this;
}

template <class T>
void TScalarAttribute<T>::Store(T&& value)
{
    OnStore();
    // TODO(babenko): protobuf messages currently do not support move semantics
    NewValue_.Emplace();
    std::swap(*NewValue_, value);
}

template <class T>
TScalarAttribute<T>& TScalarAttribute<T>::operator=(T&& value)
{
    Store(std::move(value));
    return *this;
}

template <class T>
T* TScalarAttribute<T>::Get()
{
    OnStore();
    if (!NewValue_) {
        Load();
        Y_ASSERT(OldValue_);
        NewValue_ = OldValue_;
    }
    return NewValue_.GetPtr();
}

template <class T>
T* TScalarAttribute<T>::operator->()
{
    return Get();
}

template <class T>
void TScalarAttribute<T>::SetDefaultValues()
{
    OldValue_.Emplace();
    NewValue_.Emplace();
}

template <class T>
void TScalarAttribute<T>::LoadOldValue(const NTableClient::TVersionedValue& value, ILoadContext* /*context*/)
{
    OldValue_.Emplace();
    FromDbValue(OldValue_.GetPtr(), static_cast<const NTableClient::TUnversionedValue&>(value));
}

template <class T>
void TScalarAttribute<T>::StoreNewValue(NTableClient::TUnversionedValue* dbValue, IStoreContext* context)
{
    ToDbValue(dbValue, *NewValue_, context->GetRowBuffer());
}

////////////////////////////////////////////////////////////////////////////////

template <class TMany, class TOne>
TManyToOneAttribute<TMany, TOne>::TManyToOneAttribute(
    TObject* owner,
    const TManyToOneAttributeSchema<TMany, TOne>* schema)
    : TAttributeBase(owner)
    , Schema_(schema)
    , UnderlyingSchema_(Schema_->Field, nullptr)
    , Underlying_(owner, &UnderlyingSchema_)
{ }

template <class TMany, class TOne>
TOne* TManyToOneAttribute<TMany, TOne>::Load() const
{
    return IdToOne(Underlying_.Load());
}

template <class TMany, class TOne>
TManyToOneAttribute<TMany, TOne>::operator TOne*() const
{
    return Load();
}

template <class TMany, class TOne>
TOne* TManyToOneAttribute<TMany, TOne>::LoadOld() const
{
    return IdToOne(Underlying_.LoadOld());
}

template <class TMany, class TOne>
bool TManyToOneAttribute<TMany, TOne>::IsChanged() const
{
    return Underlying_.IsChanged();
}

template <class TMany, class TOne>
void TManyToOneAttribute<TMany, TOne>::ScheduleLoad() const
{
    Underlying_.ScheduleLoad();
}

template <class TMany, class TOne>
TOne* TManyToOneAttribute<TMany, TOne>::IdToOne(const TObjectId& id) const
{
    return id
        ? Underlying_.GetOwner()->GetSession()->GetObject(TOne::Type, id)->template As<TOne>()
        : nullptr;
}

template <class TMany, class TOne>
void TManyToOneAttribute<TMany, TOne>::Store(TOne* value)
{
    Underlying_.Store(GetObjectId(value));
}

template <class TMany, class TOne>
void TManyToOneAttribute<TMany, TOne>::OnObjectRemoved()
{
    // TODO(babenko): consider using preload
    auto* one = Load();
    if (one) {
        auto* inverseAttribute = Schema_->InverseAttributeGetter(one);
        inverseAttribute->Remove(Owner_->As<TMany>());
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TOne, class TMany>
TOneToManyAttribute<TOne, TMany>::TOneToManyAttribute(
    TOne* owner,
    const TOneToManyAttributeSchema<TOne, TMany>* schema)
    : TOneToManyAttributeBase(owner, schema)
    , TypedOwner_(owner)
    , TypedSchema_(schema)
{ }

template <class TOne, class TMany>
std::vector<TMany*> TOneToManyAttribute<TOne, TMany>::Load() const
{
    const auto& untypedResult = UntypedLoad();
    std::vector<TMany*> result;
    result.reserve(untypedResult.size());
    for (auto* untypedObject : untypedResult) {
        result.push_back(untypedObject->template As<TMany>());
    }
    return result;
}

template <class TOne, class TMany>
void TOneToManyAttribute<TOne, TMany>::Add(TMany* many)
{
    Y_ASSERT(many);
    auto* inverseAttribute = TypedSchema_->InverseAttributeGetter(many);
    auto* currentOne = inverseAttribute->Load();
    if (currentOne == Owner_) {
        return;
    }
    if (currentOne) {
        auto* forwardAttribute = TypedSchema_->ForwardAttributeGetter(currentOne);
        forwardAttribute->Remove(many);
    }
    inverseAttribute->Store(TypedOwner_);
    DoAdd(many);
}

template <class TOne, class TMany>
void TOneToManyAttribute<TOne, TMany>::Remove(TMany* many)
{
    Y_ASSERT(many);
    auto* inverseAttribute = TypedSchema_->InverseAttributeGetter(many);
    YCHECK(inverseAttribute->Load() == TypedOwner_);
    inverseAttribute->Store(nullptr);
    DoRemove(many);
}

template <class TOne, class TMany>
EObjectType TOneToManyAttribute<TOne, TMany>::GetForeignObjectType() const
{
    return TMany::Type;
}

template <class TOne, class TMany>
void TOneToManyAttribute<TOne, TMany>::OnObjectRemoved()
{
    for (auto* many : Load()) {
        Remove(many);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP
