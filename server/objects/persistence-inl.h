#pragma once
#ifndef PERSISTENCE_INL_H_
#error "Direct inclusion of this file is not allowed, include persistence.h"
// For the sake of sane code completion.
#include "persistence.h"
#endif

#include <yt/client/table_client/row_buffer.h>
#include <yt/client/table_client/helpers.h>

namespace NYP::NServer::NObjects {

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

template <class T, class = void>
struct TScalarAttributeTraits
{
    static bool Equals(const T& lhs, const T& rhs)
    {
        return lhs == rhs;
    }
};

template <class T>
struct TScalarAttributeTraits<
    T,
    typename std::enable_if<std::is_convertible<T*, google::protobuf::Message*>::value, void>::type
>
{
    static bool Equals(const T& lhs, const T& rhs)
    {
        return SerializeProtoToString(lhs) == SerializeProtoToString(rhs);
    }
};

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
    return LoadOld();
}

template <class T>
TScalarAttribute<T>::operator const T&() const
{
    return Load();
}

template <class T>
const T& TScalarAttribute<T>::LoadOld() const
{
    switch (Owner_->GetState()) {
        case EObjectState::Instantiated:
        case EObjectState::Removing:
        case EObjectState::Removed:
            OnLoad();
            return *OldValue_;

        case EObjectState::Creating:
        case EObjectState::Created:
        case EObjectState::CreatedRemoving:
        case EObjectState::CreatedRemoved: {
            static const T Default{};
            return Default;
        }

        default:
            YT_ABORT();
    }
}

template <class T>
bool TScalarAttribute<T>::IsChanged() const
{
    return NewValue_ && !TScalarAttributeTraits<T>::Equals(LoadOld(), *NewValue_);
}

template <class T>
void TScalarAttribute<T>::Store(const T& value)
{
    OnStore();
    NewValue_ = value;
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
    NewValue_.emplace(std::move(value));
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
        YT_ASSERT(OldValue_);
        NewValue_ = OldValue_;
    }
    return NewValue_ ? &*NewValue_ : nullptr;
}

template <class T>
T* TScalarAttribute<T>::operator->()
{
    return Get();
}

template <class T>
void TScalarAttribute<T>::SetDefaultValues()
{
    OldValue_.emplace();
    NewValue_.emplace();
}

template <class T>
void TScalarAttribute<T>::LoadOldValue(const NTableClient::TVersionedValue& value, ILoadContext* /*context*/)
{
    OldValue_.emplace();
    FromUnversionedValue(&*OldValue_, static_cast<const NTableClient::TUnversionedValue&>(value));
}

template <class T>
void TScalarAttribute<T>::StoreNewValue(NTableClient::TUnversionedValue* dbValue, IStoreContext* context)
{
    ToUnversionedValue(dbValue, *NewValue_, context->GetRowBuffer());
}

////////////////////////////////////////////////////////////////////////////////

template <class TThis, class TThat>
TOneToOneAttribute<TThis, TThat>::TOneToOneAttribute(
    TObject* owner,
    const TSchema* schema)
    : TAttributeBase(owner)
    , Schema_(schema)
    , UnderlyingSchema_(Schema_->Field, nullptr)
    , Underlying_(owner, &UnderlyingSchema_)
{ }

template <class TThis, class TThat>
TThat* TOneToOneAttribute<TThis, TThat>::Load() const
{
    return IdToThat(Underlying_.Load());
}

template <class TThis, class TThat>
TOneToOneAttribute<TThis, TThat>::operator TThat*() const
{
    return Load();
}

template <class TThis, class TThat>
TThat* TOneToOneAttribute<TThis, TThat>::LoadOld() const
{
    return IdToThat(Underlying_.LoadOld());
}

template <class TThis, class TThat>
void TOneToOneAttribute<TThis, TThat>::Store(TThat* value)
{
    auto* currentValue = Load();
    if (currentValue == value) {
        return;
    }

    if (currentValue) {
        Underlying_.Store(TObjectId());
        Schema_->InverseAttributeGetter(currentValue)->Underlying_.Store(Owner_->GetId());
    }

    if (value) {
        Underlying_.Store(value->GetId());
        Schema_->InverseAttributeGetter(value)->Underlying_.Store(Owner_->GetId());
    }
}

template <class TThis, class TThat>
TOneToOneAttribute<TThis, TThat>& TOneToOneAttribute<TThis, TThat>::operator=(TThat* value)
{
    Store(value);
    return *this;
}

template <class TThis, class TThat>
void TOneToOneAttribute<TThis, TThat>::ScheduleLoadTimestamp() const
{
    Underlying_.ScheduleLoadTimestamp();
}

template <class TThis, class TThat>
TTimestamp TOneToOneAttribute<TThis, TThat>::LoadTimestamp() const
{
    return Underlying_.LoadTimestamp();
}

template <class TThis, class TThat>
bool TOneToOneAttribute<TThis, TThat>::IsChanged() const
{
    return Underlying_.IsChanged();
}

template <class TThis, class TThat>
void TOneToOneAttribute<TThis, TThat>::ScheduleLoad() const
{
    Underlying_.ScheduleLoad();
}

template <class TThis, class TThat>
TThat* TOneToOneAttribute<TThis, TThat>::IdToThat(const TObjectId& id) const
{
    return id
        ? Underlying_.GetOwner()->GetSession()->GetObject(TThat::Type, id)->template As<TThat>()
        : nullptr;
}

template <class TThis, class TThat>
void TOneToOneAttribute<TThis, TThat>::OnObjectRemoved()
{
    // TODO(babenko): consider using preload
    if (auto* value = Load()) {
        Schema_->InverseAttributeGetter(value)->Underlying_.Store(TObjectId());
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TMany, class TOne>
TManyToOneAttribute<TMany, TOne>::TManyToOneAttribute(
    TObject* owner,
    const TSchema* schema)
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
void TManyToOneAttribute<TMany, TOne>::ScheduleLoadTimestamp() const
{
    Underlying_.ScheduleLoadTimestamp();
}

template <class TMany, class TOne>
TTimestamp TManyToOneAttribute<TMany, TOne>::LoadTimestamp() const
{
    return Underlying_.LoadTimestamp();
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
    if (auto* one = Load()) {
        Schema_->InverseAttributeGetter(one)->Remove(Owner_->As<TMany>());
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TOne, class TMany>
TOneToManyAttribute<TOne, TMany>::TOneToManyAttribute(
    TOne* owner,
    const TSchema* schema)
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
    YT_ASSERT(many);
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
    YT_ASSERT(many);
    auto* inverseAttribute = TypedSchema_->InverseAttributeGetter(many);
    YT_VERIFY(inverseAttribute->Load() == TypedOwner_);
    inverseAttribute->Store(nullptr);
    DoRemove(many);
}

template <class TOne, class TMany>
void TOneToManyAttribute<TOne, TMany>::Clear()
{
    for (auto* many : Load()) {
        Remove(many);
    }
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

} // namespace NYP::NServer::NObjects
