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
            Y_UNREACHABLE();
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
        Y_ASSERT(OldValue_);
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
    using NYT::NTableClient::FromUnversionedValue;
    using NYP::NServer::NObjects::FromUnversionedValue;
    FromUnversionedValue(&*OldValue_, static_cast<const NTableClient::TUnversionedValue&>(value));
}

template <class T>
void TScalarAttribute<T>::StoreNewValue(NTableClient::TUnversionedValue* dbValue, IStoreContext* context)
{
    using NYT::NTableClient::ToUnversionedValue;
    using NYP::NServer::NObjects::ToUnversionedValue;
    ToUnversionedValue(dbValue, *NewValue_, context->GetRowBuffer());
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

template <class T>
TExtensibleProto<T>::TExtensibleProto(const TExtensibleProto& other)
    : KnownAttributes_(std::move(other.KnownAttributes_))
    , UnknownAttributes_(other.UnknownAttributes_ ? other.UnknownAttributes_->Clone() : nullptr)
{ }

template <class T>
TExtensibleProto<T>::TExtensibleProto(TExtensibleProto&& other)
    : KnownAttributes_(std::move(other.KnownAttributes_))
    , UnknownAttributes_(std::move(other.UnknownAttributes_))
{ }

template <class T>
TExtensibleProto<T>::TExtensibleProto(
    T&& knownAttributes,
    std::unique_ptr<NYTree::IAttributeDictionary>&& unknownAttributes)
    : KnownAttributes_(std::move(knownAttributes))
    , UnknownAttributes_(std::move(unknownAttributes))
{ }

template <class T>
NYTree::IAttributeDictionary& TExtensibleProto<T>::UnknownAttributes()
{
    if (!UnknownAttributes_) {
        UnknownAttributes_ = NYTree::CreateEphemeralAttributes();
    }
    return *UnknownAttributes_;
}

template <class T>
const NYTree::IAttributeDictionary& TExtensibleProto<T>::UnknownAttributes() const
{
    return UnknownAttributes_ ? *UnknownAttributes_ : NYTree::EmptyAttributes();
}

template <class T>
TExtensibleProto<T>& TExtensibleProto<T>::operator=(const TExtensibleProto& other)
{
    KnownAttributes_ = other.KnownAttributes_;
    UnknownAttributes_ = other.UnknownAttributes_ ? other.UnknownAttributes_->Clone() : nullptr;
    return *this;
}

template <class T>
TExtensibleProto<T>& TExtensibleProto<T>::operator=(TExtensibleProto&& other)
{
    KnownAttributes_ = std::move(other.KnownAttributes_);
    UnknownAttributes_ = std::move(other.UnknownAttributes_);
    return *this;
}

template<class T>
T& TExtensibleProto<T>::KnownAttributes()
{
    return KnownAttributes_;
}

template<class T>
const T& TExtensibleProto<T>::KnownAttributes() const
{
    return KnownAttributes_;
}

std::unique_ptr<NYT::NYson::IYsonConsumer> CreateUnknownAttributesMergingConsumer(
    NYT::NYson::IYsonConsumer* underlying,
    const NYT::NYTree::IAttributeDictionary& unknownAttributes);

template <class T>
void Serialize(const TExtensibleProto<T>& proto, NYson::IYsonConsumer* consumer)
{
    auto mergingConsumer = CreateUnknownAttributesMergingConsumer(consumer, proto.UnknownAttributes());
    NYT::NYTree::Serialize(proto.KnownAttributes(), mergingConsumer.get());
}

std::unique_ptr<NYT::NYTree::IAttributeDictionary> DeserializeWithUnknownAttributes(
    google::protobuf::Message& message,
    const NYT::NYson::TProtobufMessageType* rootType,
    const NYT::NYTree::INodePtr& node);

template <class T>
void Deserialize(TExtensibleProto<T>& proto, const NYT::NYTree::INodePtr& node)
{
    T message;
    auto attributes = DeserializeWithUnknownAttributes(
        message,
        NYT::NYson::ReflectProtobufMessageType<T>(),
        node);
    proto = TExtensibleProto<T>(std::move(message), std::move(attributes));
}

void ExtensibleProtobufToUnversionedValueImpl(
    NYT::NTableClient::TUnversionedValue* unversionedValue,
    const google::protobuf::Message& knownAttributes,
    const NYT::NYTree::IAttributeDictionary& unknownAttributes,
    const NYT::NYson::TProtobufMessageType* type,
    const NYT::NTableClient::TRowBufferPtr& rowBuffer,
    int id);

template <class T>
void ToUnversionedValue(
    NYT::NTableClient::TUnversionedValue* unversionedValue,
    const T& value,
    const NYT::NTableClient::TRowBufferPtr& rowBuffer,
    int id,
    typename std::enable_if<std::is_convertible<T*, TExtensibleProtoTag*>::value, void>::type*)
{
    ExtensibleProtobufToUnversionedValueImpl(
        unversionedValue,
        value.KnownAttributes(),
        value.UnknownAttributes(),
        NYson::ReflectProtobufMessageType<typename T::TKnownAttributes>(),
        rowBuffer,
        id);
}

void UnversionedValueToExtensibleProtobufImpl(
    google::protobuf::Message* knownAttributes,
    NYT::NYTree::IAttributeDictionary* unknownAttributes,
    const NYT::NYson::TProtobufMessageType* type,
    NYT::NTableClient::TUnversionedValue unversionedValue);

template <class T>
void FromUnversionedValue(
    T* value,
    NYT::NTableClient::TUnversionedValue unversionedValue,
    typename std::enable_if<std::is_convertible<T*, TExtensibleProtoTag*>::value, void>::type*)
{
    UnversionedValueToExtensibleProtobufImpl(
        &value->KnownAttributes(),
        &value->UnknownAttributes(),
        NYT::NYson::ReflectProtobufMessageType<typename T::TKnownAttributes>(),
        unversionedValue);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
