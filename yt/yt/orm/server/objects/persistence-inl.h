#include <yt/yt/orm/server/objects/public.h>
#ifndef PERSISTENCE_INL_H_
#error "Direct inclusion of this file is not allowed, include persistence.h"
// For the sake of sane code completion.
#include "persistence.h"
#endif

#include "config.h"
#include "db_schema.h"
#include "key_util.h"
#include "scalar_attribute_traits.h"
#include "serialize.h"
#include "session.h"
#include "transaction.h"
#include "type_handler.h"

#include <yt/yt/orm/client/objects/registry.h>

#include <yt/yt/orm/library/attributes/scalar_attribute.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/core/misc/mpl.h>

#include <util/string/split.h>
#include <util/system/type_name.h>

#include <google/protobuf/util/message_differencer.h>

namespace NYT::NOrm::NServer::NObjects {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TPathIgnoreCriteria
    : public google::protobuf::util::MessageDifferencer::IgnoreCriteria
{
public:
    TPathIgnoreCriteria(
        std::function<bool(const NYPath::TYPath&)> byPathFilter,
        const NYPath::TYPath& prefixPath)
        : PathFilter_(byPathFilter)
        , PrefixPath_(prefixPath)
    { }

    bool IsIgnored(
        const google::protobuf::Message& /*message1*/,
        const google::protobuf::Message& /*message2*/,
        const google::protobuf::FieldDescriptor* field,
        const std::vector<google::protobuf::util::MessageDifferencer::SpecificField>& parent_fields) override
    {
        if (!field) {
            return false;
        }
        auto fullPath = TStringBuilder();
        fullPath.AppendString(PrefixPath_);
        for (const auto& part : parent_fields) {
            if (part.field) {
                fullPath.AppendChar('/');
                fullPath.AppendString(part.field->name());
            }
        }
        fullPath.AppendChar('/');
        fullPath.AppendString(field->name());
        return !PathFilter_(fullPath.Flush());
    }

private:
    std::function<bool(const NYPath::TYPath&)> PathFilter_;
    const NYPath::TYPath& PrefixPath_;
};

////////////////////////////////////////////////////////////////////////////////

}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TParentAttribute<T>::TParentAttribute(TObject* owner)
    : Owner_(owner)
{ }

template <class T>
T* TParentAttribute<T>::Load(std::source_location location) const
{
    auto* session = Owner_->GetSession();
    return session->GetObject(T::Type, TObjectKey(Owner_->GetParentKey(location)))->template As<T>();
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
std::vector<T*> TChildrenAttribute<T>::Load(std::source_location location) const
{
    const auto& untypedResult = UntypedLoad(location);
    std::vector<T*> result;
    result.reserve(untypedResult.size());
    for (auto* untypedObject : untypedResult) {
        result.push_back(untypedObject->template As<T>());
    }
    return result;
}

template <class T>
TObjectTypeValue TChildrenAttribute<T>::GetChildrenType() const
{
    return T::Type;
}

////////////////////////////////////////////////////////////////////////////////

template <class TTypedObject, class TTypedValue>
const TScalarAttributeDescriptor<TTypedObject, TTypedValue>&
TScalarAttributeDescriptor<TTypedObject, TTypedValue>::SetInitializer(TInitializer initializer) const
{
    YT_VERIFY(!Initializer_);
    Initializer_ = std::move(initializer);
    return *this;
}

template <class TTypedObject, class TTypedValue>
const typename TScalarAttributeDescriptor<TTypedObject, TTypedValue>::TInitializer&
TScalarAttributeDescriptor<TTypedObject, TTypedValue>::GetInitializer() const
{
    return Initializer_;
}

template <class TTypedObject, class TTypedValue>
const TScalarAttributeDescriptor<TTypedObject, TTypedValue>&
TScalarAttributeDescriptor<TTypedObject, TTypedValue>::AddValidator(
    std::function<void(TTransaction*, const TTypedObject*, const TTypedValue&, const TTypedValue&)> validator) const
{
    OldNewValueValidators_.push_back(std::move(validator));
    return *this;
}

template <class TTypedObject, class TTypedValue>
const TScalarAttributeDescriptor<TTypedObject, TTypedValue>&
TScalarAttributeDescriptor<TTypedObject, TTypedValue>::AddValidator(
    std::function<void(TTransaction*, const TTypedObject*, const TTypedValue&)> validator) const
{
    NewValueValidators_.push_back(std::move(validator));
    return *this;
}

template <class TTypedObject, class TTypedValue>
TScalarAttribute<TTypedValue>*
TScalarAttributeDescriptor<TTypedObject, TTypedValue>::AttributeGetter(TTypedObject* typedObject) const
{
    return AttributeGetter_(typedObject);
}

template <class TTypedObject, class TTypedValue>
const TScalarAttribute<TTypedValue>*
TScalarAttributeDescriptor<TTypedObject, TTypedValue>::AttributeGetter(const TTypedObject* typedObject) const
{
    return AttributeGetter_(const_cast<TTypedObject*>(typedObject));
}

template <class TTypedObject, class TTypedValue>
TScalarAttributeBase* TScalarAttributeDescriptor<TTypedObject, TTypedValue>::AttributeBaseGetter(TObject* object) const
{
    return AttributeGetter_(object->As<TTypedObject>());
}

template <class TTypedObject, class TTypedValue>
bool TScalarAttributeDescriptor<TTypedObject, TTypedValue>::DoesNeedOldValue() const
{
    return !OldNewValueValidators_.empty();
}

template <class TTypedObject, class TTypedValue>
void TScalarAttributeDescriptor<TTypedObject, TTypedValue>::RunValidators(
    const TObject* object,
    const TTypedValue& currentValue,
    const TTypedValue* newValue) const
{
    if (OldNewValueValidators_.empty() && NewValueValidators_.empty()) {
        return;
    }
    auto* transaction = object->GetSession()->GetOwner();
    auto* typedObject = object->As<TTypedObject>();

    for (const auto& validator: OldNewValueValidators_) {
        validator(transaction, typedObject, currentValue, *newValue);
    }
    for (const auto& validator: NewValueValidators_) {
        validator(transaction, typedObject, *newValue);
    }
}

template <class TTypedObject, class TTypedValue>
void TScalarAttributeDescriptor<TTypedObject, TTypedValue>::RunFinalValueValidators(const TObject* object) const
{
    if (NewValueValidators_.empty()) {
        return;
    }
    auto* transaction = object->GetSession()->GetOwner();
    auto* typedObject = object->As<TTypedObject>();
    auto* attribute = AttributeGetter(typedObject);

    for (const auto& validator: NewValueValidators_) {
        validator(transaction, typedObject, attribute->Load());
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TScalarAttribute<T>::TScalarAttribute(TObject* owner, const TScalarAttributeDescriptorBase* descriptor)
    : TScalarAttributeBase(owner, descriptor)
{ }

template <class T>
const T& TScalarAttribute<T>::Load(std::source_location location) const
{
    if (NewValue_) {
        return *NewValue_;
    }
    return LoadOld(location);
}

template <class T>
TScalarAttribute<T>::operator const T&() const
{
    return Load();
}

template <class T>
const T& TScalarAttribute<T>::LoadOld(std::source_location location) const
{
    switch (Owner_->GetState()) {
        case EObjectState::Instantiated:
        case EObjectState::Finalizing:
        case EObjectState::Finalized:
        case EObjectState::Removing:
        case EObjectState::Removed:
            OnLoad(location);
            return *OldValue_;

        case EObjectState::Creating:
        case EObjectState::Created:
        case EObjectState::CreatedRemoving:
        case EObjectState::CreatedRemoved: {
            static const T Default = TScalarAttributeTraits<T>::GetDefaultValue();
            return Default;
        }

        case EObjectState::Unknown:
            YT_ABORT();
    }
}

// NB: Schedules store to the underlying storage.
template <class T>
T* TScalarAttribute<T>::MutableLoad(
    std::optional<bool> sharedWrite,
    std::source_location location)
{
    auto appliedSharedWrite = sharedWrite.value_or(false);

    for (const auto& observer : Descriptor_->BeforeMutableLoadObservers.Get()) {
        observer(Owner_, appliedSharedWrite);
    }

    OnStore(appliedSharedWrite, location);
    if (!NewValue_) {
        Load(location);
        YT_VERIFY(OldValue_);
        NewValue_ = OldValue_;
    }
    YT_VERIFY(NewValue_);
    return &*NewValue_;
}

template <class T>
bool TScalarAttribute<T>::IsChanged(
    const NYPath::TYPath& path,
    std::source_location location) const
{
    return NewValue_ && !NOrm::NAttributes::AreScalarAttributesEqualByPath(
        LoadOld(location),
        *NewValue_,
        path);
}

template <class T>
bool TScalarAttribute<T>::IsChangedWithFilter(
    const NYPath::TYPath& path,
    const NYPath::TYPath& schemaPath,
    std::function<bool(const NYPath::TYPath&)> byPathFilter,
    std::source_location location) const
{
    if (!NewValue_) {
        return false;
    }
    if (byPathFilter) {
        if (!byPathFilter(schemaPath) ||
            (!path.empty() && !byPathFilter(schemaPath + "/" + path)))
        {
            return false;
        }

        const auto& oldValue = LoadOld(location);
        if constexpr (std::convertible_to<T*, google::protobuf::Message*>) {
            // TODO(slysheva): Check changes inside proto message by attribute does not support.
            google::protobuf::util::MessageDifferencer differencer;
            differencer.AddIgnoreCriteria(new TPathIgnoreCriteria(byPathFilter, schemaPath));
            if (differencer.Compare(oldValue, *NewValue_)) {
                return false;
            }
        }
    }
    return IsChanged(path, location);
}

template <class T>
bool TScalarAttribute<T>::HasChangesForStoringToDB() const
{
    if (!LoadedFromDB_) {
        // If old value was not loaded from DB then object is to be created and we should store the attribute.
        return true;
    }

    YT_VERIFY(OldValue_);
    YT_VERIFY(NewValue_);

    return IsChanged(/*path*/ "");
}

template <class T>
void TScalarAttribute<T>::Store(T value, std::optional<bool> sharedWrite, std::source_location location)
{
    auto appliedSharedWrite = sharedWrite.value_or(false);

    for (const auto& observer : Descriptor_->BeforeStoreObservers.Get()) {
        observer(Owner_, appliedSharedWrite);
    }

    OnStore(appliedSharedWrite, location);
    NewValue_.emplace(std::move(value));
}

template <class T>
void TScalarAttribute<T>::StoreDefault(std::optional<bool> sharedWrite, std::source_location location)
{
    Store(TScalarAttributeTraits<T>::GetDefaultValue(), sharedWrite, location);
}

template <class T>
void TScalarAttribute<T>::SetDefaultValues()
{
    LoadedFromDB_ = false;
    OldValue_.emplace(TScalarAttributeTraits<T>::GetDefaultValue());
    NewValue_.emplace(TScalarAttributeTraits<T>::GetDefaultValue());
}

template <class T>
void TScalarAttribute<T>::LoadOldValue(const NTableClient::TUnversionedValue& value, ILoadContext* /*context*/)
{
    // Conversion of null to default typed value helps to add new columns without downtime,
    // e.g.: while adding new many-to-one reference.
    LoadedFromDB_ = true;
    OldValue_.emplace();
    OrmFromUnversionedValue(&*OldValue_,
        value,
        GetOwner()->GetSession()->GetConfigsSnapshot().TransactionManager->ScalarAttributeLoadsNullAsTyped);
}

template <class T>
void TScalarAttribute<T>::StoreNewValue(NTableClient::TUnversionedValue* dbValue, IStoreContext* context)
{
    ToUnversionedValue(dbValue, *NewValue_, context->GetRowBuffer());
}

template <class T>
TObjectKey::TKeyField TScalarAttribute<T>::LoadAsKeyField(const TString& suffix) const
{
    return ToKeyField(Load(), suffix);
}

template <class T>
TObjectKey::TKeyField TScalarAttribute<T>::LoadAsKeyFieldOld(const TString& suffix) const
{
    return ToKeyField(LoadOld(), suffix);
}

template <class T>
std::vector<TObjectKey::TKeyField> TScalarAttribute<T>::LoadAsKeyFields(const TString& suffix) const
{
    return ToKeyFields(Load(), suffix);
}

template <class T>
std::vector<TObjectKey::TKeyField> TScalarAttribute<T>::LoadAsKeyFieldsOld(
    const TString& suffix) const
{
    return ToKeyFields(LoadOld(), suffix);
}

template <class T>
TObjectKey::TKeyField TScalarAttribute<T>::ToKeyField(const T& value, const TString& suffix) const
{
    if (suffix.empty()) {
        if constexpr (CConvertibleToKeyField<T>) {
            return value;
        } else if constexpr (TEnumTraits<T>::IsStringSerializableEnum) {
            return FormatEnum(value);
        } else {
            THROW_ERROR_EXCEPTION("Type %Qv is not convertible to a key field",
                TypeName<T>());
        }
    } else {
        if constexpr (std::derived_from<T, NProtoBuf::Message>) {
            return ParseObjectKeyField(&value, suffix, /*ignorePresence*/ true);
        } else {
            THROW_ERROR_EXCEPTION("Type %Qv is not a protobuf message",
                TypeName<T>());
        }
    }
}

template <class T>
std::vector<TObjectKey::TKeyField> TScalarAttribute<T>::ToKeyFields(
    const T& value,
    const TString& suffix) const
{
    std::vector<TObjectKey::TKeyField> result;
    if (suffix.empty() || suffix == "/*") {
        if constexpr (CConvertibleToKeyFields<T>) {
            result = {value.begin(), value.end()};
        } else {
            THROW_ERROR_EXCEPTION("Type %Qv is not convertible to a list of key fields",
                TypeName<T>());
        }
    } else {
        ParseObjectKeyFields(&value, suffix, result);
    }
    return result;
}

template <class T>
void TScalarAttribute<T>::StoreKeyField(TObjectKey::TKeyField keyField, const TString& suffix)
{
    if (suffix.empty()) {
        if constexpr (IsKeyFieldType<T>()) {
            const T* value = std::get_if<T>(&keyField);
            if (value) {
                Store(std::move(*value));
                return;
            }
        } else if constexpr (TEnumTraits<T>::IsStringSerializableEnum) {
            const TString* serialized = std::get_if<TString>(&keyField);
            std::optional<T> optionalValue;
            if (serialized != nullptr) {
                optionalValue = TryParseEnum<T>(*serialized);
            }
            if (optionalValue.has_value()) {
                Store(optionalValue.value());
                return;
            }
        }
    } else {
        if constexpr (std::derived_from<T, NProtoBuf::Message>) {
            T value = Load();
            StoreObjectKeyField(&value, suffix, std::move(keyField));
            Store(value);
            return;
        }
    }

    THROW_ERROR_EXCEPTION("Key field %Qv of variant %Qv is not convertible to %Qv at suffix %Qv",
        keyField,
        keyField.index(),
        TypeName<T>(),
        suffix);
}

template <class T>
void TScalarAttribute<T>::StoreKeyFields(
    TObjectKey::TKeyFields keyFields,
    const TString& suffix)
{
    T value = Load();
    StoreObjectKeyFields(&value, suffix, std::move(keyFields));
    Store(std::move(value));
}

namespace NDetail {

template <class T>
TScalarAttributeBase::TConstProtosView LoadAsProtos(const TScalarAttribute<T>* /*attribute*/, bool /*old*/)
{
    return std::nullopt;
}

template <class T>
TScalarAttributeBase::TProtosView MutableLoadAsProtos(TScalarAttribute<T>* /*attribute*/)
{
    return std::nullopt;
}

template <class T>
    requires std::derived_from<T, google::protobuf::Message>
TScalarAttributeBase::TConstProtosView LoadAsProtos(const TScalarAttribute<T>* attribute, bool old)
{
    const auto& value = old ? attribute->LoadOld() : attribute->Load();
    TScalarAttributeBase::TConstProtosView result(std::in_place);
    result->push_back(&value);
    return result;
}

template <class T>
    requires std::derived_from<T, google::protobuf::Message>
TScalarAttributeBase::TProtosView MutableLoadAsProtos(TScalarAttribute<T>* attribute)
{
    auto* value = attribute->MutableLoad();
    TScalarAttributeBase::TProtosView result(std::in_place);
    result->push_back(value);
    return result;
}

template <class T>
    requires std::derived_from<T, google::protobuf::Message>
TScalarAttributeBase::TConstProtosView LoadAsProtos(
    const TScalarAttribute<std::vector<T>>* attribute, bool old)
{
    const auto& values = old ? attribute->LoadOld() : attribute->Load();
    TScalarAttributeBase::TConstProtosView result(std::in_place);
    result->reserve(values.size());
    for (const auto& value : values) {
        result->push_back(&value);
    }
    return result;
}

template <class T>
    requires std::derived_from<T, google::protobuf::Message>
TScalarAttributeBase::TProtosView MutableLoadAsProtos(TScalarAttribute<std::vector<T>>* attribute)
{
    auto* values = attribute->MutableLoad();
    TScalarAttributeBase::TProtosView result(std::in_place);
    result->reserve(values->size());
    for (auto& value : *values) {
        result->push_back(&value);
    }
    return result;
}

} // namespace NDetail

template <class T>
auto TScalarAttribute<T>::LoadAsProtos(bool old) const -> TConstProtosView
{
    return NDetail::LoadAsProtos(this, old);
}

template <class T>
auto TScalarAttribute<T>::MutableLoadAsProtos() -> TProtosView
{
    return NDetail::MutableLoadAsProtos(this);
}

template <class T>
const TScalarAttributeBase* TScalarAttribute<T>::GetAttributeForIndex() const
{
    return this;
}

////////////////////////////////////////////////////////////////////////////////

template <class TThis, class TThat>
TOneToOneAttribute<TThis, TThat>* TOneToOneAttributeDescriptor<TThis, TThat>::ForwardAttributeGetter(TThis* thisObject) const
{
    return ForwardAttributeGetter_(thisObject);
}

template <class TThis, class TThat>
const TOneToOneAttribute<TThis, TThat>* TOneToOneAttributeDescriptor<TThis, TThat>::ForwardAttributeGetter(
    const TThis* thisObject) const
{
    return ForwardAttributeGetter_(const_cast<TThis*>(thisObject));
}

template <class TThis, class TThat>
TOneToOneAttribute<TThat, TThis>* TOneToOneAttributeDescriptor<TThis, TThat>::InverseAttributeGetter(TThat* thatObject) const
{
    return InverseAttributeGetter_(thatObject);
}

template <class TThis, class TThat>
const TOneToOneAttribute<TThat, TThis>* TOneToOneAttributeDescriptor<TThis, TThat>::InverseAttributeGetter(
    const TThat* thatObject) const
{
    return InverseAttributeGetter_(const_cast<TThat*>(thatObject));
}

////////////////////////////////////////////////////////////////////////////////

template <class TThis, class TThat>
TOneToOneAttribute<TThis, TThat>::TOneToOneAttribute(
    TObject* owner,
    const TDescriptor* descriptor)
    : TAttributeBase(owner)
    , Descriptor_(descriptor)
    , UnderlyingDescriptor_(Descriptor_->Field,
        /*attributeGetter*/ nullptr)
    , Underlying_(owner, &UnderlyingDescriptor_)
{ }

template <class TThis, class TThat>
TObjectPlugin<TThat>* TOneToOneAttribute<TThis, TThat>::Load(std::source_location location) const
{
    return IdToThat(Underlying_.Load(location));
}

template <class TThis, class TThat>
TOneToOneAttribute<TThis, TThat>::operator TObjectPlugin<TThat>*() const
{
    return Load();
}

template <class TThis, class TThat>
TObjectPlugin<TThat>* TOneToOneAttribute<TThis, TThat>::LoadOld(std::source_location location) const
{
    return IdToThat(Underlying_.LoadOld(location));
}

template <class TThis, class TThat>
void TOneToOneAttribute<TThis, TThat>::Store(TThat* value, std::source_location location)
{
    auto* currentValue = Load(location);
    if (currentValue == value) {
        return;
    }

    if (currentValue) {
        Underlying_.Store(TObjectId(), /*sharedWrite*/ std::nullopt, location);
        Descriptor_->InverseAttributeGetter(currentValue)->Underlying_.Store(
            ExtractObjectIdFromObjectKey(Owner_->GetKey()),
            /*sharedWrite*/ std::nullopt,
            location);
    }

    if (value) {
        Owner_->ValidateNotFinalized();
        value->ValidateNotFinalized();

        Underlying_.Store(value->GetId(), /*sharedWrite*/ std::nullopt, location);
        Descriptor_->InverseAttributeGetter(value)->Underlying_.Store(
            ExtractObjectIdFromObjectKey(Owner_->GetKey()),
            /*sharedWrite*/ std::nullopt,
            location);
    }
}

template <class TThis, class TThat>
void TOneToOneAttribute<TThis, TThat>::Lock(NTableClient::ELockType lockType)
{
    Underlying_.Lock(lockType);
}

template <class TThis, class TThat>
void TOneToOneAttribute<TThis, TThat>::ScheduleLoadTimestamp() const
{
    Underlying_.ScheduleLoadTimestamp();
}

template <class TThis, class TThat>
TTimestamp TOneToOneAttribute<TThis, TThat>::LoadTimestamp(std::source_location location) const
{
    return Underlying_.LoadTimestamp(location);
}

template <class TThis, class TThat>
bool TOneToOneAttribute<TThis, TThat>::IsChanged(std::source_location location) const
{
    return Underlying_.IsChanged(/*path*/ "", location);
}

template <class TThis, class TThat>
bool TOneToOneAttribute<TThis, TThat>::IsStoreScheduled() const
{
    return Underlying_.IsStoreScheduled();
}

template <class TThis, class TThat>
const TScalarAttributeBase* TOneToOneAttribute<TThis, TThat>::GetAttributeForIndex() const
{
    return &Underlying_;
}

template <class TThis, class TThat>
void TOneToOneAttribute<TThis, TThat>::ScheduleLoad() const
{
    Underlying_.ScheduleLoad();
}

template <class TThis, class TThat>
TObjectPlugin<TThat>* TOneToOneAttribute<TThis, TThat>::IdToThat(const TObjectId& id) const
{
    if (id.empty()) {
        return nullptr;
    }
    auto* baseObject = Underlying_.GetOwner()->GetSession()
        ->GetObject(TThat::Type, TObjectKey(id))->template As<TThat>();
    return DowncastObject(baseObject);
}

template <class TThis, class TThat>
void TOneToOneAttribute<TThis, TThat>::PreloadObjectRemoval() const
{
    ScheduleLoad();
}

template <class TThis, class TThat>
void TOneToOneAttribute<TThis, TThat>::CheckObjectRemoval() const
{
    auto allowRemoval = Owner_->GetSession()->GetOwner()->RemovalWithNonEmptyReferencesAllowed()
        .value_or(!Descriptor_->ForbidNonEmptyRemoval);
    if (auto* that = Load()) {
        THROW_ERROR_EXCEPTION_UNLESS(allowRemoval,
            NClient::EErrorCode::RemovalForbidden,
            "Cannot remove %v since it has related %v",
            Owner_->GetDisplayName(),
            that->GetDisplayName());
    }
}

template <class TThis, class TThat>
void TOneToOneAttribute<TThis, TThat>::OnObjectRemovalStart()
{
    if (auto* that = Load()) {
        Descriptor_->InverseAttributeGetter(that)->Underlying_.Store(TObjectId());
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TMany, class TOne>
TManyToOneAttribute<TMany, TOne>* TManyToOneAttributeDescriptor<TMany, TOne>::ForwardAttributeGetter(TMany* many) const
{
    return ForwardAttributeGetter_(many);
}

template <class TMany, class TOne>
const TManyToOneAttribute<TMany, TOne>* TManyToOneAttributeDescriptor<TMany, TOne>::ForwardAttributeGetter(
    const TMany* many) const
{
    return ForwardAttributeGetter_(const_cast<TMany*>(many));
}

template <class TMany, class TOne>
TOneToManyAttribute<TOne, TMany>* TManyToOneAttributeDescriptor<TMany, TOne>::InverseAttributeGetter(TOne* one) const
{
    return InverseAttributeGetter_(one);
}

template <class TMany, class TOne>
const TOneToManyAttribute<TOne, TMany>* TManyToOneAttributeDescriptor<TMany, TOne>::InverseAttributeGetter(
    const TOne* one) const
{
    return InverseAttributeGetter_(const_cast<TOne*>(one));
}

////////////////////////////////////////////////////////////////////////////////

template <class TMany, class TOne>
TManyToOneAttribute<TMany, TOne>::TManyToOneAttribute(
    TObject* owner,
    const TDescriptor* descriptor)
    : TAttributeBase(owner)
    , Descriptor_(descriptor)
    , UnderlyingDescriptor_(Descriptor_->Field,
        /*attributeGetter*/ nullptr)
    , Underlying_(owner, &UnderlyingDescriptor_)
{ }

template <class TMany, class TOne>
TObjectKey TManyToOneAttribute<TMany, TOne>::LoadKey(std::source_location location) const
{
    return TObjectKey(Underlying_.Load(location));
}

template <class TMany, class TOne>
TObjectKey TManyToOneAttribute<TMany, TOne>::LoadKeyOld(std::source_location location) const
{
    return TObjectKey(Underlying_.LoadOld(location));
}

template <class TMany, class TOne>
TObjectPlugin<TOne>* TManyToOneAttribute<TMany, TOne>::Load(std::source_location location) const
{
    return KeyFirstFieldToOne(Underlying_.Load(location));
}

template <class TMany, class TOne>
TManyToOneAttribute<TMany, TOne>::operator TOne*() const
{
    return Load();
}

template <class TMany, class TOne>
TObjectPlugin<TOne>* TManyToOneAttribute<TMany, TOne>::LoadOld(std::source_location location) const
{
    return KeyFirstFieldToOne(Underlying_.LoadOld(location));
}

template <class TMany, class TOne>
void TManyToOneAttribute<TMany, TOne>::Lock(NTableClient::ELockType lockType)
{
    Underlying_.Lock(lockType);
}

template <class TMany, class TOne>
void TManyToOneAttribute<TMany, TOne>::ScheduleLoadTimestamp() const
{
    Underlying_.ScheduleLoadTimestamp();
}

template <class TMany, class TOne>
TTimestamp TManyToOneAttribute<TMany, TOne>::LoadTimestamp(std::source_location location) const
{
    return Underlying_.LoadTimestamp(location);
}

template <class TMany, class TOne>
bool TManyToOneAttribute<TMany, TOne>::IsChanged(std::source_location location) const
{
    return Underlying_.IsChanged(/*path*/ "", location);
}

template <class TMany, class TOne>
bool TManyToOneAttribute<TMany, TOne>::IsStoreScheduled() const
{
    return Underlying_.IsStoreScheduled();
}

template <class TMany, class TOne>
const TScalarAttributeBase* TManyToOneAttribute<TMany, TOne>::GetAttributeForIndex() const
{
    return &Underlying_;
}

template <class TMany, class TOne>
void TManyToOneAttribute<TMany, TOne>::ScheduleLoad() const
{
    Underlying_.ScheduleLoad();
}

template <class TMany, class TOne>
TObjectPlugin<TOne>* TManyToOneAttribute<TMany, TOne>::KeyFirstFieldToOne(const TOneKeyFirstField& keyFirstField) const
{
    if (keyFirstField == TOneKeyFirstField{}) {
        return nullptr;
    }
    auto* baseObject = Underlying_.GetOwner()->GetSession()
        ->GetObject(TOne::Type, TObjectKey(keyFirstField))
        ->template As<TOne>();
    return DowncastObject(baseObject);
}

template <class TMany, class TOne>
void TManyToOneAttribute<TMany, TOne>::Store(TOne* one, std::source_location location)
{
    TOneKeyFirstField field{};
    if (one) {
        Owner_->ValidateNotFinalized();
        one->ValidateNotFinalized();

        auto key = one->GetKey();
        if (key) {
            YT_VERIFY(1 == key.size());
            YT_VERIFY(std::holds_alternative<TOneKeyFirstField>(key[0]));
            field = std::get<TOneKeyFirstField>(key[0]);
        }
    }
    Underlying_.Store(std::move(field), /*sharedWrite*/ std::nullopt, location);
}

template <class TMany, class TOne>
void TManyToOneAttribute<TMany, TOne>::PreloadObjectRemoval() const
{
    ScheduleLoad();
}

template <class TMany, class TOne>
void TManyToOneAttribute<TMany, TOne>::CheckObjectRemoval() const
{
    auto allowRemoval = Owner_->GetSession()->GetOwner()->RemovalWithNonEmptyReferencesAllowed()
        .value_or(!Descriptor_->ForbidNonEmptyRemoval);
    if (auto* one = Load()) {
        THROW_ERROR_EXCEPTION_UNLESS(allowRemoval,
            NClient::EErrorCode::RemovalForbidden,
            "Cannot remove %v since it has related %v",
            Owner_->GetDisplayName(),
            one->GetDisplayName());
    }
}

template <class TMany, class TOne>
void TManyToOneAttribute<TMany, TOne>::OnObjectRemovalStart()
{
    if (auto* one = Load()) {
        Descriptor_->InverseAttributeGetter(one)->Remove(DowncastObject(Owner_->As<TMany>()));
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TOne, class TMany>
TOneToManyAttribute<TOne, TMany>* TOneToManyAttributeDescriptor<TOne, TMany>::ForwardAttributeGetter(TOne* one) const
{
    return ForwardAttributeGetter_(one);
}

template <class TOne, class TMany>
const TOneToManyAttribute<TOne, TMany>* TOneToManyAttributeDescriptor<TOne, TMany>::ForwardAttributeGetter(
    const TOne* one)const
{
    return ForwardAttributeGetter_(const_cast<TOne*>(one));
}

template <class TOne, class TMany>
TManyToOneAttribute<TMany, TOne>* TOneToManyAttributeDescriptor<TOne, TMany>::InverseAttributeGetter(TMany* many) const
{
    return InverseAttributeGetter_(many);
}

template <class TOne, class TMany>
const TManyToOneAttribute<TMany, TOne>* TOneToManyAttributeDescriptor<TOne, TMany>::InverseAttributeGetter(
    const TMany* many) const
{
    return InverseAttributeGetter_(const_cast<TMany*>(many));
}

////////////////////////////////////////////////////////////////////////////////

template <class TTypedObject>
std::vector<TObjectPlugin<TTypedObject>*> TAnyToManyAttributeBase::TypedLoad(std::source_location location, bool old) const
{
    const auto& untypedResult = UntypedLoad(location, old);
    std::vector<TObjectPlugin<TTypedObject>*> result;
    result.reserve(untypedResult.size());
    for (auto* untypedObject : untypedResult) {
        result.push_back(DowncastObject(untypedObject->template As<TTypedObject>()));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

template <class TOne, class TMany>
TOneToManyAttribute<TOne, TMany>::TOneToManyAttribute(
    TOne* owner,
    const TDescriptor* descriptor)
    : TAnyToManyAttributeBase(owner, descriptor)
    , TypedOwner_(owner)
    , TypedDescriptor_(descriptor)
{ }

template <class TOne, class TMany>
std::vector<TObjectPlugin<TMany>*> TOneToManyAttribute<TOne, TMany>::Load(std::source_location location) const
{
    return TypedLoad<TMany>(location, /*old*/ false);
}

template <class TOne, class TMany>
std::vector<TObjectPlugin<TMany>*> TOneToManyAttribute<TOne, TMany>::LoadOld(std::source_location location) const
{
    return TypedLoad<TMany>(location, /*old*/ true);
}

template <class TOne, class TMany>
void TOneToManyAttribute<TOne, TMany>::Add(TObjectPlugin<TMany>* typedMany, std::source_location location)
{
    YT_VERIFY(typedMany);
    auto* many = UpcastObject(typedMany);
    TypedOwner_->ValidateNotFinalized();
    many->ValidateNotFinalized();

    auto* inverseAttribute = TypedDescriptor_->InverseAttributeGetter(many);
    auto* currentOne = inverseAttribute->Load(location);
    if (currentOne == TypedOwner_) {
        return;
    }
    if (currentOne) {
        auto* forwardAttribute = TypedDescriptor_->ForwardAttributeGetter(currentOne);
        forwardAttribute->Remove(typedMany, location);
    }
    inverseAttribute->Store(TypedOwner_, location);
    DoAdd(many);
}

template <class TOne, class TMany>
void TOneToManyAttribute<TOne, TMany>::Remove(TObjectPlugin<TMany>* typedMany, std::source_location location)
{
    auto* many = UpcastObject(typedMany);
    YT_VERIFY(many);
    auto* inverseAttribute = TypedDescriptor_->InverseAttributeGetter(many);
    YT_VERIFY(inverseAttribute->Load(location) == TypedOwner_);
    inverseAttribute->Store(nullptr, location);
    DoRemove(many);
}

template <class TOne, class TMany>
void TOneToManyAttribute<TOne, TMany>::Clear(std::source_location location)
{
    // NB: Make a copy.
    for (auto* many : Load(location)) {
        Remove(many, location);
    }
}

template <class TOne, class TMany>
TObjectTypeValue TOneToManyAttribute<TOne, TMany>::GetForeignObjectType() const
{
    return TMany::Type;
}

template <class TOne, class TMany>
void TOneToManyAttribute<TOne, TMany>::ScheduleLoadForeignObjectAttribute(TObject* many) const
{
    if (!many) {
        return;
    }
    auto* typedMany = many->template As<TMany>();
    TypedDescriptor_->InverseAttributeGetter(typedMany)->ScheduleLoad();
}

template <class TOne, class TMany>
void TOneToManyAttribute<TOne, TMany>::PreloadObjectRemoval() const
{
    ScheduleLoad();
}

template <class TOne, class TMany>
void TOneToManyAttribute<TOne, TMany>::CheckObjectRemoval() const
{
    auto hasMany = !UntypedLoad().empty();
    auto allowRemoval = TypedOwner_->GetSession()->GetOwner()->RemovalWithNonEmptyReferencesAllowed()
        .value_or(!TypedDescriptor_->ForbidNonEmptyRemoval);
    THROW_ERROR_EXCEPTION_IF(hasMany && !allowRemoval,
        NClient::EErrorCode::RemovalForbidden,
        "Cannot remove %v since it has %v related %v(s)",
        Owner_->GetDisplayName(),
        UntypedLoad().size(),
        NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(TMany::Type));
}

template <class TOne, class TMany>
void TOneToManyAttribute<TOne, TMany>::OnObjectRemovalStart()
{
    Clear();
}

////////////////////////////////////////////////////////////////////////////////

template <class TOwner, class TForeign>
TManyToManyInlineAttribute<TOwner, TForeign>*
TManyToManyInlineAttributeDescriptor<TOwner, TForeign>::ForwardAttributeGetter(TOwner* owner) const
{
    return ForwardAttributeGetter_(owner);
}

template <class TOwner, class TForeign>
const TManyToManyInlineAttribute<TOwner, TForeign>*
TManyToManyInlineAttributeDescriptor<TOwner, TForeign>::ForwardAttributeGetter(const TOwner* owner) const
{
    return ForwardAttributeGetter_(const_cast<TOwner*>(owner));
}

template <class TOwner, class TForeign>
TManyToManyTabularAttribute<TForeign, TOwner>*
TManyToManyInlineAttributeDescriptor<TOwner, TForeign>::InverseAttributeGetter(TForeign* foreign) const
{
    return InverseAttributeGetter_(foreign);
}

template <class TOwner, class TForeign>
const TManyToManyTabularAttribute<TForeign, TOwner>*
TManyToManyInlineAttributeDescriptor<TOwner, TForeign>::InverseAttributeGetter(const TForeign* foreign) const
{
    return InverseAttributeGetter_(const_cast<TForeign*>(foreign));
}

////////////////////////////////////////////////////////////////////////////////

template <class TOwner, class TForeign>
TManyToManyInlineAttribute<TOwner, TForeign>::TManyToManyInlineAttribute(
    TObject* owner,
    const TDescriptor* descriptor)
    : TAttributeBase(owner)
    , Descriptor_(descriptor)
    , UnderlyingDescriptor_(Descriptor_->Field, /*attributeGetter*/ nullptr)
    , Underlying_(owner, &UnderlyingDescriptor_)
{ }

template <class TOwner, class TForeign>
void TManyToManyInlineAttribute<TOwner, TForeign>::Lock(NTableClient::ELockType lockType)
{
    Underlying_.Lock(lockType);
}

template <class TOwner, class TForeign>
std::vector<TObjectKey> TManyToManyInlineAttribute<TOwner, TForeign>::LoadKeys(std::source_location location) const
{
    const auto& keyFields = Underlying_.Load(location);
    return std::vector<TObjectKey>{
        keyFields.begin(),
        keyFields.end()
    };
}

template <class TOwner, class TForeign>
std::vector<TObjectKey> TManyToManyInlineAttribute<TOwner, TForeign>::LoadKeysOld(std::source_location location) const
{
    const auto& keyFields = Underlying_.LoadOld(location);
    return std::vector<TObjectKey>{
        keyFields.begin(),
        keyFields.end()
    };
}

template <class TOwner, class TForeign>
std::vector<TObjectPlugin<TForeign>*> TManyToManyInlineAttribute<TOwner, TForeign>::Load(std::source_location location) const
{
    return KeyFirstFieldToForeigns(Underlying_.Load(location));
}

template <class TOwner, class TForeign>
std::vector<TObjectPlugin<TForeign>*> TManyToManyInlineAttribute<TOwner, TForeign>::LoadOld(std::source_location location) const
{
    return KeyFirstFieldToForeigns(Underlying_.LoadOld(location));
}

template <class TOwner, class TForeign>
void TManyToManyInlineAttribute<TOwner, TForeign>::ScheduleLoadTimestamp() const
{
    Underlying_.ScheduleLoadTimestamp();
}

template <class TOwner, class TForeign>
TTimestamp TManyToManyInlineAttribute<TOwner, TForeign>::LoadTimestamp(std::source_location location) const
{
    return Underlying_.LoadTimestamp(location);
}

template <class TOwner, class TForeign>
bool TManyToManyInlineAttribute<TOwner, TForeign>::IsChanged(std::source_location location) const
{
    return Underlying_.IsChanged(/*path*/ "", location);
}

template <class TOwner, class TForeign>
bool TManyToManyInlineAttribute<TOwner, TForeign>::IsStoreScheduled() const
{
    return Underlying_.IsStoreScheduled();
}

template <class TOwner, class TForeign>
void TManyToManyInlineAttribute<TOwner, TForeign>::ScheduleLoad() const
{
    Underlying_.ScheduleLoad();
}

template <class TOwner, class TForeign>
std::vector<TObjectPlugin<TForeign>*> TManyToManyInlineAttribute<TOwner, TForeign>::KeyFirstFieldToForeigns(
    const std::vector<TForeignKeyFirstField>& keyFirstFields) const
{
    std::vector<TObjectPlugin<TForeign>*> result;
    result.reserve(keyFirstFields.size());
    for (const auto& keyFirstField : keyFirstFields) {
        auto* baseObject = Underlying_.GetOwner()->GetSession()
            ->GetObject(TForeign::Type, TObjectKey(keyFirstField))
            ->template As<TForeign>();
        result.push_back(DowncastObject(baseObject));
    }
    return result;
}

template <class TOwner, class TForeign>
auto TManyToManyInlineAttribute<TOwner, TForeign>::ForeignToKeyFirstField(
    const TForeign* foreign) const -> TForeignKeyFirstField
{
    auto key = foreign->GetKey();
    THROW_ERROR_EXCEPTION_UNLESS(
        key,
        "Foreign key %v of %v is empty",
        NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(TForeign::Type),
        NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(TForeign::Type));
    YT_VERIFY(1 == key.size());
    YT_VERIFY(std::holds_alternative<TForeignKeyFirstField>(key[0]));
    return std::get<TForeignKeyFirstField>(key[0]);
}

template <class TOwner, class TForeign>
void TManyToManyInlineAttribute<TOwner, TForeign>::Store(std::vector<TForeign*> foreigns, std::source_location location)
{
    if (!foreigns.empty()) {
        Owner_->ValidateNotFinalized();
        for (const auto& foreign: foreigns) {
            foreign->ValidateNotFinalized();
        }
    }

    std::vector<TForeignKeyFirstField> fields;
    fields.reserve(foreigns.size());
    THashSet<TForeign*> newForeignsSet;
    newForeignsSet.reserve(foreigns.size());
    for (const auto& foreign : foreigns) {
        if (newForeignsSet.contains(foreign)) {
            continue;
        }
        foreign->ValidateExists(location);
        fields.push_back(ForeignToKeyFirstField(foreign));
        newForeignsSet.insert(foreign);
    }

    auto oldForeigns = Load(location);
    THashSet<TForeign*> oldForeignsSet(oldForeigns.begin(), oldForeigns.end());
    for (auto* foreign : oldForeigns) {
        if (!newForeignsSet.contains(foreign)) {
            auto* inverseAttribute = Descriptor_->InverseAttributeGetter(foreign);
            inverseAttribute->DoRemove(Owner_->As<TOwner>());
        }
    }
    for (auto* foreign : foreigns) {
        if (!oldForeignsSet.contains(foreign)) {
            auto* inverseAttribute = Descriptor_->InverseAttributeGetter(foreign);
            inverseAttribute->DoAdd(Owner_->As<TOwner>());
        }
    }

    Underlying_.Store(std::move(fields), /*sharedWrite*/ std::nullopt, location);
}

template <class TOwner, class TForeign>
void TManyToManyInlineAttribute<TOwner, TForeign>::Add(TForeign* foreign, std::source_location location)
{
    YT_VERIFY(foreign);
    Owner_->ValidateNotFinalized();
    foreign->ValidateNotFinalized();

    auto* foreignKeyFirstFields = Underlying_.MutableLoad(/*sharedWrite*/ std::nullopt, location);
    auto foreignKeyField = ForeignToKeyFirstField(foreign);
    auto it = std::find(foreignKeyFirstFields->begin(), foreignKeyFirstFields->end(), foreignKeyField);
    if (it != foreignKeyFirstFields->end()) {
        return;
    }

    foreignKeyFirstFields->push_back(foreignKeyField);
    Descriptor_->InverseAttributeGetter(foreign)->DoAdd(Owner_->As<TOwner>());
}

template <class TOwner, class TForeign>
void TManyToManyInlineAttribute<TOwner, TForeign>::Remove(TForeign* foreign, std::source_location location)
{
    YT_VERIFY(foreign);

    auto* foreignKeyFirstFields = Underlying_.MutableLoad(/*sharedWrite*/ std::nullopt, location);
    auto it = std::find(
        foreignKeyFirstFields->begin(),
        foreignKeyFirstFields->end(),
        ForeignToKeyFirstField(foreign));
    if (it == foreignKeyFirstFields->end()) {
        return;
    }

    foreignKeyFirstFields->erase(it);
    Descriptor_->InverseAttributeGetter(foreign)->DoRemove(Owner_->As<TOwner>());
}

template <class TOwner, class TForeign>
const TScalarAttributeBase* TManyToManyInlineAttribute<TOwner, TForeign>::GetAttributeForIndex() const
{
    return &Underlying_;
}

template <class TOwner, class TForeign>
void TManyToManyInlineAttribute<TOwner, TForeign>::PreloadObjectRemoval() const
{
    ScheduleLoad();
}

template <class TOwner, class TForeign>
void TManyToManyInlineAttribute<TOwner, TForeign>::CheckObjectRemoval() const
{
    auto allowRemoval = GetOwner()->GetSession()->GetOwner()->RemovalWithNonEmptyReferencesAllowed()
        .value_or(!Descriptor_->ForbidNonEmptyRemoval);
    const auto& keys = Underlying_.Load();
    THROW_ERROR_EXCEPTION_UNLESS(keys.empty() || allowRemoval,
        NClient::EErrorCode::RemovalForbidden,
        "Cannot remove %v since it has %v related %v(s)",
        Owner_->GetDisplayName(),
        keys.size(),
        NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(TForeign::Type));
}

template <class TOwner, class TForeign>
void TManyToManyInlineAttribute<TOwner, TForeign>::OnObjectRemovalStart()
{
    Store({});
}

////////////////////////////////////////////////////////////////////////////////

template <class TOwner, class TForeign>
TManyToManyTabularAttribute<TOwner, TForeign>*
TManyToManyTabularAttributeDescriptor<TOwner, TForeign>::ForwardAttributeGetter(TOwner* owner) const
{
    return ForwardAttributeGetter_(owner);
}

template <class TOwner, class TForeign>
const TManyToManyTabularAttribute<TOwner, TForeign>*
TManyToManyTabularAttributeDescriptor<TOwner, TForeign>::ForwardAttributeGetter(const TOwner* owner) const
{
    return ForwardAttributeGetter_(const_cast<TOwner*>(owner));
}

template <class TOwner, class TForeign>
TManyToManyInlineAttribute<TForeign, TOwner>*
TManyToManyTabularAttributeDescriptor<TOwner, TForeign>::InverseAttributeGetter(TForeign* foreign) const
{
    return InverseAttributeGetter_(foreign);
}

template <class TOwner, class TForeign>
const TManyToManyInlineAttribute<TForeign, TOwner>*
TManyToManyTabularAttributeDescriptor<TOwner, TForeign>::InverseAttributeGetter(const TForeign* foreign) const
{
    return InverseAttributeGetter_(const_cast<TForeign*>(foreign));
}

////////////////////////////////////////////////////////////////////////////////

template <class TOwner, class TForeign>
TManyToManyTabularAttribute<TOwner, TForeign>::TManyToManyTabularAttribute(
    TOwner* owner,
    const TDescriptor* descriptor)
    : TAnyToManyAttributeBase(owner, descriptor)
    , TypedOwner_(owner)
    , TypedDescriptor_(descriptor)
{ }

template <class TOwner, class TForeign>
std::vector<TObjectPlugin<TForeign>*> TManyToManyTabularAttribute<TOwner, TForeign>::Load(
    std::source_location location) const
{
    return TypedLoad<TForeign>(location, /*old*/ false);
}

template <class TOwner, class TForeign>
std::vector<TObjectPlugin<TForeign>*> TManyToManyTabularAttribute<TOwner, TForeign>::LoadOld(
    std::source_location location) const
{
    return TypedLoad<TForeign>(location, /*old*/ true);
}

template <class TOwner, class TForeign>
void TManyToManyTabularAttribute<TOwner, TForeign>::Clear(std::source_location location)
{
    // NB: Make a copy.
    for (auto* foreign : Load(location)) {
        auto* inverseAttribute = TypedDescriptor_->InverseAttributeGetter(foreign);
        inverseAttribute->Remove(TypedOwner_, location);
    }
}

template <class TOwner, class TForeign>
void TManyToManyTabularAttribute<TOwner, TForeign>::DoAdd(TForeign* foreign)
{
    TAnyToManyAttributeBase::DoAdd(foreign);
}

template <class TOwner, class TForeign>
void TManyToManyTabularAttribute<TOwner, TForeign>::DoRemove(TForeign* foreign)
{
    TAnyToManyAttributeBase::DoRemove(foreign);
}

template <class TOwner, class TForeign>
TObjectTypeValue TManyToManyTabularAttribute<TOwner, TForeign>::GetForeignObjectType() const
{
    return TForeign::Type;
}

template <class TOwner, class TForeign>
void TManyToManyTabularAttribute<TOwner, TForeign>::ScheduleLoadForeignObjectAttribute(TObject* foreign) const
{
    if (!foreign) {
        return;
    }
    auto* typedForeign = foreign->template As<TForeign>();
    TypedDescriptor_->InverseAttributeGetter(typedForeign)->ScheduleLoad();
}

template <class TOwner, class TForeign>
void TManyToManyTabularAttribute<TOwner, TForeign>::PreloadObjectRemoval() const
{
    ScheduleLoad();
}

template <class TOwner, class TForeign>
void TManyToManyTabularAttribute<TOwner, TForeign>::CheckObjectRemoval() const
{
    auto hasForeigns = !UntypedLoad().empty();
    auto allowRemoval = TypedOwner_->GetSession()->GetOwner()->RemovalWithNonEmptyReferencesAllowed()
        .value_or(!TypedDescriptor_->ForbidNonEmptyRemoval);
    THROW_ERROR_EXCEPTION_UNLESS(!hasForeigns || allowRemoval,
        NClient::EErrorCode::RemovalForbidden,
        "Cannot remove %v since it has %v related %v(s)",
        Owner_->GetDisplayName(),
        UntypedLoad().size(),
        NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(TForeign::Type));
}

template <class TOwner, class TForeign>
void TManyToManyTabularAttribute<TOwner, TForeign>::OnObjectRemovalStart()
{
    Clear();
}

////////////////////////////////////////////////////////////////////////////////

template <class TOwner, class TForeign>
TOneTransitiveAttribute<TOwner, TForeign>* TOneTransitiveAttributeDescriptor<TOwner, TForeign>::ForwardAttributeGetter(
    TOwner* owner) const
{
    return ForwardAttributeGetter_(owner);
}

template <class TOwner, class TForeign>
TOneTransitiveAttribute<TOwner, TForeign>* TOneTransitiveAttributeDescriptor<TOwner, TForeign>::ForwardAttributeGetter(
    const TOwner* owner) const
{
    return ForwardAttributeGetter_(const_cast<TOwner*>(owner));
}

template <class TOwner, class TForeign>
void TOneTransitiveAttributeDescriptor<TOwner, TForeign>::InitialForeignLoader(TOwner* owner) const
{
    InitialForeignLoader_(owner);
}

////////////////////////////////////////////////////////////////////////////////

template <class TOwner, class TForeign>
TOneTransitiveAttribute<TOwner, TForeign>::TOneTransitiveAttribute(
    TOwner* owner,
    const TDescriptor* descriptor)
    : TAttributeBase(owner)
    , TypedOwner_(owner)
    , Descriptor_(descriptor)
    , UnderlyingDescriptor_(descriptor->Field, /*attributeGetter*/ nullptr)
    , Underlying_(owner, &UnderlyingDescriptor_)
{ }

template <class TOwner, class TForeign>
TForeign* TOneTransitiveAttribute<TOwner, TForeign>::IdToForeign(const TForeignKeyFirstField& id) const
{
    if (id == TForeignKeyFirstField{}) {
        return nullptr;
    }
    return Underlying_.GetOwner()->GetSession()
        ->GetObject(TForeign::Type, TObjectKey{id})->template As<TForeign>();
}

template <class TOwner, class TForeign>
void TOneTransitiveAttribute<TOwner, TForeign>::ScheduleLoad() const
{
    Underlying_.ScheduleLoad();
}

template <class TOwner, class TForeign>
TObjectPlugin<TForeign>* TOneTransitiveAttribute<TOwner, TForeign>::Load(std::source_location location) const
{
    return IdToForeign(Underlying_.Load(location));
}

template <class TOwner, class TForeign>
TObjectKey TOneTransitiveAttribute<TOwner, TForeign>::LoadKey(std::source_location location) const
{
    return TObjectKey(Underlying_.Load(location));
}

template <class TOwner, class TForeign>
void TOneTransitiveAttribute<TOwner, TForeign>::ScheduleLoadTimestamp() const
{
    Underlying_.ScheduleLoadTimestamp();
}

template <class TOwner, class TForeign>
TTimestamp TOneTransitiveAttribute<TOwner, TForeign>::LoadTimestamp(std::source_location location) const
{
    return Underlying_.LoadTimestamp(location);
}

template <class TOwner, class TForeign>
bool TOneTransitiveAttribute<TOwner, TForeign>::IsStoreScheduled() const
{
    return Underlying_.IsStoreScheduled();
}

template <class TOwner, class TForeign>
void TOneTransitiveAttribute<TOwner, TForeign>::StoreInitial(TForeign* foreign)
{
    auto state = Owner_->GetState();
    if (EObjectState::CreatedRemoved == state) {
        return;
    }

    YT_VERIFY(EObjectState::Creating == state || EObjectState::Created == state);
    Store(foreign);
}

template <class TOwner, class TForeign>
void TOneTransitiveAttribute<TOwner, TForeign>::Store(TForeign* foreign)
{
    TForeignKeyFirstField field{};
    if (foreign != nullptr) {
        auto key = foreign->GetKey();
        if (key) {
            YT_VERIFY(1 == key.size());
            YT_VERIFY(std::holds_alternative<TForeignKeyFirstField>(key[0]));
            field = std::get<TForeignKeyFirstField>(std::move(key[0]));
        }
    }
    Underlying_.Store(std::move(field));
}

template <class TOwner, class TForeign>
void TOneTransitiveAttribute<TOwner, TForeign>::OnObjectInitialization()
{
    Descriptor_->InitialForeignLoader(TypedOwner_);
}

template <class TOwner, class TForeign>
const TScalarAttributeBase* TOneTransitiveAttribute<TOwner, TForeign>::GetAttributeForIndex() const
{
    return &Underlying_;
}

////////////////////////////////////////////////////////////////////////////////

template <class TTypedObject, class TTypedValue>
const TScalarAggregatedAttributeDescriptor<TTypedObject, TTypedValue>&
TScalarAggregatedAttributeDescriptor<TTypedObject, TTypedValue>::AddValidator(
    std::function<void(TTransaction*, const TTypedObject*, const TTypedValue&)> validator) const
{
    NewValueValidators_.push_back(std::move(validator));
    return *this;
}

template <class TTypedObject, class TTypedValue>
TScalarAggregatedAttribute<TTypedValue>*
TScalarAggregatedAttributeDescriptor<TTypedObject, TTypedValue>::AttributeGetter(TTypedObject* typedObject) const
{
    return AttributeGetter_(typedObject);
}

template <class TTypedObject, class TTypedValue>
const TScalarAggregatedAttribute<TTypedValue>*
TScalarAggregatedAttributeDescriptor<TTypedObject, TTypedValue>::AttributeGetter(const TTypedObject* typedObject) const
{
    return AttributeGetter_(const_cast<TTypedObject*>(typedObject));
}

template <class TTypedObject, class TTypedValue>
TScalarAttributeBase*
TScalarAggregatedAttributeDescriptor<TTypedObject, TTypedValue>::AttributeBaseGetter(TObject* object) const
{
    return AttributeGetter_(object->As<TTypedObject>());
}

template <class TTypedObject, class TTypedValue>
void TScalarAggregatedAttributeDescriptor<TTypedObject, TTypedValue>::RunFinalValueValidators(
    const TObject* object) const
{
    if (NewValueValidators_.empty()) {
        return;
    }
    auto* transaction = object->GetSession()->GetOwner();
    auto* typedObject = object->As<TTypedObject>();
    auto* attribute = AttributeGetter(typedObject);

    for (const auto& validator: NewValueValidators_) {
        validator(transaction, typedObject, attribute->Load());
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename TTypedValue>
TScalarAggregatedAttribute<TTypedValue>::TScalarAggregatedAttribute(
    TObject* owner,
    const TScalarAttributeDescriptorBase* descriptor)
    : TScalarAttributeBase(owner, descriptor)
    , ColumnId_(Owner_->GetTypeHandler()->GetTableSchema()->GetColumnIndexOrThrow(Descriptor_->Field->Name))
    , RowBuffer_(New<NTableClient::TRowBuffer>(TRowBufferTag()))
    , MergerHolder_(BIND([this] {
        auto evaluator = Owner_->GetSession()->GetObjectTableEvaluator(Owner_->GetType());
        return NTableClient::TUnversionedRowMerger(
            RowBuffer_,
            Owner_->GetTypeHandler()->GetTableSchema()->GetColumnCount(),
            /*keyColumnCount*/ 0,
            std::move(evaluator));
    }))
{ }

template <class TTypedValue>
const TTypedValue& TScalarAggregatedAttribute<TTypedValue>::LoadOld(std::source_location location) const
{
    switch (Owner_->GetState()) {
        case EObjectState::Instantiated:
        case EObjectState::Finalizing:
        case EObjectState::Finalized:
        case EObjectState::Removing:
        case EObjectState::Removed:
            OnLoad(location);
            YT_VERIFY(Old_);
            return Old_->TypedValue;

        case EObjectState::Creating:
        case EObjectState::Created:
        case EObjectState::CreatedRemoving:
        case EObjectState::CreatedRemoved: {
            if (!Old_) {
                static const auto Default = TScalarAttributeTraits<TTypedValue>::GetDefaultValue();
                Old_ = MakeAggregate(Default);
            }
            return Old_->TypedValue;
        }

        case EObjectState::Unknown:
            YT_ABORT();
    }
}

template <class TTypedValue>
const TTypedValue& TScalarAggregatedAttribute<TTypedValue>::Load(std::source_location location) const
{
    if (Override_) {
        if (!Pending_.empty()) {
            Override_ = MakeAggregate(MergeRows(Override_->UnversionedRow, MergePending()));
        }
        return Override_->TypedValue;
    } else {
        if (!Current_) {
            LoadOld(location);
            Current_ = Old_;
        }

        if (DeltaForCurrent_) {
            Current_ = MakeAggregate(MergeRows(Current_->UnversionedRow, *DeltaForCurrent_));
            DeltaForCurrent_.reset();
        }

        if (!Pending_.empty()) {
            auto mergedAggregates = MergePending();
            Current_ = MakeAggregate(MergeRows(Current_->UnversionedRow, mergedAggregates));
            if (DeltaForDb_) {
                DeltaForDb_ = MergeRows(*DeltaForDb_, mergedAggregates);
            } else {
                DeltaForDb_.emplace(mergedAggregates);
            }
        }
        return Current_->TypedValue;
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TTypedValue>
void TScalarAggregatedAttribute<TTypedValue>::Store(
    TTypedValue value,
    EAggregateMode aggregateMode,
    std::optional<bool> sharedWrite,
    std::source_location location)
{
    auto appliedSharedWrite = sharedWrite.value_or(true);
    switch (aggregateMode) {
        case EAggregateMode::Unspecified:
        case EAggregateMode::Aggregate:
            Aggregate(std::move(value), appliedSharedWrite, location);
            return;
        case EAggregateMode::Override:
            Override(std::move(value), appliedSharedWrite, location);
            return;
    }
}

template <class TTypedValue>
void TScalarAggregatedAttribute<TTypedValue>::StoreDefault(
    EAggregateMode aggregateMode,
    std::optional<bool> sharedWrite,
    std::source_location location)
{
    Store(TScalarAttributeTraits<TTypedValue>::GetDefaultValue(), aggregateMode, sharedWrite, location);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TTypedValue>
TObjectKey::TKeyField TScalarAggregatedAttribute<TTypedValue>::LoadAsKeyField(const TString& /*suffix*/) const
{
    YT_ABORT();
}

template <typename TTypedValue>
TObjectKey::TKeyField TScalarAggregatedAttribute<TTypedValue>::LoadAsKeyFieldOld(const TString& /*suffix*/) const
{
    YT_ABORT();
};

template <typename TTypedValue>
std::vector<TObjectKey::TKeyField> TScalarAggregatedAttribute<TTypedValue>::LoadAsKeyFields(
    const TString& /*suffix*/) const
{
    YT_ABORT();
};

template <typename TTypedValue>
std::vector<TObjectKey::TKeyField> TScalarAggregatedAttribute<TTypedValue>::LoadAsKeyFieldsOld(
    const TString& /*suffix*/) const
{
    YT_ABORT();
};

template <typename TTypedValue>
void TScalarAggregatedAttribute<TTypedValue>::StoreKeyField(
    TObjectKey::TKeyField /*keyField*/,
    const TString& /*suffix*/)
{
    YT_ABORT();
};

template <typename TTypedValue>
void TScalarAggregatedAttribute<TTypedValue>::StoreKeyFields(
    TObjectKey::TKeyFields /*keyFields*/,
    const TString& /*suffix*/)
{
    YT_ABORT();
}

template <typename TTypedValue>
auto TScalarAggregatedAttribute<TTypedValue>::LoadAsProtos(bool /*old*/) const -> TConstProtosView
{
    YT_ABORT();
};

template <typename TTypedValue>
auto TScalarAggregatedAttribute<TTypedValue>::MutableLoadAsProtos() -> TProtosView
{
    YT_ABORT();
};

////////////////////////////////////////////////////////////////////////////////

template <class TTypedValue>
void TScalarAggregatedAttribute<TTypedValue>::Aggregate(
    TTypedValue&& value,
    bool sharedWrite,
    std::source_location location)
{
    OnStore(sharedWrite, location);
    Pending_.push_back(std::move(value));
}

template <class TTypedValue>
void TScalarAggregatedAttribute<TTypedValue>::Override(
    TTypedValue&& typedValue, bool sharedWrite, std::source_location location)
{
    OnStore(sharedWrite, location);
    if (!Override_) {
        Current_.reset();
        DeltaForCurrent_.reset();
        DeltaForDb_.reset();
    }
    Override_ = MakeAggregate(std::move(typedValue));
    Pending_.clear();
}

template<typename TTypedValue>
TScalarAggregatedAttribute<TTypedValue>::TMergerHolder::TMergerHolder(TFactory factory)
    : Factory_(std::move(factory))
{ }

template<typename TTypedValue>
NTableClient::TUnversionedRowMerger& TScalarAggregatedAttribute<TTypedValue>::TMergerHolder::GetUnderlying()
{
    if (!Merger_) {
        Merger_.emplace(Factory_());
    }
    return *Merger_;
}

template<typename TTypedValue>
void TScalarAggregatedAttribute<TTypedValue>::SetDefaultValues()
{
    YT_ABORT();
}

template <class TTypedValue>
void TScalarAggregatedAttribute<TTypedValue>::LoadOldValue(
    const NTableClient::TUnversionedValue& value,
    ILoadContext* /*context*/)
{
    // `value.Id` is a position in the rectangle request column filter and we need it to be consistent with the
    // merger table schema. This id may differ from the id used in TStoreContext.
    auto unversionedValue = value;
    unversionedValue.Id = ColumnId_;
    Old_ = MakeAggregate(ValueToRow(std::move(unversionedValue), /*captureValue*/ true));
}

template <class TTypedValue>
void TScalarAggregatedAttribute<TTypedValue>::StoreNewValue(
    NTableClient::TUnversionedValue* dbValue,
    IStoreContext* context)
{
    if (Override_) {
        if (!Pending_.empty()) {
            Override_ = MakeAggregate(MergeRows(Override_->UnversionedRow, MergePending()));
        }
        YT_VERIFY(Override_);
        YT_VERIFY(Override_->UnversionedRow.GetCount() == 1);
        YT_VERIFY(Override_->UnversionedRow[0].Flags == NTableClient::EValueFlags::None);
        Current_ = Override_;
        *dbValue = context->GetRowBuffer()->CaptureValue(Override_->UnversionedRow[0]);
        Override_.reset();
    } else {
        if (!Pending_.empty()) {
            auto mergedAggregates = MergePending();

            if (DeltaForCurrent_) {
                DeltaForCurrent_ = MergeRows(*DeltaForCurrent_, mergedAggregates);
            } else {
                DeltaForCurrent_ = mergedAggregates;
            }
            if (DeltaForDb_) {
                DeltaForDb_ = MergeRows(*DeltaForDb_, mergedAggregates);
            } else {
                DeltaForDb_ = mergedAggregates;
            }
        }
        YT_VERIFY(DeltaForDb_);
        YT_VERIFY(DeltaForDb_->GetCount() == 1);
        YT_VERIFY((*DeltaForDb_)[0].Flags == NTableClient::EValueFlags::Aggregate);
        *dbValue = context->GetRowBuffer()->CaptureValue((*DeltaForDb_)[0]);
        DeltaForDb_.reset();
    }
}

template <class TTypedValue>
NTableClient::TMutableUnversionedRow TScalarAggregatedAttribute<TTypedValue>::ValueToRow(
    NTableClient::TUnversionedValue unversionedValue,
    bool captureValue) const
{
    NTableClient::TUnversionedRowBuilder builder(/*initialValueCapacity*/ 1);
    builder.AddValue(std::move(unversionedValue));
    return RowBuffer_->CaptureRow(builder.GetRow(), /*captureValues*/ captureValue);
}

template <class TTypedValue>
NTableClient::TMutableUnversionedRow TScalarAggregatedAttribute<TTypedValue>::MergePending() const
{
    YT_VERIFY(!Pending_.empty());

    std::vector<NTableClient::TMutableUnversionedRow> rows;
    rows.reserve(Pending_.size());
    std::ranges::transform(std::move(Pending_), std::back_inserter(rows), [this] (TTypedValue typedValue) {
        auto unversionedValue = ToUnversionedValue(
            std::move(typedValue), RowBuffer_, ColumnId_, NTableClient::EValueFlags::Aggregate);
        return ValueToRow(std::move(unversionedValue), /*captureValue*/ false);
    });
    Pending_.clear();

    if (rows.size() == 1) {
        return rows.back();
    }

    auto& merger = MergerHolder_.GetUnderlying();
    merger.InitPartialRow(rows.front());
    for (auto& row: rows) {
        merger.AddPartialRow(row);
    }
    return merger.BuildMergedRow();
}

template <class TTypedValue>
NTableClient::TMutableUnversionedRow TScalarAggregatedAttribute<TTypedValue>::MergeRows(
    NTableClient::TUnversionedRow lhsRow,
    NTableClient::TUnversionedRow rhsRow) const
{
    auto& merger = MergerHolder_.GetUnderlying();
    merger.InitPartialRow(lhsRow);
    merger.AddPartialRow(lhsRow);
    YT_VERIFY(lhsRow.GetCount() == 1);
    YT_VERIFY(rhsRow.GetCount() == 1);
    YT_VERIFY(rhsRow[0].Flags == NTableClient::EValueFlags::Aggregate);
    merger.AddPartialRow(rhsRow);
    return merger.BuildMergedRow();
}

template <class TTypedValue>
auto TScalarAggregatedAttribute<TTypedValue>::MakeAggregate(TTypedValue typedValue) const -> TAggregate
{
    auto unversionedValue = ToUnversionedValue(typedValue, RowBuffer_, ColumnId_);
    return TAggregate(std::move(typedValue), ValueToRow(std::move(unversionedValue), /*captureValue*/ false));
}

// Assume that row is already captured in right RowBuffer.
template <class TTypedValue>
auto TScalarAggregatedAttribute<TTypedValue>::MakeAggregate(
    NTableClient::TMutableUnversionedRow unversionedRow) const -> TAggregate
{
    YT_VERIFY(unversionedRow.GetCount() == 1);
    unversionedRow[0].Flags = NTableClient::EValueFlags::None;
    TTypedValue typedValue;
    OrmFromUnversionedValue(&typedValue,
        unversionedRow[0],
        GetOwner()->GetSession()->GetConfigsSnapshot().TransactionManager->ScalarAttributeLoadsNullAsTyped);

    return TAggregate(std::move(typedValue), unversionedRow);
}

// TODO(kmokrov): Remove SetDefaultValues from all classes, use this implementation in TScalarAttributeBase.
template <class TTypedValue>
void TScalarAggregatedAttribute<TTypedValue>::OnObjectInitialization()
{
    LoadState_ = {
        .Value = NDetail::EAttributeLoadState::Loaded,
        .Timestamp = NDetail::EAttributeLoadState::Loaded,
    };
    StoreDefault(EAggregateMode::Override, /*sharedWrite*/ false);
}

template <typename TTypedValue>
bool TScalarAggregatedAttribute<TTypedValue>::IsChanged(
    const NYPath::TYPath& /*path*/,
    std::source_location /*location*/) const
{
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
