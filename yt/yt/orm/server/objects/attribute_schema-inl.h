#ifndef ATTRIBUTE_SCHEMA_INL_H_
#error "Direct inclusion of this file is not allowed, include attribute_schema.h"
// For the sake of sane code completion.
#include "attribute_schema.h"
#endif

#include "config.h"
#include "db_schema.h"
#include "helpers.h"
#include "key_util.h"
#include "object_manager.h"
#include "attribute_schema_traits.h"

#include <yt/yt/orm/client/objects/registry.h>

#include <yt/yt/orm/library/attributes/scalar_attribute.h>
#include <yt/yt/orm/library/attributes/helpers.h>
#include <yt/yt/orm/library/attributes/ytree.h>

#include <yt/yt/core/misc/mpl.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/tree_builder.h>
#include <yt/yt/core/ytree/tree_visitor.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <yt/yt/core/yson/protobuf_interop_options.h>
#include <yt/yt/core/yson/ypath_designated_consumer.h>
#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt_proto/yt/orm/data_model/generic.pb.h>

#include <util/charset/utf8.h>

#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

void EvaluateViewAttribute(
    const TAttributeSchema* viewAttributeSchema,
    TTransaction* transaction,
    const TObject* object,
    NYson::IYsonConsumer* consumer,
    const NYPath::TYPath& path);

////////////////////////////////////////////////////////////////////////////////

template <class TMessage>
void ParseProtobufPayload(const TString& payload, NYson::IYsonConsumer* consumer)
{
    google::protobuf::io::ArrayInputStream inputStream(
        payload.data(),
        payload.length());
    NYson::ParseProtobuf(
        consumer,
        &inputStream,
        NYson::ReflectProtobufMessageType<TMessage>());
}

////////////////////////////////////////////////////////////////////////////////

void ValidateString(std::string_view value, EUtf8Check check, NYPath::TYPathBuf path);
void ValidateString(const std::vector<TString>& values, EUtf8Check check, NYPath::TYPathBuf path);
template <class T>
    requires std::is_convertible_v<T*, google::protobuf::Message*>
void ValidateString(const THashMap<TString, T>& values, EUtf8Check check, NYPath::TYPathBuf path)
{
    if (check == EUtf8Check::Disable) {
        return;
    }
    for (const auto& [key, _] : values) {
        ValidateString(key, check, path);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TTypedValue>
inline NYTree::INodePtr ValueToNode(const TTypedValue& value)
{
    return NYTree::ConvertToNode(value);
}

template <>
inline NYTree::INodePtr ValueToNode<TInstant>(const TInstant& value)
{
    return NYTree::ConvertToNode(value.GetValue());
}

template <class TTypedValue>
inline void YsonConsumeValue(const TTypedValue& value, NYson::IYsonConsumer* consumer)
{
    NYTree::ConvertToProducer(value).Run(consumer);
}

template <>
inline void YsonConsumeValue<TInstant>(const TInstant& value, NYson::IYsonConsumer* consumer)
{
    NYTree::ConvertToProducer(value.GetValue()).Run(consumer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class TTypedObject>
TAttributeSchema* TAttributeSchema::AddUpdatePrehandler(
    std::function<void(TTransaction*, const TTypedObject*)> prehandler)
{
    UpdatePrehandlers_.push_back(
        [prehandler = std::move(prehandler)] (TTransaction* transaction, const TObject* object) {
            const auto* typedObject = object->As<TTypedObject>();
            prehandler(transaction, typedObject);
        });
    return this;
}

template <class TTypedObject>
TAttributeSchema* TAttributeSchema::AddUpdateHandler(std::function<void(
    TTransaction*,
    TTypedObject*)> handler)
{
    UpdateHandlers_.push_back(
        [handler = std::move(handler)] (TTransaction* transaction, TObject* object) {
            auto* typedObject = object->As<TTypedObject>();
            handler(transaction, typedObject);
        });
    return this;
}

template <class TTypedObject>
TAttributeSchema* TAttributeSchema::AddValidator(std::function<void(TTransaction*, const TTypedObject*)> handler)
{
    YT_VERIFY(!ForbidValidators_);
    Validators_.push_back(
        [handler = std::move(handler)] (TTransaction* transaction, const TObject* object) {
            const auto* typedObject = object->As<TTypedObject>();
            handler(transaction, typedObject);
        });
    return this;
}

template <class TTypedObject>
TAttributeSchema* TAttributeSchema::SetPreloader(std::function<void(const TTypedObject*)> preloader)
{
    Preloader_ =
        [preloader = std::move(preloader)] (const TObject* object, const NYPath::TYPath&) {
            const auto* typedObject = object->As<TTypedObject>();
            preloader(typedObject);
        };
    return this;
}

////////////////////////////////////////////////////////////////////////////////

template <class TTypedObject>
THistoryEnabledAttributeSchema& THistoryEnabledAttributeSchema::SetValueFilter(
    std::function<bool(const TTypedObject*)> valueFilter)
{
    ValueFilter_ =
        [valueFilter = std::move(valueFilter)] (const TObject* object) {
            const auto* typedObject = object->As<TTypedObject>();
            return valueFilter(typedObject);
        };
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

template <class TTypedObject, class TTypedValue>
void TAttributeSchema::InitPolicyValidator(
    const TScalarAttributeDescriptor<TTypedObject, TTypedValue>& descriptor,
    TIntrusivePtr<IAttributePolicy<TTypedValue>> policy)
{
    AddValidator<TTypedObject>(
        [&descriptor, policy, this] (TTransaction*, const TTypedObject* typedObject) {
            auto* attribute = descriptor.AttributeGetter(typedObject);
            policy->Validate(attribute->Load(), FormatTypePath());
        });
}

////////////////////////////////////////////////////////////////////////////////

template <class TTypedObject>
TScalarAttributeSchema* TScalarAttributeSchema::SetChangedGetter(std::function<bool(
    const TTypedObject*,
    const NYPath::TYPath&)> changedGetter)
{
    ChangedGetterType_ = EChangedGetterType::Custom;
    ChangedGetter_ =
        [changedGetter = std::move(changedGetter)] (const TObject* object, const NYPath::TYPath& path) {
            const auto* typedObject = object->As<TTypedObject>();
            return changedGetter(typedObject, path);
        };

    return this;
}

template <class TTypedObject>
TScalarAttributeSchema* TScalarAttributeSchema::SetFilteredChangedGetter(std::function<bool(
    const TTypedObject*,
    const NYPath::TYPath& path,
    std::function<bool(const NYPath::TYPath&)>)> filteredChangedGetter)
{
    FilteredChangedGetterType_ = EChangedGetterType::Custom;
    FilteredChangedGetter_ =
        [filteredChangedGetter = std::move(filteredChangedGetter)] (
            const TObject* object,
            const NYPath::TYPath& path,
            std::function<bool(const NYPath::TYPath& path)> byPathFilter)
        {
            const auto* typedObject = object->As<TTypedObject>();
            return filteredChangedGetter(typedObject, path, byPathFilter);
        };

    return this;
}

template <class TTypedObject>
TScalarAttributeSchema* TScalarAttributeSchema::SetAttributeGetter(
    std::function<const TAttributeBase*(const TTypedObject*)> getter)
{
    AttributeGetter_ =
        [getter = std::move(getter)] (const TObject* object) -> const TAttributeBase* {
            return getter(object->As<TTypedObject>());
        };
    return this;
}

template <typename TTypedObject, typename TTypedValue>
TScalarAttributeSchema* TScalarAttributeSchema::SetTypedValueSetter(std::function<void(
    TTransaction*,
    TTypedObject*,
    const NYPath::TYPath&,
    const TTypedValue&,
    bool recursive,
    std::optional<bool> sharedWrite,
    EAggregateMode aggregateMode,
    const TTransactionCallContext&)> setter)
{
    ValueSetter_ =
        [setter = std::move(setter), this] (
            TTransaction* transaction,
            TObject* object,
            const NYPath::TYPath& path,
            const NYTree::INodePtr& value,
            bool recursive,
            std::optional<bool> sharedWrite,
            EAggregateMode aggregateMode,
            const TTransactionCallContext& transactionCallContext)
        {
            auto typedValue = TScalarAttributeTraits<TTypedValue>::GetDefaultValue();
            UpdateScalarAttributeValue(
                this,
                path,
                value,
                typedValue,
                recursive,
                /*forceSetViaYson*/ this->IsExtensible());
            auto* typedObject = object->As<TTypedObject>();
            setter(
                transaction,
                typedObject,
                path,
                typedValue,
                recursive,
                sharedWrite,
                aggregateMode,
                transactionCallContext);
        };

    return this;
}

template <class TTypedObject>
TScalarAttributeSchema* TScalarAttributeSchema::SetLocker(std::function<void(
    TTransaction*,
    TTypedObject*,
    const NYPath::TYPath&,
    NTableClient::ELockType)> locker)
{
    Locker_ =
        [locker = std::move(locker)] (
            TTransaction* transaction,
            TObject* object,
            const NYPath::TYPath& path,
            NTableClient::ELockType lockType)
        {
            auto* typedObject = object->As<TTypedObject>();
            locker(transaction, typedObject, path, lockType);
        };

    return this;
}

template <class TTypedObject>
TScalarAttributeSchema* TScalarAttributeSchema::SetRemover(std::function<void(
    TTransaction*,
    TTypedObject*,
    const NYPath::TYPath&,
    bool)> remover)
{
    Remover_ =
        [remover = std::move(remover)] (
            TTransaction* transaction,
            TObject* object,
            const NYPath::TYPath& path,
            bool force)
        {
            auto* typedObject = object->As<TTypedObject>();
            remover(transaction, typedObject, path, force);
        };

    return this;
}

template <class TTypedObject>
TScalarAttributeSchema* TScalarAttributeSchema::SetTimestampPregetter(std::function<void(
    const TTypedObject*,
    const NYPath::TYPath&)> timestampPregetter)
{
    TimestampPregetter_ =
        [timestampPregetter = std::move(timestampPregetter)] (const TObject* object, const NYPath::TYPath& path) {
            const auto* typedObject = object->As<TTypedObject>();
            timestampPregetter(typedObject, path);
        };

    return this;
}

template <class TTypedObject>
TScalarAttributeSchema* TScalarAttributeSchema::SetTimestampGetter(std::function<TTimestamp(
    const TTypedObject*,
    const NYPath::TYPath&)> timestampGetter)
{
    TimestampGetter_ =
        [timestampGetter = std::move(timestampGetter)] (const TObject* object, const NYPath::TYPath& path) {
            const auto* typedObject = object->As<TTypedObject>();
            return timestampGetter(typedObject, path);
        };

    return this;
}

template <std::derived_from<TAttributeBase> TAttribute>
const TAttribute* TScalarAttributeSchema::GetAttribute(const TObject* object) const
{
    const auto* attribute = AttributeGetter_(object);
    if constexpr (std::same_as<TAttributeBase, TAttribute>) {
        return attribute;
    } else {
        auto* typedAttribute = dynamic_cast<const TAttribute*>(attribute);
        if (Y_UNLIKELY(!typedAttribute)) {
            THROW_ERROR_EXCEPTION("Attribute type mismatch for %v",
                FormatTypePath())
                << TErrorAttribute("object_key", object->GetKey());
        }
        return typedAttribute;
    }
}

template <std::derived_from<TAttributeBase> TAttribute>
TAttribute* TScalarAttributeSchema::GetAttribute(TObject* object) const
{
    return const_cast<TAttribute*>(GetAttribute<TAttribute>(&std::as_const(*object)));
}

template <class TTypedObject, class TTypedValue>
TScalarAttributeSchema* TScalarAttributeSchema::SetPolicy(
    const TScalarAttributeDescriptor<TTypedObject, TTypedValue>& descriptor,
    TIntrusivePtr<IAttributePolicy<TTypedValue>> policy)
{
    YT_VERIFY(policy);

    InitPolicyInitializer(descriptor, policy);
    InitPolicyValidator(descriptor, policy);
    return this;
}

template <class TTypedObject>
TScalarAttributeSchema* TScalarAttributeSchema::SetStoreScheduledGetter(std::function<bool(
    const TTypedObject*)> storeScheduledGetter)
{
    StoreScheduledGetter_ =
        [storeScheduledGetter = std::move(storeScheduledGetter)] (const TObject* object) {
            const auto* typedObject = object->As<TTypedObject>();
            return storeScheduledGetter(typedObject);
        };

    return this;
}

template <class TTypedObject>
TScalarAttributeSchema* TScalarAttributeSchema::SetUpdatePreloader(
    std::function<void(TTransaction*, const TTypedObject*, const TUpdateRequest&)> preupdater)
{
    UpdatePreloader_ =
        [preupdater = std::move(preupdater)] (TTransaction* transaction, const TObject* object, const TUpdateRequest& updateRequest) {
            const auto* typedObject = object->As<TTypedObject>();
            preupdater(transaction, typedObject, updateRequest);
        };
    return this;
}

////////////////////////////////////////////////////////////////////////////////

template <class TTypedObject, class TTypedRequest, class TTypedResponse>
TScalarAttributeSchema* TScalarAttributeSchema::SetMethod(std::function<void(
    TTransaction*,
    TTypedObject*,
    const NYPath::TYPath&,
    const TTypedRequest&,
    TTypedResponse*)> method)
{
    SetUpdatePolicy(EUpdatePolicy::Updatable);
    MethodEvaluator_ =
        [this, method = std::move(method)] (
            TTransaction* transaction,
            TObject* object,
            const NYPath::TYPath& path,
            const NYTree::INodePtr& value,
            NYson::IYsonConsumer* consumer)
        {
            THROW_ERROR_EXCEPTION_UNLESS(path.empty(), "Method does not support partial payload");
            auto typedValue = ParseScalarAttributeFromYsonNode<TTypedRequest>(this, value);
            auto* typedObject = object->As<TTypedObject>();
            object->ScheduleStore();
            TTypedResponse response;
            method(transaction, typedObject, path, typedValue, &response);
            consumer->OnRaw(NYson::ConvertToYsonString(response));
        };

    if (auto* scalarSchema = TryAsScalar(); !scalarSchema->HasChangedGetter()) {
        scalarSchema->SetChangedGetter<TTypedObject>([this] (const TTypedObject* object, const NYPath::TYPath& path) {
            THROW_ERROR_EXCEPTION_UNLESS(path.empty(),
                "Cannot collect changes at the non-empty path %v: method attribute is not composite",
                path);
            return object->ControlAttributeObserver().IsCalled(GetPath());
        });
    }
    return this;
}

template <class TTypedObject, class TTypedValue>
TScalarAttributeSchema* TScalarAttributeSchema::SetControl(std::function<void(
    TTransaction*,
    TTypedObject*,
    const TTypedValue&)> control)
{
    SetUpdatePolicy(EUpdatePolicy::Updatable);
    SetMethod<TTypedObject, TTypedValue, NDataModel::TVoid>([this, control = std::move(control)] (
        TTransaction* transaction,
        TTypedObject* object,
        const NYPath::TYPath& path,
        const TTypedValue& value,
        auto* /*response*/)
    {
        THROW_ERROR_EXCEPTION_UNLESS(path.empty(),
            "Partial updates are not supported");
        control(transaction, object, value);
        object->ControlAttributeObserver().OnCall(GetPath());
    });
    SetTypedValueSetter<TTypedObject, TTypedValue>([this, control = std::move(control)] (
        TTransaction* transaction,
        TTypedObject* object,
        const NYPath::TYPath& path,
        const TTypedValue& value,
        bool /*recursive*/,
        std::optional<bool> sharedWrite,
        EAggregateMode aggregateMode,
        const TTransactionCallContext& /*transactionCallContext*/)
    {
        THROW_ERROR_EXCEPTION_UNLESS(path.empty(),
            "Partial updates are not supported");
        THROW_ERROR_EXCEPTION_IF(sharedWrite && *sharedWrite || aggregateMode != EAggregateMode::Unspecified,
            NClient::EErrorCode::InvalidRequestArguments,
            "%Qv control cannot be updated with shared write lock / aggregate mode",
            object->GetDisplayName());
        object->ScheduleStore();
        control(transaction, object, value);
        object->ControlAttributeObserver().OnCall(GetPath());
    });
    if (auto* scalarSchema = TryAsScalar(); !scalarSchema->HasChangedGetter()) {
        scalarSchema->SetChangedGetter<TTypedObject>([this] (const TTypedObject* object, const NYPath::TYPath& path) {
            THROW_ERROR_EXCEPTION_UNLESS(path.empty(),
                "Cannot collect changes at the non-empty path %v: control attribute is not composite",
                path);
            return object->ControlAttributeObserver().IsCalled(GetPath());
        });
    }
    return this;
}

template <class TTypedObject, class TTypedValue>
TScalarAttributeSchema* TScalarAttributeSchema::SetControlWithTransactionCallContext(std::function<void(
    TTransaction*,
    TTypedObject*,
    const TTypedValue&,
    const TTransactionCallContext&)> control)
{
    SetUpdatePolicy(EUpdatePolicy::Updatable);
    SetTypedValueSetter<TTypedObject, TTypedValue>([this, control = std::move(control)] (
        TTransaction* transaction,
        TTypedObject* object,
        const NYPath::TYPath& path,
        const TTypedValue& value,
        bool /*recursive*/,
        std::optional<bool> sharedWrite,
        EAggregateMode aggregateMode,
        const TTransactionCallContext& transactionCallContext)
    {
        THROW_ERROR_EXCEPTION_UNLESS(path.empty(),
            "Partial updates are not supported");
        THROW_ERROR_EXCEPTION_IF(sharedWrite && *sharedWrite || aggregateMode != EAggregateMode::Unspecified,
            NClient::EErrorCode::InvalidRequestArguments,
            "%Qv control cannot be updated with shared write lock / aggregate mode",
            object->GetDisplayName());
        object->ScheduleStore();
        control(transaction, object, value, transactionCallContext);
        object->ControlAttributeObserver().OnCall(GetPath());
    });
    if (auto* scalarSchema = TryAsScalar(); !scalarSchema->HasChangedGetter()) {
        scalarSchema->SetChangedGetter<TTypedObject>([this] (const TTypedObject* object, const NYPath::TYPath& path) {
            THROW_ERROR_EXCEPTION_UNLESS(path.empty(),
                "Cannot collect changes at the non-empty path %v: control attribute is not composite",
                path);
            return object->ControlAttributeObserver().IsCalled(GetPath());
        });
    }

    return this;
}

template <class TTypedObject>
TScalarAttributeSchema* TScalarAttributeSchema::SetValueGetter(std::function<void(
    TTransaction*,
    const TTypedObject*,
    NYson::IYsonConsumer*)> valueGetter)
{
    ValueGetterType_ = EValueGetterType::Default;
    ValueGetter_ =
        [valueGetter = std::move(valueGetter)] (
            TTransaction* transaction,
            const TObject* object,
            NYson::IYsonConsumer* consumer,
            const NYPath::TYPath& path)
        {
            TValuePresentConsumer valuePresentConsumer(consumer);
            auto pathConsumer = NYson::CreateYPathDesignatedConsumer(
                path,
                NYson::EMissingPathMode::Ignore,
                &valuePresentConsumer);
            valueGetter(transaction, object->As<TTypedObject>(), pathConsumer.get());
            if (!valuePresentConsumer.IsValuePresent()) {
                consumer->OnEntity();
            }
        };
    return this;
}

template <class TTypedObject>
TScalarAttributeSchema* TScalarAttributeSchema::SetValueGetter(std::function<void(
    TTransaction*,
    const TTypedObject*,
    NYson::IYsonConsumer*,
    const NYPath::TYPath& path)> valueGetter)
{
    ValueGetterType_ = EValueGetterType::Evaluator;
    ValueGetter_ =
        [valueGetter = std::move(valueGetter)] (
            TTransaction* transaction,
            const TObject* object,
            NYson::IYsonConsumer* consumer,
            const NYPath::TYPath& path)
        {
            valueGetter(transaction, object->As<TTypedObject>(), consumer, path);
        };
    return this;
}

template <std::derived_from<TScalarAttributeDescriptorBase> TDescriptor>
const TDescriptor* TScalarAttributeSchema::GetAttributeDescriptor() const
{
    if (Y_UNLIKELY(!AttributeDescriptor_)) {
        THROW_ERROR_EXCEPTION("Attribute descriptor is not set for %v", FormatTypePath());
    }
    if constexpr (std::same_as<TDescriptor, TScalarAttributeDescriptorBase>) {
        return AttributeDescriptor_;
    } else {
        auto* typedDescriptor = dynamic_cast<const TDescriptor*>(AttributeDescriptor_);
        if (Y_UNLIKELY(!typedDescriptor)) {
            THROW_ERROR_EXCEPTION("Attribute descriptor type mismatch for %v",
                FormatTypePath());
        }
        return typedDescriptor;
    }
}

template <std::derived_from<TScalarAttributeDescriptorBase> TDescriptor>
bool TScalarAttributeSchema::HasAttributeDescriptor() const
{
    if (!AttributeDescriptor_) {
        return false;
    }
    if constexpr (std::same_as<TDescriptor, TScalarAttributeDescriptorBase>) {
        return true;
    } else {
        auto* typedDescriptor = dynamic_cast<const TDescriptor*>(AttributeDescriptor_);
        return typedDescriptor;
    }
}

template <class TTypedValue>
TScalarAttributeSchema* TScalarAttributeSchema::SetKeyFieldPolicy(TIntrusivePtr<IAttributePolicy<TTypedValue>> policy)
{
    YT_VERIFY(IsIdAttribute_);
    YT_VERIFY(!KeyFieldValidator_);
    YT_VERIFY(!KeyFieldGenerator_);
    YT_VERIFY(policy);

    KeyFieldValidator_ =
        [policy, this] (const TObjectKey::TKeyField& keyField, std::string_view overrideTitle) {
            const auto* value = std::get_if<TTypedValue>(&keyField);
            std::string title = overrideTitle.empty() ? FormatTypePath() : std::string(overrideTitle);
            THROW_ERROR_EXCEPTION_IF(!value,
                "%v %Qv is not of an appropriate type",
                title,
                *value);
            policy->Validate(*value, title);
            if constexpr (std::is_same_v<TTypedValue, TString>) {
                NDetail::ValidateString(*value, ObjectManager_->GetConfig()->Utf8Check, FormatTypePath());
            }
        };
    KeyFieldGenerator_ =
        [policy, this] (TTransaction* transaction) {
            return policy->Generate(transaction, FormatTypePath());
        };
    return this;
}

template <class TTypedObject, class TTypedValue>
void TScalarAttributeSchema::InitPolicyInitializer(
    const TScalarAttributeDescriptor<TTypedObject, TTypedValue>& descriptor,
    TIntrusivePtr<IAttributePolicy<TTypedValue>> policy)
{
    if (policy->GetGenerationPolicy() == EAttributeGenerationPolicy::Manual) {
        return;
    }
    YT_VERIFY(!descriptor.GetInitializer());
    YT_VERIFY(!Initializer_);
    Initializer_ =
        [&descriptor, policy, this] (TTransaction* transaction, TObject* object) {
            auto* typedObject = object->As<TTypedObject>();
            auto* attribute = descriptor.AttributeGetter(typedObject);
            TTypedValue value = policy->Generate(transaction, FormatTypePath());
            descriptor.RunValidators(typedObject, attribute->Load(), &value);
            attribute->Store(value);
        };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
