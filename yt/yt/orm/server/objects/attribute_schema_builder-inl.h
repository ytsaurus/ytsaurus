#ifndef ATTRIBUTE_SCHEMA_BUILDER_INL_H_
#error "Direct inclusion of this file is not allowed, include attribute_schema_builder.h"
// For the sake of sane code completion.
#include "attribute_schema_builder.h"
#endif

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <std::derived_from<TObject> TTypedObject, class TTypedValue>
TScalarAttributeSchema* TScalarAttributeSchemaBuilder::SetAttribute(
    const TScalarAttributeDescriptor<TTypedObject, TTypedValue>& descriptor)
{
    Schema_->AttributeDescriptor_ = &descriptor;

    Schema_->AttributeGetter_ =
        [&descriptor] (const TObject* object) -> const TAttributeBase* {
            const auto* typedObject = object->As<TTypedObject>();
            return descriptor.AttributeGetter(typedObject);
        };

    if constexpr (std::is_convertible_v<TTypedValue*, google::protobuf::Message*>) {
        if (Schema_->IsEtc()) {
            const auto* descriptor = TTypedValue::GetDescriptor();
            int fieldCount = descriptor->field_count();
            std::vector<std::string> fields;
            fields.reserve(fieldCount);
            for (int i = 0; i < fieldCount; ++i) {
                fields.push_back(descriptor->field(i)->name());
            }
            Schema_->SetNestedFieldsMap(std::move(fields));
        }
    }

    InitInitializer(descriptor);
    InitValueSetter(descriptor);
    InitValueGetter(descriptor);
    InitDefaultValueGetter<TTypedValue>();
    InitStoreScheduledGetter<TTypedObject, TTypedValue>(descriptor);
    InitChangedGetter(descriptor);
    InitFilteredChangedGetter(descriptor);
    InitTimestampGetter<TTypedObject, TTypedValue>(descriptor);
    InitTimestampExpressionBuilder(descriptor);
    InitRemover(descriptor);
    InitLocker<TTypedObject, TTypedValue>(descriptor);
    InitPreloader(descriptor);
    InitUpdatePreloader(descriptor);
    Schema_->ExpressionBuilder_ =
        [&descriptor, schema = Schema_] (
            IQueryContext* context,
            const NYPath::TYPath& path,
            EAttributeExpressionContext expressionContext)
        {
            auto type = ValidateAndExtractTypeFromScalarAttribute<TTypedValue>(/*attribute*/ schema, path);
            return context->GetScalarAttributeExpression(descriptor.Field, path, expressionContext, type);
        };
    InitListContainsExpressionBuilder(descriptor);
    Schema_->ValueFilteredChildrenValidator_ = [&descriptor] {
        return descriptor.ScheduleLoadOnStore;
    };
    Schema_->Field_ = descriptor.Field;

    AddPostValidatorIfNecessary<TTypedObject, TTypedValue>(descriptor);

    return Schema_;
}

template <std::derived_from<TObject> TTypedObject, class TTypedValue>
TScalarAttributeSchema* TScalarAttributeSchemaBuilder::SetAttribute(
    const TScalarAggregatedAttributeDescriptor<TTypedObject, TTypedValue>& descriptor)
{
    Schema_->AttributeDescriptor_ = &descriptor;

    Schema_->AttributeGetter_ =
        [&descriptor] (const TObject* object) -> const TAttributeBase* {
            const auto* typedObject = object->As<TTypedObject>();
            return descriptor.AttributeGetter(typedObject);
        };

    Schema_->ValueSetter_ =
        [&descriptor, schema = Schema_] (
            TTransaction* /*transaction*/,
            TObject* object,
            const NYPath::TYPath& path,
            const NYTree::INodePtr& value,
            bool /*recursive*/,
            std::optional<bool> sharedWrite,
            EAggregateMode aggregateMode,
            const TTransactionCallContext& /*transactionCallContext*/)
        {
            // TODO(kmokrov): Support updates with nonempty path
            if (!path.empty()) {
                THROW_ERROR_EXCEPTION("Update of aggregated %v attribute %v with nonempty path is not supported",
                    NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(
                        schema->TypeHandler_->GetType()),
                    schema->GetPath());
            }

            auto* typedObject = object->As<TTypedObject>();
            auto* attribute = descriptor.AttributeGetter(typedObject);
            auto typedValue = ParseScalarAttributeFromYsonNode<TTypedValue>(schema, value);
            // When creating an object, a default value is written to its attribute. If there is an explicit value for
            // this attribute specified in the creation request, it overrides the default. Without the Override mode it
            // will require the use of a merger or in case of the "First" aggregate function an incorrect value will be
            // written to the database.
            if (object->GetState() == EObjectState::Creating) {
                sharedWrite = false;
                aggregateMode = EAggregateMode::Override;
            }
            attribute->Store(std::move(typedValue), aggregateMode, sharedWrite);
        };

    InitTimestampGetter<TTypedObject, TTypedValue>(descriptor);
    InitTimestampExpressionBuilder(descriptor);
    InitDefaultValueGetter<TTypedValue>();
    InitStoreScheduledGetter<TTypedObject, TTypedValue>(descriptor);

    Schema_->Remover_ =
        [&descriptor, schema = Schema_] (
            TTransaction* /*transaction*/,
            TObject* object,
            const NYPath::TYPath& path,
            bool /*force*/)
        {
            // TODO(kmokrov): Support updates with nonempty path
            if (!path.empty()) {
                THROW_ERROR_EXCEPTION("Update of aggregated %v attribute %v with nonempty path is not supported",
                    NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(
                        schema->TypeHandler_->GetType()),
                    schema->GetPath());
            }
            auto* typedObject = object->As<TTypedObject>();
            auto* attribute = descriptor.AttributeGetter(typedObject);

            attribute->StoreDefault(EAggregateMode::Override, /*sharedWrite*/ false);
        };

    InitLocker<TTypedObject, TTypedValue>(descriptor);

    Schema_->ExpressionBuilder_ =
        [&descriptor, schema = Schema_] (
            IQueryContext* context,
            const NYPath::TYPath& path,
            EAttributeExpressionContext expressionContext)
        {
            auto type = ValidateAndExtractTypeFromScalarAttribute<TTypedValue>(/*attribute*/ schema, path);
            return context->GetScalarAttributeExpression(descriptor.Field, path, expressionContext, type);
        };
    Schema_->Field_ = descriptor.Field;

    AddPostValidatorIfNecessary<TTypedObject, TTypedValue>(descriptor);

    Schema_->SetOpaqueForHistory();
    Schema_->Aggregated_ = true;

    return Schema_;
}

template <class TOne, class TMany>
TScalarAttributeSchema* TScalarAttributeSchemaBuilder::SetAttribute(
    const TManyToOneAttributeDescriptor<TMany, TOne>& descriptor)
{
    Schema_->AttributeDescriptor_ = &descriptor;

    using TOneKeyFirstField = std::tuple_element_t<0, typename TObjectKeyTraits<TOne>::TTypes>;
    InitDefaultValueGetter<TOneKeyFirstField>();

    auto parseOne = [&descriptor] (TTransaction* transaction, const NYTree::INodePtr& value) -> TObject*
    {
        TObjectKey::TKeyField oneKeyFirstField = ParseObjectKeyField(value, descriptor.Field->Type);
        return oneKeyFirstField == TObjectKey::TKeyField(TOneKeyFirstField())
            ? nullptr
            : transaction->GetObject(TOne::Type, TObjectKey(oneKeyFirstField));
    };

    Schema_->AttributeGetter_ =
        [&descriptor] (const TObject* many) -> const TAttributeBase* {
            return descriptor.ForwardAttributeGetter(many->As<TMany>());
        };

    Schema_->Preloader_ =
        [&descriptor] (const TObject* many, const NYPath::TYPath&) {
            descriptor.ForwardAttributeGetter(many->As<TMany>())->ScheduleLoad();
        };

    Schema_->UpdatePreloader_ =
        [parseOne, schema = Schema_] (TTransaction* transaction, const TObject* many, const TUpdateRequest& request)
        {
            Visit(request,
                [&] (const TSetUpdateRequest& typedRequest) {
                    THROW_ERROR_EXCEPTION_UNLESS(typedRequest.Path.empty(),
                        "Partial updates are not supported");

                    auto* one = parseOne(transaction, typedRequest.Value);
                    Y_UNUSED(one);
                    schema->RunPreloader(many);
                },
                [&] (const TLockUpdateRequest& /*typedRequest*/) {
                },
                [&] (const TRemoveUpdateRequest& /*typedRequest*/) {
                    schema->RunPreloader(many);
                },
                [&] (const TMethodRequest& /*typedRequest*/) {
                    schema->RunPreloader(many);
                });
        };

    Schema_->ValueSetter_ =
        [parseOne, &descriptor] (
            TTransaction* transaction,
            TObject* many,
            const NYPath::TYPath& path,
            const NYTree::INodePtr& value,
            bool /*recursive*/,
            std::optional<bool> sharedWrite,
            EAggregateMode aggregateMode,
            const TTransactionCallContext& /*transactionCallContext*/)
        {
            THROW_ERROR_EXCEPTION_UNLESS(path.empty(),
                "Partial updates are not supported");
            if (sharedWrite && *sharedWrite || aggregateMode != EAggregateMode::Unspecified) {
                THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments,
                    "Many to One attribute cannot be updated with shared write lock / aggregate mode")
                    << TErrorAttribute("many",
                        NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(TMany::Type))
                    << TErrorAttribute("one",
                        NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(TOne::Type));
            }

            auto* typedMany = TObjectPluginTraits<TMany>::Downcast(many->As<TMany>());
            if (TObject* one = parseOne(transaction, value))  {
                one->ValidateExists();
                auto* typedOne = one->As<TOne>();
                auto* inverseAttribute = descriptor.InverseAttributeGetter(typedOne);
                inverseAttribute->Add(typedMany);
            } else {
                THROW_ERROR_EXCEPTION_UNLESS(descriptor.Nullable,
                    "Cannot set null %v",
                    NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(TOne::Type));
                auto* forwardAttribute = descriptor.ForwardAttributeGetter(typedMany);
                auto* currentTypedOne = forwardAttribute->Load();
                if (currentTypedOne) {
                    auto* inverseAttribute = descriptor.InverseAttributeGetter(currentTypedOne);
                    inverseAttribute->Remove(typedMany);
                }
            }
        };

    Schema_->Locker_ =
        [&descriptor] (
            TTransaction* /*transaction*/,
            TObject* many,
            const NYPath::TYPath& path,
            NTableClient::ELockType lockType)
        {
            THROW_ERROR_EXCEPTION_UNLESS(path.empty(),
                "Partial locks are not supported");

            descriptor.ForwardAttributeGetter(many->As<TMany>())
                ->Lock(lockType);
        };

    Schema_->Remover_ =
        [&descriptor] (
            TTransaction* /*transaction*/,
            TObject* many,
            const NYPath::TYPath& path,
            bool /*force*/)
        {
            THROW_ERROR_EXCEPTION_UNLESS(path.empty(),
                "Partial removes are not supported");

            THROW_ERROR_EXCEPTION_UNLESS(descriptor.Nullable,
                "Cannot set null %v",
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(TOne::Type));

            auto* typedMany = TObjectPluginTraits<TMany>::Downcast(many->As<TMany>());
            auto* forwardAttribute = descriptor.ForwardAttributeGetter(typedMany);
            auto* currentTypedOne = forwardAttribute->Load();
            if (currentTypedOne) {
                auto* inverseAttribute = descriptor.InverseAttributeGetter(currentTypedOne);
                inverseAttribute->Remove(typedMany);
            }
        };

    Schema_->TimestampPregetter_ =
        [&descriptor] (const TObject* many, const NYPath::TYPath& /*path*/)
        {
            return descriptor.ForwardAttributeGetter(many->As<TMany>())->ScheduleLoadTimestamp();
        };

    Schema_->TimestampGetter_ =
        [&descriptor] (const TObject* many, const NYPath::TYPath& /*path*/)
        {
            return descriptor.ForwardAttributeGetter(many->As<TMany>())->LoadTimestamp();
        };

    InitTimestampExpressionBuilder(descriptor);

    Schema_->SetValueGetter<TMany>(
        [&descriptor] (
            TTransaction* /*transaction*/,
            const TMany* object,
            NYson::IYsonConsumer* consumer)
        {
            auto* forwardAttribute = descriptor.ForwardAttributeGetter(object);
            auto key = forwardAttribute->LoadKey();
            YT_VERIFY(key.size() == 1);
            if (!key[0].IsZero()) {
                NDetail::YsonConsumeValue(key[0], consumer);
            } else {
                const auto& configsSnapshot = object->GetSession()->GetConfigsSnapshot();
                if (configsSnapshot.TransactionManager->AnyToOneAttributeValueGetterReturnsNull) {
                    consumer->OnEntity();
                } else {
                    NDetail::YsonConsumeValue(TOneKeyFirstField{}, consumer);
                }
            }
        });

    Schema_->StoreScheduledGetter_  =
        [&descriptor] (const TObject* many) {
            return descriptor.ForwardAttributeGetter(many->As<TMany>())->IsStoreScheduled();
        };

    Schema_->ChangedGetterType_ = EChangedGetterType::Default;
    Schema_->ChangedGetter_ =
        [&descriptor] (const TObject* many, const NYPath::TYPath& path) {
            THROW_ERROR_EXCEPTION_UNLESS(path.empty(),
                "Cannot collect changes at the non-empty path %v: many-to-one attribute is not composite",
                path);

            return descriptor.ForwardAttributeGetter(many->As<TMany>())->IsChanged();
        };

    Schema_->SetExpressionBuilder(
        ValidatePathIsEmpty,
        [&descriptor] (
            IQueryContext* context,
            const NYPath::TYPath& path,
            EAttributeExpressionContext expressionContext)
        {
            return context->GetScalarAttributeExpression(
                descriptor.Field,
                path,
                expressionContext,
                descriptor.Field->Type);
        });

    Schema_->Field_ = descriptor.Field;
    return Schema_;
}

template <class TOne, class TMany>
TScalarAttributeSchema* TScalarAttributeSchemaBuilder::SetAttribute(
    const TOneToManyAttributeDescriptor<TOne, TMany>& descriptor)
{
    Schema_->SetOpaque();

    Schema_->AttributeGetter_ =
        [&descriptor] (const TObject* one) -> const TAttributeBase* {
            return descriptor.ForwardAttributeGetter(one->As<TOne>());
        };

    Schema_->SetValueGetter<TOne>(
        [&descriptor] (
            TTransaction* /*transaction*/,
            const TOne* object,
            NYson::IYsonConsumer* consumer)
        {
            auto* forwardAttribute = descriptor.ForwardAttributeGetter(object);
            auto attributeValue = forwardAttribute->Load();

            std::sort(
                attributeValue.begin(),
                attributeValue.end(),
                [] (const auto& lhs, const auto& rhs) {
                    return lhs->GetKey() < rhs->GetKey();
                });
            return NYTree::BuildYsonFluently(consumer)
                .DoListFor(attributeValue, [] (auto list, auto* many) {
                    list.Item().Value(many->GetKey().ToString());
                });
        });

    Schema_->Preloader_ =
        [&descriptor] (const TObject* one, const NYPath::TYPath& /*path*/) {
            const auto* typedOne = one->As<TOne>();
            const auto* forwardAttribute = descriptor.ForwardAttributeGetter(typedOne);
            forwardAttribute->ScheduleLoad();
        };

    Schema_->StoreScheduledGetter_  =
        [&descriptor] (const TObject* one) {
            const auto* typedOne = one->As<TOne>();
            const auto* forwardAttribute = descriptor.ForwardAttributeGetter(typedOne);
            return forwardAttribute->IsStoreScheduled();
        };

    Schema_->ChangedGetterType_ = EChangedGetterType::Default;
    Schema_->ChangedGetter_  =
        [&descriptor] (const TObject* one, const NYPath::TYPath& path) {
            THROW_ERROR_EXCEPTION_UNLESS(path.empty(),
                "Cannot collect changes at the non-empty path %v: one-to-many attribute is not composite",
                path);
            const auto* typedOne = one->As<TOne>();
            const auto* forwardAttribute = descriptor.ForwardAttributeGetter(typedOne);
            return forwardAttribute->IsChanged();
        };

    return Schema_;
}

template <class TThis, class TThat>
TScalarAttributeSchema* TScalarAttributeSchemaBuilder::SetAttribute(
    const TOneToOneAttributeDescriptor<TThis, TThat>& descriptor)
{
    using TThatKeyFirstField = std::tuple_element_t<0, typename TObjectKeyTraits<TThat>::TTypes>;
    InitDefaultValueGetter<TObjectId>();

    auto parseOne = [&descriptor] (TTransaction* transaction, const NYTree::INodePtr& value)
    {
        TObjectKey::TKeyField id = ParseObjectKeyField(value, descriptor.Field->Type);
        return id == TObjectKey::TKeyField(TThatKeyFirstField())
            ? transaction->GetTypedObject<TThat>(TObjectKey(id))
            : nullptr;
    };

    Schema_->AttributeGetter_ =
        [&descriptor] (const TObject* this_) -> const TAttributeBase* {
            return descriptor.ForwardAttributeGetter(this_->As<TThis>());
        };

    Schema_->Preloader_ =
        [&descriptor] (const TObject* this_, const NYPath::TYPath&) {
            const auto* typedThis = this_->As<TThis>();
            auto* forwardAttribute = descriptor.ForwardAttributeGetter(typedThis);
            forwardAttribute->ScheduleLoad();
        };

    Schema_->UpdatePreloader_ =
        [parseOne, schema = Schema_] (
            TTransaction* transaction,
            const TObject* object,
            const TUpdateRequest& updateRequest)
        {
            Visit(updateRequest,
                [&] (const TSetUpdateRequest& typedRequest) {
                    THROW_ERROR_EXCEPTION_UNLESS(typedRequest.Path.empty(),
                        "Partial updates are not supported");

                    auto* one = parseOne(transaction, typedRequest.Value);
                    Y_UNUSED(one);
                    schema->RunPreloader(object);
                },
                [&] (const TLockUpdateRequest& /*typedRequest*/) {
                },
                [&] (const TRemoveUpdateRequest& /*typedRequest*/) {
                    schema->RunPreloader(object);
                },
                [&] (const TMethodRequest& /*typedRequest*/) {
                    schema->RunPreloader(object);
                });

        };

    Schema_->ValueSetter_ =
        [parseOne, &descriptor] (
            TTransaction* transaction,
            TObject* this_,
            const NYPath::TYPath& path,
            const NYTree::INodePtr& value,
            bool /*recursive*/,
            std::optional<bool> sharedWrite,
            EAggregateMode aggregateMode,
            const TTransactionCallContext& /*transactionCallContext*/)
        {
            THROW_ERROR_EXCEPTION_UNLESS(path.empty(),
                "Partial updates are not supported");
            if (sharedWrite && *sharedWrite || aggregateMode != EAggregateMode::Unspecified) {
                THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments,
                    "One to One attribute cannot be updated with shared write lock / aggregate mode")
                    << TErrorAttribute("this",
                        NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(TThis::Type))
                    << TErrorAttribute("that",
                        NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(TThat::Type));
            }

            auto* typedThis = this_->As<TThis>();
            auto* forwardAttribute = descriptor.ForwardAttributeGetter(typedThis);
            if (auto* typedThat = parseOne(transaction, value)) {
                typedThat->ValidateExists();
                forwardAttribute->Store(typedThat);
            } else {
                THROW_ERROR_EXCEPTION_UNLESS(descriptor.Nullable,
                    "Cannot set null %v",
                    NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(TThat::Type));
                forwardAttribute->Store(nullptr);
            }
        };

    Schema_->Locker_ =
        [&descriptor] (
            TTransaction* /*transaction*/,
            TObject* this_,
            const NYPath::TYPath& path,
            NTableClient::ELockType lockType)
        {
            THROW_ERROR_EXCEPTION_UNLESS(path.empty(),
                "Partial locks are not supported");

            auto* typedThis = this_->As<TThis>();
            auto* forwardAttribute = descriptor.ForwardAttributeGetter(typedThis);
            forwardAttribute->Lock(lockType);
        };

    Schema_->Remover_ =
        [&descriptor] (
            TTransaction* /*transaction*/,
            TObject* this_,
            const NYPath::TYPath& path,
            bool /*force*/)
        {
            THROW_ERROR_EXCEPTION_UNLESS(path.empty(),
                "Partial removes are not supported");

            THROW_ERROR_EXCEPTION_UNLESS(descriptor.Nullable,
                "Cannot set null %v",
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(TThat::Type));

            auto* typedThis = this_->As<TThis>();
            auto* forwardAttribute = descriptor.ForwardAttributeGetter(typedThis);
            forwardAttribute->Store(nullptr);
        };

    Schema_->TimestampPregetter_ =
        [&descriptor] (const TObject* this_, const NYPath::TYPath& /*path*/)
        {
            const auto* typedThis = this_->As<TThis>();
            auto* forwardAttribute = descriptor.ForwardAttributeGetter(typedThis);
            forwardAttribute->ScheduleLoadTimestamp();
        };

    Schema_->TimestampGetter_ =
        [&descriptor] (const TObject* this_, const NYPath::TYPath& /*path*/)
        {
            const auto* typedThis = this_->As<TThis>();
            auto* forwardAttribute = descriptor.ForwardAttributeGetter(typedThis);
            return forwardAttribute->LoadTimestamp();
        };

    InitTimestampExpressionBuilder(descriptor);

    Schema_->SetValueGetter<TThis>(
        [&descriptor] (
            TTransaction* /*transaction*/,
            const TThis* object,
            NYson::IYsonConsumer* consumer)
        {
            auto* forwardAttribute = descriptor.ForwardAttributeGetter(object);
            auto* attributeValue = forwardAttribute->Load();
            if (attributeValue) {
                NDetail::YsonConsumeValue(attributeValue->GetId(), consumer);
            } else {
                const auto& configsSnapshot = object->GetSession()->GetConfigsSnapshot();
                if (configsSnapshot.TransactionManager->AnyToOneAttributeValueGetterReturnsNull) {
                    consumer->OnEntity();
                } else {
                    NDetail::YsonConsumeValue(TThatKeyFirstField{}, consumer);
                }
            }
        });

    Schema_->StoreScheduledGetter_  =
        [&descriptor] (const TObject* this_) {
            const auto* typedThis = this_->As<TThis>();
            auto* forwardAttribute = descriptor.ForwardAttributeGetter(typedThis);
            return forwardAttribute->IsStoreScheduled();
        };

    Schema_->ChangedGetterType_ = EChangedGetterType::Default;
    Schema_->ChangedGetter_ =
        [&descriptor] (const TObject* this_, const NYPath::TYPath& path) {
            THROW_ERROR_EXCEPTION_UNLESS(path.empty(),
                "Cannot collect changes at the non-empty path %v: one-to-one attribute is not composite",
                path);
            const auto* typedThis = this_->As<TThis>();
            auto* forwardAttribute = descriptor.ForwardAttributeGetter(typedThis);
            return forwardAttribute->IsChanged();
        };

    Schema_->SetExpressionBuilder(
        ValidatePathIsEmpty,
        MakeFieldExpressionGetter(descriptor.Field));

    return Schema_;
}

template <class TOwner, class TForeign>
TScalarAttributeSchema* TScalarAttributeSchemaBuilder::SetAttribute(
    const TOneTransitiveAttributeDescriptor<TOwner, TForeign>& descriptor)
{
    using TForeignKeyFirstField = std::tuple_element_t<0, typename TObjectKeyTraits<TForeign>::TTypes>;
    InitDefaultValueGetter<TForeignKeyFirstField>();
    Schema_->SetConstantChangedGetter(false);

    Schema_->TimestampPregetter_ =
        [&descriptor] (const TObject* owner, const NYPath::TYPath& /*path*/)
        {
            auto* typedOwner = owner->As<TOwner>();
            descriptor.ForwardAttributeGetter(typedOwner)->ScheduleLoadTimestamp();
        };

    Schema_->TimestampGetter_ =
        [&descriptor] (const TObject* owner, const NYPath::TYPath& /*path*/)
        {
            auto* typedOwner = owner->As<TOwner>();
            return descriptor.ForwardAttributeGetter(typedOwner)->LoadTimestamp();
        };

    InitTimestampExpressionBuilder(descriptor);

    Schema_->Preloader_ =
        [&descriptor] (const TObject* owner, const NYPath::TYPath& /*path*/) {
            auto* typedOwner = owner->As<TOwner>();
            descriptor.ForwardAttributeGetter(typedOwner)->ScheduleLoad();
        };

    Schema_->UpdatePreloader_ =
        [&descriptor] (TTransaction* /*transaction*/, const TObject* owner, const TUpdateRequest& /*request*/)
        {
            auto* typedOwner = owner->As<TOwner>();
            descriptor.ForwardAttributeGetter(typedOwner)->ScheduleLoad();
        };

    Schema_->SetTypedValueSetter<TOwner, TForeignKeyFirstField>(
        [&descriptor, schema = Schema_] (
            TTransaction* /*transaction*/,
            TOwner* owner,
            const NYPath::TYPath& path,
            const TForeignKeyFirstField& value,
            bool /*recursive*/,
            std::optional<bool> sharedWrite,
            EAggregateMode aggregateMode,
            const TTransactionCallContext& /*transactionCallContext*/)
        {
            THROW_ERROR_EXCEPTION_UNLESS(path.empty(), "Partial updates are not supported");
            if (sharedWrite && *sharedWrite || aggregateMode != EAggregateMode::Unspecified) {
                THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments,
                    "Transitive attribute cannot be updated with shared write lock / aggregate mode")
                    << TErrorAttribute("owner",
                        NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(TOwner::Type))
                    << TErrorAttribute("foreign",
                        NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(TForeign::Type));
            }

            if (owner->GetState() == EObjectState::Creating) {
                owner->GetSession()->FlushLoads();
            }

            auto key = descriptor.ForwardAttributeGetter(owner)->LoadKey();
            YT_VERIFY(key.size() == 1);
            std::visit([&, schema] (auto& typedKey) {
                if constexpr (std::is_same_v<std::decay_t<decltype(typedKey)>, TForeignKeyFirstField>) {
                    THROW_ERROR_EXCEPTION_UNLESS(value == typedKey,
                        "Cannot manually change value of transitive key attribute %Qv from %v to %v",
                        schema->GetPath(),
                        typedKey,
                        value);
                } else {
                    YT_ABORT();
                }
            }, key[0]);
        });

    Schema_->SetValueGetter<TOwner>(
        [&descriptor] (TTransaction* /*transaction*/, const TOwner* owner, NYson::IYsonConsumer* consumer) {
            auto key = descriptor.ForwardAttributeGetter(owner)->LoadKey();
            YT_VERIFY(key.size() == 1);
            if (!key[0].IsZero()) {
                NDetail::YsonConsumeValue(key[0], consumer);
            } else {
                if (owner->GetSession()->GetConfigsSnapshot().TransactionManager->AnyToOneAttributeValueGetterReturnsNull) {
                    consumer->OnEntity();
                } else {
                    NDetail::YsonConsumeValue(TForeignKeyFirstField{}, consumer);
                }
            }
        });

    Schema_->SetExpressionBuilder(
        ValidatePathIsEmpty,
        [&descriptor] (IQueryContext* context, const NYPath::TYPath& path, EAttributeExpressionContext expressionContext) {
            return context->GetScalarAttributeExpression(descriptor.Field, path, expressionContext, descriptor.Field->Type);
        });

    return Schema_;
}

template <class TOwner, class TForeign>
TScalarAttributeSchema* TScalarAttributeSchemaBuilder::SetAttribute(
    const TManyToManyInlineAttributeDescriptor<TOwner, TForeign>& descriptor)
{
    using TForeignKeyFirstField = std::tuple_element_t<0, typename TObjectKeyTraits<TForeign>::TTypes>;
    InitDefaultValueGetter<std::vector<TForeignKeyFirstField>>();
    InitListContainsExpressionBuilder(descriptor);

    auto parseForeigns = [] (TTransaction* transaction, const NYTree::INodePtr& value)
    {
        std::vector<TForeignKeyFirstField> foreignKeyFirstFields;
        try {
            if (value->GetType() != NYTree::ENodeType::Entity) {
                foreignKeyFirstFields = NYTree::ConvertTo<std::vector<TForeignKeyFirstField>>(value);
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION(
                NClient::EErrorCode::InvalidObjectId,
                "Error parsing list of %v ids",
                NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(TForeign::Type))
                << ex;
        }
        std::vector<TForeign*> result;
        result.reserve(foreignKeyFirstFields.size());
        for (const auto& foreignKeyFirstField : foreignKeyFirstFields) {
            result.push_back(transaction->GetTypedObject<TForeign>(TObjectKey(foreignKeyFirstField)));
        }
        return result;
    };

    Schema_->AttributeDescriptor_ = &descriptor;

    Schema_->AttributeGetter_ =
        [&descriptor] (const TObject* owner) -> const TAttributeBase* {
            return descriptor.ForwardAttributeGetter(owner->As<TOwner>());
        };

    Schema_->Preloader_ =
        [&descriptor] (const TObject* owner, const NYPath::TYPath&) {
            descriptor.ForwardAttributeGetter(owner->As<TOwner>())->ScheduleLoad();
        };

    Schema_->UpdatePreloader_ =
        [parseForeigns, schema = Schema_] (
            TTransaction* transaction,
            const TObject* owner,
            const TUpdateRequest& request)
        {
            Visit(request,
                [&] (const TSetUpdateRequest& typedRequest) {
                    THROW_ERROR_EXCEPTION_UNLESS(typedRequest.Path.empty(),
                        "Partial updates are not supported");
                    auto foreigns = parseForeigns(transaction, typedRequest.Value);
                    Y_UNUSED(foreigns);
                    schema->RunPreloader(owner);
                },
                [&] (const TRemoveUpdateRequest& /*typedRequest*/) {
                    schema->RunPreloader(owner);
                },
                [&] (const TLockUpdateRequest& /*typedRequest*/) {
                },
                [&] (const TMethodRequest& /*typedRequest*/) {
                    schema->RunPreloader(owner);
                });
        };

    Schema_->ValueSetter_ =
        [parseForeigns, &descriptor] (
            TTransaction* transaction,
            TObject* owner,
            const NYPath::TYPath& path,
            const NYTree::INodePtr& value,
            bool /*recursive*/,
            std::optional<bool> sharedWrite,
            EAggregateMode aggregateMode,
            const TTransactionCallContext& /*transactionCallContext*/)
        {
            THROW_ERROR_EXCEPTION_UNLESS(path.empty(),
                "Partial updates are not supported");
            if (sharedWrite && *sharedWrite || aggregateMode != EAggregateMode::Unspecified) {
                THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments,
                    "Many to Many inline attribute cannot be updated with shared write lock / aggregate mode")
                    << TErrorAttribute("owner",
                        NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(TOwner::Type))
                    << TErrorAttribute("foreign",
                        NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(TForeign::Type));
            }

            auto* typedOwner = owner->As<TOwner>();
            auto* forwardAttribute = descriptor.ForwardAttributeGetter(typedOwner);

            forwardAttribute->Store(parseForeigns(transaction, value));
        };

    Schema_->Remover_ =
        [&descriptor] (
            TTransaction* /*transaction*/,
            TObject* owner,
            const NYPath::TYPath& path,
            bool /*force*/)
        {
            THROW_ERROR_EXCEPTION_UNLESS(path.empty(),
                "Partial removes are not supported");

            auto* typedOwner = owner->As<TOwner>();
            auto* forwardAttribute = descriptor.ForwardAttributeGetter(typedOwner);
            forwardAttribute->Store({});
        };

    Schema_->Locker_ =
        [&descriptor] (
            TTransaction* /*transaction*/,
            TObject* this_,
            const NYPath::TYPath& path,
            NTableClient::ELockType lockType)
        {
            THROW_ERROR_EXCEPTION_UNLESS(path.empty(),
                "Partial locks are not supported");

            auto* typedThis = this_->As<TOwner>();
            auto* forwardAttribute = descriptor.ForwardAttributeGetter(typedThis);
            forwardAttribute->Lock(lockType);
        };

    Schema_->TimestampPregetter_ =
        [&descriptor] (const TObject* owner, const NYPath::TYPath& /*path*/)
        {
            const auto* typedOwner = owner->As<TOwner>();
            auto* forwardAttribute = descriptor.ForwardAttributeGetter(typedOwner);
            forwardAttribute->ScheduleLoadTimestamp();
        };

    Schema_->TimestampGetter_ =
        [&descriptor] (const TObject* owner, const NYPath::TYPath& /*path*/)
        {
            const auto* typedOwner = owner->As<TOwner>();
            auto* forwardAttribute = descriptor.ForwardAttributeGetter(typedOwner);
            return forwardAttribute->LoadTimestamp();
        };

    InitTimestampExpressionBuilder(descriptor);

    Schema_->SetValueGetter<TOwner>(
        [&descriptor] (
            TTransaction* /*transaction*/,
            const TOwner* object,
            NYson::IYsonConsumer* consumer)
        {
            auto* forwardAttribute = descriptor.ForwardAttributeGetter(object);
            auto keys = forwardAttribute->LoadKeys();
            return NYTree::BuildYsonFluently(consumer)
                .DoListFor(keys, [&] (auto list, const auto& key) {
                    YT_VERIFY(key.size() == 1);
                    YT_VERIFY(std::holds_alternative<TForeignKeyFirstField>(key[0]));
                    list.Item().Value(std::get<TForeignKeyFirstField>(key[0]));
                });
        });

    Schema_->StoreScheduledGetter_  =
        [&descriptor] (const TObject* owner) {
            const auto* typedOwner = owner->As<TOwner>();
            auto* forwardAttribute = descriptor.ForwardAttributeGetter(typedOwner);
            return forwardAttribute->IsStoreScheduled();
        };

    Schema_->ChangedGetterType_ = EChangedGetterType::Default;
    Schema_->ChangedGetter_ =
        [&descriptor] (const TObject* owner, const NYPath::TYPath& path) {
            THROW_ERROR_EXCEPTION_UNLESS(path.empty(),
                "Cannot collect changes at the non-empty path %v: many-to-many attribute is not composite",
                path);
            const auto* typedOwner = owner->As<TOwner>();
            auto* forwardAttribute = descriptor.ForwardAttributeGetter(typedOwner);
            return forwardAttribute->IsChanged();
        };

    Schema_->SetExpressionBuilder(
        ValidatePathIsEmpty,
        [&descriptor] (IQueryContext* context, const NYPath::TYPath& path, EAttributeExpressionContext expressionContext) {
            return context->GetScalarAttributeExpression(descriptor.Field, path, expressionContext, descriptor.Field->Type);
        });

    Schema_->Field_ = descriptor.Field;
    return Schema_;
}

template <class TOwner, class TForeign>
TScalarAttributeSchema* TScalarAttributeSchemaBuilder::SetAttribute(
    const TManyToManyTabularAttributeDescriptor<TOwner, TForeign>& descriptor)
{
    Schema_->SetOpaque();

    Schema_->AttributeGetter_ =
        [&descriptor] (const TObject* owner) -> const TAttributeBase* {
            return descriptor.ForwardAttributeGetter(owner->As<TOwner>());
        };

    Schema_->SetValueGetter<TOwner>(
        [&descriptor] (
            TTransaction* /*transaction*/,
            const TOwner* object,
            NYson::IYsonConsumer* consumer)
        {
            auto* forwardAttribute = descriptor.ForwardAttributeGetter(object);
            auto foreigns = forwardAttribute->Load();
            return NYTree::BuildYsonFluently(consumer)
                .DoListFor(foreigns, [] (auto list, auto* foreign) {
                    list.Item().Value(foreign->GetId());
                });
        });

    return Schema_;
}

template <class TMany, class TOne>
TScalarAttributeSchema* TScalarAttributeSchemaBuilder::SetAttribute(
    const TManyToOneViewAttributeDescriptor<TMany, TOne>& descriptor)
{
    Schema_->SetOpaque();
    Schema_->SetView();
    Schema_->SetConstantChangedGetter(false);
    Schema_->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly);
    Schema_->ViewObjectType_ = TOne::Type;

    Schema_->DefaultValueGetter_ =
        [] {
            return NYTree::GetEphemeralNodeFactory()->CreateEntity();
        };

    Schema_->Preloader_ =
        [&descriptor, schema = Schema_] (const TObject* many, const NYPath::TYPath& path) {
            const auto* typedMany = many->As<TMany>();
            auto* forwardAttribute = descriptor.ReferenceDescriptor.ForwardAttributeGetter(typedMany);
            forwardAttribute->ScheduleLoad();

            auto viewResolveResult = descriptor.ViewAttributeGetter(typedMany)->ResolveViewAttribute(schema, path);
            auto* viewAttribute = viewResolveResult.Attribute;
            many->GetSession()->ScheduleLoad(
                [forwardAttribute, viewAttribute, viewPath = std::move(viewResolveResult.SuffixPath)] (ILoadContext*) {
                    auto* one = forwardAttribute->Load();
                    if (!one) {
                        return;
                    }
                    viewAttribute->ForEachVisibleLeafAttribute(
                        [&] (const TAttributeSchema* schema) {
                            if (schema->HasPreloader()) {
                                schema->RunPreloader(one, viewPath);
                            }
                        });
                },
                ELoadPriority::View);
        };

    Schema_->ValueGetterType_ = EValueGetterType::Evaluator;
    Schema_->ValueGetter_ =
        [&descriptor, schema = Schema_] (
            TTransaction* transaction,
            const TObject* many,
            NYson::IYsonConsumer* consumer,
            const NYPath::TYPath& path)
        {
            auto* typedMany = many->As<TMany>();
            auto* forwardAttribute = descriptor.ReferenceDescriptor.ForwardAttributeGetter(typedMany);
            auto* typedOne = forwardAttribute->Load();
            if (!typedOne) {
                consumer->OnEntity();
                return;
            }

            auto viewResolveResult = descriptor.ViewAttributeGetter(typedMany)->ResolveViewAttribute(schema, path);
            NDetail::EvaluateViewAttribute(
                viewResolveResult.Attribute,
                transaction,
                typedOne,
                consumer,
                viewResolveResult.SuffixPath);
        };

    Schema_->TimestampPregetter_ =
        [&descriptor, schema = Schema_] (const TObject* many, const NYPath::TYPath& path)
        {
            const auto* typedMany = many->As<TMany>();
            auto* forwardAttribute = descriptor.ReferenceDescriptor.ForwardAttributeGetter(typedMany);
            forwardAttribute->ScheduleLoad();

            auto viewResolveResult = descriptor.ViewAttributeGetter(typedMany)->ResolveViewAttribute(schema, path);
            many->GetSession()->ScheduleLoad(
                [forwardAttribute, viewResolveResult = std::move(viewResolveResult)] (ILoadContext*) {
                    auto* typedOne = forwardAttribute->Load();
                    if (!typedOne) {
                        return;
                    }
                    PrefetchAttributeTimestamp(viewResolveResult, typedOne);
                },
                ELoadPriority::View);
        };

    Schema_->TimestampGetter_ =
        [&descriptor, schema = Schema_] (const TObject* many, const NYPath::TYPath& path)
        {
            const auto* typedMany = many->As<TMany>();
            auto* forwardAttribute = descriptor.ReferenceDescriptor.ForwardAttributeGetter(typedMany);
            auto* typedOne = forwardAttribute->Load();

            auto result = NullTimestamp;
            if (!typedOne) {
                return result;
            }

            auto viewResolveResult = descriptor.ViewAttributeGetter(typedMany)->ResolveViewAttribute(schema, path);
            return FetchAttributeTimestamp(viewResolveResult, typedOne);
        };

    return Schema_;
}

template <class TOwner, class TForeign>
TScalarAttributeSchema* TScalarAttributeSchemaBuilder::SetAttribute(
    const TManyToManyInlineViewAttributeDescriptor<TOwner, TForeign>& descriptor)
{
    Schema_->SetOpaque();
    Schema_->SetView();
    Schema_->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly);
    Schema_->SetConstantChangedGetter(false);
    Schema_->ViewObjectType_ = TForeign::Type;

    Schema_->DefaultValueGetter_ =
        [] {
            return NYTree::GetEphemeralNodeFactory()->CreateEntity();
        };

    Schema_->Preloader_ =
        [&descriptor, schema = Schema_] (const TObject* owner, const NYPath::TYPath& path) {
            const auto* typedOwner = owner->As<TOwner>();
            auto* forwardAttribute = descriptor.ReferenceDescriptor.ForwardAttributeGetter(typedOwner);
            forwardAttribute->ScheduleLoad();

            auto viewResolveResult = descriptor.ViewAttributeGetter(typedOwner)->ResolveViewAttribute(schema, path);
            auto* viewAttribute = viewResolveResult.Attribute;
            typedOwner->GetSession()->ScheduleLoad(
                [forwardAttribute, viewAttribute, viewPath = std::move(viewResolveResult.SuffixPath)] (ILoadContext*) {
                    auto foreigns = forwardAttribute->Load();
                    if (foreigns.empty()) {
                        return;
                    }

                    viewAttribute->ForEachVisibleLeafAttribute(
                        [&] (const TAttributeSchema* schema) {
                            if (schema->HasPreloader()) {
                                for (auto* foreign : foreigns) {
                                    schema->RunPreloader(foreign, viewPath);
                                }
                            }
                        });
                },
                ELoadPriority::View);
        };

    Schema_->ValueGetterType_ = EValueGetterType::Evaluator;
    Schema_->ValueGetter_ =
        [&descriptor, schema = Schema_] (
            TTransaction* transaction,
            const TObject* many,
            NYson::IYsonConsumer* consumer,
            const NYPath::TYPath& path)
        {
            const auto* typedOwner = many->As<TOwner>();
            auto* forwardAttribute = descriptor.ReferenceDescriptor.ForwardAttributeGetter(typedOwner);
            auto foreigns = forwardAttribute->Load();

            auto viewResolveResult = descriptor.ViewAttributeGetter(typedOwner)->ResolveViewAttribute(schema, path);
            NYTree::BuildYsonFluently(consumer)
                .BeginList()
                .DoFor(foreigns.begin(), foreigns.end(), [&] (NYTree::TFluentList list, auto** foreign) {
                    NDetail::EvaluateViewAttribute(
                        viewResolveResult.Attribute,
                        transaction,
                        *foreign,
                        list.Item().GetConsumer(),
                        viewResolveResult.SuffixPath);
                })
                .EndList();
        };

    Schema_->TimestampPregetter_ =
        [&descriptor, schema = Schema_] (const TObject* owner, const NYPath::TYPath& path)
        {
            const auto* typedOwner = owner->As<TOwner>();
            auto* forwardAttribute = descriptor.ReferenceDescriptor.ForwardAttributeGetter(typedOwner);
            forwardAttribute->ScheduleLoad();

            auto viewResolveResult = descriptor.ViewAttributeGetter(typedOwner)->ResolveViewAttribute(schema, path);
            typedOwner->GetSession()->ScheduleLoad(
                [forwardAttribute, viewResolveResult = std::move(viewResolveResult)] (ILoadContext*) {
                    auto foreigns = forwardAttribute->Load();
                    for (auto* foreign : foreigns) {
                        PrefetchAttributeTimestamp(viewResolveResult, foreign);
                    }
                },
                ELoadPriority::View);
        };

    Schema_->TimestampGetter_ =
        [&descriptor, schema = Schema_] (const TObject* owner, const NYPath::TYPath& path)
        {
            const auto* typedOwner = owner->As<TOwner>();
            auto* forwardAttribute = descriptor.ReferenceDescriptor.ForwardAttributeGetter(typedOwner);
            auto foreigns = forwardAttribute->Load();

            auto result = NullTimestamp;

            auto viewResolveResult = descriptor.ViewAttributeGetter(typedOwner)->ResolveViewAttribute(schema, path);
            for (auto* foreign : foreigns) {
                auto ts = FetchAttributeTimestamp(viewResolveResult, foreign);
                result = std::max(result, ts);
            }

            return result;
        };

    return Schema_;
}

template <class TOwner, class TForeign>
TScalarAttributeSchema* TScalarAttributeSchemaBuilder::SetAttribute(
    const TSingleViewDescriptor<TOwner, TForeign>& descriptor)
{
    SetViewCommon<TOwner, TForeign>();

    Schema_->Preloader_ =
        [&descriptor, schema = Schema_] (const TObject* owner, const NYPath::TYPath& path) {
            auto* typedOwner = const_cast<TOwner*>(owner->As<TOwner>());
            auto* forwardAttribute = descriptor.ReferenceDescriptor.ForwardAttributeGetter(typedOwner);
            forwardAttribute->ScheduleLoad();
            auto viewResolveResult = descriptor.ViewAttributeGetter(typedOwner)
                ->ResolveViewAttribute(schema, path, /*many*/ false);

            typedOwner->GetSession()->ScheduleLoad(
                [forwardAttribute, viewResolveResult = std::move(viewResolveResult)] (ILoadContext*) {
                    auto foreign = forwardAttribute->Load();
                    if (!foreign) {
                        return;
                    }

                    viewResolveResult.Attribute->ForEachVisibleLeafAttribute(
                        [&] (const TAttributeSchema* schema) {
                            if (schema->HasPreloader()) {
                                schema->RunPreloader(foreign, viewResolveResult.SuffixPath);
                            }
                        });
                },
                ELoadPriority::View);
        };

    Schema_->ValueGetterType_ = EValueGetterType::Evaluator;
    Schema_->ValueGetter_ =
        [&descriptor, schema = Schema_] (
            TTransaction* transaction,
            const TObject* owner,
            NYson::IYsonConsumer* consumer,
            const NYPath::TYPath& path)
        {
            auto* typedOwner = const_cast<TOwner*>(owner->As<TOwner>());
            auto* forwardAttribute = descriptor.ReferenceDescriptor.ForwardAttributeGetter(typedOwner);
            auto foreign = forwardAttribute->Load();
            if (!foreign) {
                consumer->OnEntity();
                return;
            }
            auto viewResolveResult = descriptor.ViewAttributeGetter(typedOwner)
                ->ResolveViewAttribute(schema, path, /*many*/ false);

            NDetail::EvaluateViewAttribute(
                viewResolveResult.Attribute,
                transaction,
                foreign,
                consumer,
                viewResolveResult.SuffixPath);
        };

    Schema_->TimestampPregetter_ =
        [&descriptor, schema = Schema_] (const TObject* owner, const NYPath::TYPath& path) {
            auto* typedOwner = const_cast<TOwner*>(owner->As<TOwner>());
            auto* forwardAttribute = descriptor.ReferenceDescriptor.ForwardAttributeGetter(typedOwner);
            forwardAttribute->ScheduleLoad();
            auto viewResolveResult = descriptor.ViewAttributeGetter(typedOwner)
                ->ResolveViewAttribute(schema, path, /*many*/ false);

            typedOwner->GetSession()->ScheduleLoad(
                [forwardAttribute, viewResolveResult = std::move(viewResolveResult)] (ILoadContext*) {
                    auto foreign = forwardAttribute->Load();
                    if (!foreign) {
                        return;
                    }
                    PrefetchAttributeTimestamp(viewResolveResult, foreign);
                },
                ELoadPriority::View);
        };

    Schema_->TimestampGetter_ =
        [&descriptor, schema = Schema_] (const TObject* owner, const NYPath::TYPath& path) {
            auto* typedOwner = const_cast<TOwner*>(owner->As<TOwner>());
            auto* forwardAttribute = descriptor.ReferenceDescriptor.ForwardAttributeGetter(typedOwner);
            auto foreign = forwardAttribute->Load();
            if (!foreign) {
                return NullTimestamp;
            }
            auto viewResolveResult = descriptor.ViewAttributeGetter(typedOwner)
                ->ResolveViewAttribute(schema, path, /*many*/ false);

            return FetchAttributeTimestamp(viewResolveResult, foreign);
        };

    return Schema_;
}

template <class TOwner, class TForeign>
TScalarAttributeSchema* TScalarAttributeSchemaBuilder::SetAttribute(
    const TMultiViewDescriptor<TOwner, TForeign>& descriptor)
{
    SetViewCommon<TOwner, TForeign>();

    Schema_->Preloader_ =
        [&descriptor, schema = Schema_] (const TObject* owner, const NYPath::TYPath& path) {
            auto* typedOwner = const_cast<TOwner*>(owner->As<TOwner>());
            auto* forwardAttribute = descriptor.ReferenceDescriptor.ForwardAttributeGetter(typedOwner);
            forwardAttribute->ScheduleLoad();
            auto viewResolveResult = descriptor.ViewAttributeGetter(typedOwner)
                ->ResolveViewAttribute(schema, path, /*many*/ true);

            typedOwner->GetSession()->ScheduleLoad(
                [forwardAttribute, viewResolveResult = std::move(viewResolveResult)] (ILoadContext*) {
                    auto foreigns = forwardAttribute->Load();
                    if (foreigns.empty()) {
                        return;
                    }

                    viewResolveResult.Attribute->ForEachVisibleLeafAttribute(
                        [&] (const TAttributeSchema* schema) {
                            if (schema->HasPreloader()) {
                                for (auto* foreign : foreigns) {
                                    schema->RunPreloader(foreign, viewResolveResult.SuffixPath);
                                }
                            }
                        });
                },
                ELoadPriority::View);
        };

    Schema_->ValueGetterType_ = EValueGetterType::Evaluator;
    Schema_->ValueGetter_ =
        [&descriptor, schema = Schema_] (
            TTransaction* transaction,
            const TObject* owner,
            NYson::IYsonConsumer* consumer,
            const NYPath::TYPath& path)
        {
            auto* typedOwner = const_cast<TOwner*>(owner->As<TOwner>());
            auto* forwardAttribute = descriptor.ReferenceDescriptor.ForwardAttributeGetter(typedOwner);
            auto foreigns = forwardAttribute->Load();
            auto viewResolveResult = descriptor.ViewAttributeGetter(typedOwner)
                ->ResolveViewAttribute(schema, path, /*many*/ true);

            NYTree::BuildYsonFluently(consumer)
                .BeginList()
                .DoFor(foreigns.begin(), foreigns.end(), [&] (NYTree::TFluentList list, auto** foreign) {
                    NDetail::EvaluateViewAttribute(
                        viewResolveResult.Attribute,
                        transaction,
                        *foreign,
                        list.Item().GetConsumer(),
                        viewResolveResult.SuffixPath);
                })
                .EndList();
        };

    Schema_->TimestampPregetter_ =
        [&descriptor, schema = Schema_] (const TObject* owner, const NYPath::TYPath& path) {
            auto* typedOwner = const_cast<TOwner*>(owner->As<TOwner>());
            auto* forwardAttribute = descriptor.ReferenceDescriptor.ForwardAttributeGetter(typedOwner);
            forwardAttribute->ScheduleLoad();
            auto viewResolveResult = descriptor.ViewAttributeGetter(typedOwner)
                ->ResolveViewAttribute(schema, path, /*many*/ true);

            typedOwner->GetSession()->ScheduleLoad(
                [forwardAttribute, viewResolveResult = std::move(viewResolveResult)] (ILoadContext*) {
                    auto foreigns = forwardAttribute->Load();
                    for (auto* foreign : foreigns) {
                        PrefetchAttributeTimestamp(viewResolveResult, foreign);
                    }
                },
                ELoadPriority::View);
        };

    Schema_->TimestampGetter_ =
        [&descriptor, schema = Schema_] (const TObject* owner, const NYPath::TYPath& path) {
            auto* typedOwner = const_cast<TOwner*>(owner->As<TOwner>());
            auto* forwardAttribute = descriptor.ReferenceDescriptor.ForwardAttributeGetter(typedOwner);
            auto foreigns = forwardAttribute->Load();
            auto viewResolveResult = descriptor.ViewAttributeGetter(typedOwner)
                ->ResolveViewAttribute(schema, path, /*many*/ true);

            auto result = NullTimestamp;
            for (auto* foreign : foreigns) {
                auto ts = FetchAttributeTimestamp(viewResolveResult, foreign);
                result = std::max(result, ts);
            }

            return result;
        };

    return Schema_;
}

template <class TOwner, class TForeign>
TScalarAttributeSchema* TScalarAttributeSchemaBuilder::SetAttribute(
    const TSingleReferenceDescriptor<TOwner, TForeign>& descriptor)
{
    SetReferenceCommon(descriptor);

    auto* foreignTypeHandler = Schema_->ObjectManager_->GetTypeHandlerOrThrow(TForeign::Type);
    SetNullKey(descriptor.KeyStorageDescriptor, foreignTypeHandler->GetNullKey());

    return Schema_;
}

template <class TOwner, class TForeign>
TScalarAttributeSchema* TScalarAttributeSchemaBuilder::SetAttribute(
    const TMultiReferenceDescriptor<TOwner, TForeign>& descriptor)
{
    SetReferenceCommon(descriptor);
    return Schema_;
}

template <class TTypedObject, class TTypedValue>
TScalarAttributeSchema* TScalarAttributeSchemaBuilder::SetProtobufAttribute(
    const TScalarAttributeDescriptor<TTypedObject, TString>& descriptor)
{
    InitTimestampGetter<TTypedObject, TString>(descriptor);
    InitTimestampExpressionBuilder(descriptor);
    InitStoreScheduledGetter<TTypedObject, TTypedValue>(descriptor);
    InitDefaultValueGetter<TTypedValue>();

    Schema_->AttributeDescriptor_ = &descriptor;

    Schema_->AttributeGetter_ =
        [&descriptor] (const TObject* object) -> const TAttributeBase* {
            const auto* typedObject = object->As<TTypedObject>();
            return descriptor.AttributeGetter(typedObject);
        };

    Schema_->SetPreloader<TTypedObject>([&descriptor] (const TTypedObject* object) {
        auto* attribute = descriptor.AttributeGetter(object);
        attribute->ScheduleLoad();
    });

    Schema_->SetValueGetter<TTypedObject>([&descriptor] (
        TTransaction* /*transaction*/,
        const TTypedObject* object,
        NYson::IYsonConsumer* consumer)
    {
        auto* attribute = descriptor.AttributeGetter(object);
        NDetail::ParseProtobufPayload<TTypedValue>(attribute->Load(), consumer);
    });

    Schema_->ChangedGetterType_ = EChangedGetterType::Default;
    Schema_->ChangedGetter_ =
        [&descriptor] (const TObject* object, const NYPath::TYPath& path) {
            const auto* typedObject = object->As<TTypedObject>();
            auto* attribute = descriptor.AttributeGetter(typedObject);
            auto payloadChanged = attribute->IsChanged();
            if (path.empty()) {
                return payloadChanged;
            }
            if (!payloadChanged) {
                return false;
            }
            NYTree::INodePtr oldNode;
            {
                auto treeBuilder = NYTree::CreateBuilderFromFactory(NYTree::GetEphemeralNodeFactory());
                NDetail::ParseProtobufPayload<TTypedValue>(attribute->LoadOld(), treeBuilder.get());
                oldNode = treeBuilder->EndTree();
            }
            NYTree::INodePtr newNode;
            {
                auto treeBuilder = NYTree::CreateBuilderFromFactory(NYTree::GetEphemeralNodeFactory());
                NDetail::ParseProtobufPayload<TTypedValue>(attribute->Load(), treeBuilder.get());
                newNode = treeBuilder->EndTree();
            }
            return !NYTree::AreNodesEqual(
                NAttributes::GetNodeByPathOrEntity(oldNode, path),
                NAttributes::GetNodeByPathOrEntity(newNode, path));
        };

    Schema_->ValueSetter_ =
        [&descriptor, schema = Schema_] (
            TTransaction* transaction,
            TObject* object,
            const NYPath::TYPath& path,
            const NYTree::INodePtr& value,
            bool recursive,
            std::optional<bool> sharedWrite,
            EAggregateMode aggregateMode,
            const TTransactionCallContext& /*transactionCallContext*/)
        {
            THROW_ERROR_EXCEPTION_IF(aggregateMode != EAggregateMode::Unspecified,
                NClient::EErrorCode::InvalidRequestArguments,
                "Protobuf attribute cannot be updated with aggregate mode");
            auto* typedObject = object->As<TTypedObject>();
            auto* attribute = descriptor.AttributeGetter(typedObject);
            TString protobuf;
            NYson::TProtobufWriterOptions options;
            options.UnknownYsonFieldModeResolver = schema->BuildUnknownYsonFieldModeResolver();
            if (path.empty()) {
                google::protobuf::io::StringOutputStream outputStream(&protobuf);
                auto protobufWriter = NYson::CreateProtobufWriter(
                    &outputStream,
                    NYson::ReflectProtobufMessageType<TTypedValue>(),
                    options);
                NYTree::VisitTree(value, protobufWriter.get(), true);
            } else {
                auto treeBuilder = NYTree::CreateBuilderFromFactory(NYTree::GetEphemeralNodeFactory());
                NDetail::ParseProtobufPayload<TTypedValue>(attribute->Load(), treeBuilder.get());

                auto node = treeBuilder->EndTree();
                NYTree::SyncYPathSet(node, path, NYson::ConvertToYsonString(value), recursive);

                google::protobuf::io::StringOutputStream outputStream(&protobuf);
                auto protobufWriter = NYson::CreateProtobufWriter(
                    &outputStream,
                    NYson::ReflectProtobufMessageType<TTypedValue>(),
                    options);
                NYTree::VisitTree(node, protobufWriter.get(), true);
            }
            if (auto& initializer = descriptor.GetInitializer();
                object->GetState() == EObjectState::Creating && initializer)
            {
                initializer(transaction, typedObject, &protobuf);
            }
            descriptor.RunValidators(typedObject, attribute->Load(), &protobuf);
            attribute->Store(std::move(protobuf), sharedWrite);
        };

    Schema_->Field_ = descriptor.Field;
    return Schema_;
}

////////////////////////////////////////////////////////////////////////////////

template <class TTypedObject, class TTypedValue>
void TScalarAttributeSchemaBuilder::InitPreloader(
    const TScalarAttributeDescriptor<TTypedObject, TTypedValue>& descriptor)
{
    Schema_->Preloader_ =
        [&descriptor] (const TObject* object, const NYPath::TYPath&)
        {
            const auto* typedObject = object->As<TTypedObject>();
            auto* attribute = descriptor.AttributeGetter(typedObject);
            attribute->ScheduleLoad();
        };
}

template <class TTypedObject, class TTypedValue>
void TScalarAttributeSchemaBuilder::InitUpdatePreloader(
    const TScalarAttributeDescriptor<TTypedObject, TTypedValue>& descriptor)
{
    Schema_->UpdatePreloader_ =
        [&descriptor, schema = Schema_] (
            TTransaction* /*transaction*/,
            const TObject* object,
            const TUpdateRequest& request)
        {
            auto path = GetUpdateRequestPath(request);
            descriptor.AttributeGetter(object->As<TTypedObject>())->BeforeStorePreload();
            // Otherwise attribute will be overwritten, no need to load its previous value.
            bool needsUpdatePreload = path ||
                object->GetTypeHandler()->SkipStoreWithoutChanges() ||
                object->GetTypeHandler()->AreTagsEnabled() ||
                !schema->UpdateHandlers_.empty() ||
                !schema->Validators_.empty() ||
                descriptor.DoesNeedOldValue();
            if (needsUpdatePreload) {
                schema->RunPreloader(object, path);
            }
        };
}

template <class TTypedObject, class TTypedValue>
void TScalarAttributeSchemaBuilder::InitValueSetter(
    const TScalarAttributeDescriptor<TTypedObject, TTypedValue>& descriptor)
{
    Schema_->ValueSetter_ =
        [&descriptor, schema = Schema_] (
            TTransaction* transaction,
            TObject* object,
            const NYPath::TYPath& path,
            const NYTree::INodePtr& value,
            bool recursive,
            std::optional<bool> sharedWrite,
            EAggregateMode aggregateMode,
            const TTransactionCallContext& /*transactionCallContext*/)
        {
            THROW_ERROR_EXCEPTION_IF(aggregateMode != EAggregateMode::Unspecified,
                NClient::EErrorCode::InvalidRequestArguments,
                "ScalarAttribute cannot be updated with aggregate mode");
            auto* typedObject = object->As<TTypedObject>();
            auto* attribute = descriptor.AttributeGetter(typedObject);

            std::optional<TTypedValue> oldValueHolder;
            if (descriptor.DoesNeedOldValue()) {
                oldValueHolder = attribute->Load();
            } else if (path.Empty()) {
                attribute->StoreDefault(sharedWrite);
            }

            // TODO(dgolear): Drop |IsExtensible| check in YTORM-775.
            auto* mutableValue = attribute->MutableLoad(sharedWrite);
            UpdateScalarAttributeValue(schema, path, value, *mutableValue, recursive, /*forceSetViaYson*/ schema->IsExtensible());

            if (auto& initializer = descriptor.GetInitializer();
                object->GetState() == EObjectState::Creating && initializer)
            {
                initializer(transaction, typedObject, mutableValue);
            }

            if (oldValueHolder) {
                descriptor.RunValidators(object, *oldValueHolder, mutableValue);
            } else {
                descriptor.RunFinalValueValidators(object);
            }
        };
}

template <class TTypedObject, class TTypedValue>
void TScalarAttributeSchemaBuilder::InitValueGetter(const TScalarAttributeDescriptor<TTypedObject, TTypedValue>& descriptor)
{
    Schema_->SetValueGetter<TTypedObject>(
        [&descriptor] (
            TTransaction* /*transaction*/,
            const TTypedObject* object,
            NYson::IYsonConsumer* consumer)
        {
            auto* attribute = descriptor.AttributeGetter(object);
            NDetail::YsonConsumeValue(attribute->Load(), consumer);
        });
}

template <class TTypedValue>
void TScalarAttributeSchemaBuilder::InitDefaultValueGetter()
{
    Schema_->DefaultValueGetter_ =
        [] {
            return NDetail::ValueToNode(TTypedValue{});
        };
}

template <class TTypedObject, class TTypedValue, class TDescriptor>
void TScalarAttributeSchemaBuilder::InitStoreScheduledGetter(const TDescriptor& descriptor)
{
    Schema_->StoreScheduledGetter_ =
        [&descriptor] (const TObject* object) {
            const auto* typedObject = object->As<TTypedObject>();
            auto* attribute = descriptor.AttributeGetter(typedObject);
            return attribute->IsStoreScheduled();
        };
}

template <class TTypedObject, class TTypedValue>
void TScalarAttributeSchemaBuilder::InitChangedGetter(const TScalarAttributeDescriptor<TTypedObject, TTypedValue>& descriptor)
{
    Schema_->ChangedGetterType_ = EChangedGetterType::Default;
    Schema_->ChangedGetter_ =
        [&descriptor] (const TObject* object, const NYPath::TYPath& path) {
            const auto* typedObject = object->As<TTypedObject>();
            auto* attribute = descriptor.AttributeGetter(typedObject);
            return attribute->IsChanged(path);
        };
}

template <class TTypedObject, class TTypedValue>
void TScalarAttributeSchemaBuilder::InitFilteredChangedGetter(
    const TScalarAttributeDescriptor<TTypedObject, TTypedValue>& descriptor)
{
    Schema_->FilteredChangedGetterType_ = EChangedGetterType::Default;
    Schema_->FilteredChangedGetter_ =
        [&descriptor, schema = Schema_] (
            const TObject* object,
            const NYPath::TYPath& path,
            std::function<bool(const NYPath::TYPath&)> byPathFilter)
        {
            if (schema->HasChangedGetter()) {
                switch (schema->GetChangedGetterType()) {
                    case EChangedGetterType::ConstantTrue:
                        return true;
                    case EChangedGetterType::ConstantFalse:
                        return false;
                    default:
                        break;
                }
            }
            const auto* typedObject = object->As<TTypedObject>();
            auto* attribute = descriptor.AttributeGetter(typedObject);
            if (!byPathFilter) {
                return attribute->IsChanged(path);
            }
            return attribute->IsChangedWithFilter(path, schema->GetPath(), byPathFilter);
        };
}

template <
    class TTypedObject,
    class TTypedValue,
    CScalarOrAggregatedAttributeDescriptor<TTypedObject, TTypedValue> TDescriptor
>
void TScalarAttributeSchemaBuilder::InitTimestampGetter(const TDescriptor& descriptor)
{
    Schema_->TimestampPregetter_ =
        [&descriptor] (const TObject* object, const NYPath::TYPath& /*path*/)
        {
            const auto* typedObject = object->As<TTypedObject>();
            auto* attribute = descriptor.AttributeGetter(typedObject);
            attribute->ScheduleLoadTimestamp();
        };

    Schema_->TimestampGetter_ =
        [&descriptor] (const TObject* object, const NYPath::TYPath& /*path*/)
        {
            const auto* typedObject = object->As<TTypedObject>();
            auto* attribute = descriptor.AttributeGetter(typedObject);
            return attribute->LoadTimestamp();
        };
}

template <class TTypedObject, class TTypedValue>
void TScalarAttributeSchemaBuilder::InitInitializer(const TScalarAttributeDescriptor<TTypedObject, TTypedValue>& descriptor)
{
    if (!descriptor.GetInitializer()) {
        return;
    }
    YT_VERIFY(!Schema_->Initializer_);
    Schema_->Initializer_ =
        [&descriptor] (TTransaction* transaction, TObject* object) {
            auto* typedObject = object->As<TTypedObject>();
            auto* attribute = descriptor.AttributeGetter(typedObject);
            TTypedValue typedValue{};
            descriptor.GetInitializer()(transaction, typedObject, &typedValue);
            descriptor.RunValidators(typedObject, attribute->Load(), &typedValue);
            attribute->Store(typedValue);
        };
}

template <class TTypedObject, class TTypedValue>
void TScalarAttributeSchemaBuilder::InitRemover(const TScalarAttributeDescriptor<TTypedObject, TTypedValue>& descriptor)
{
    Schema_->Remover_ =
        [&descriptor, schema = Schema_] (
            TTransaction* /*transaction*/,
            TObject* object,
            const NYPath::TYPath& path,
            bool force)
        {
            auto* typedObject = object->As<TTypedObject>();
            auto* attribute = descriptor.AttributeGetter(typedObject);
            auto sharedWrite = false;

            if (path.empty()) {
                attribute->StoreDefault(sharedWrite);
                return;
            }
            bool removeViaNode = true;
            if constexpr (std::is_convertible_v<TTypedValue*, google::protobuf::Message*>) {
                // If OldNewValidators in TScalarAttributeDescriptor needs old value need to keep it.
                std::optional<TTypedValue> tmpTypedValue;
                if (descriptor.DoesNeedOldValue()) {
                    tmpTypedValue = attribute->Load();
                }
                auto* typedValue = tmpTypedValue ? &*tmpTypedValue : attribute->MutableLoad(sharedWrite);
                YT_VERIFY(typedValue);
                NAttributes::ClearProtobufFieldByPath(*typedValue, path, /*skipMissing*/ force);
                descriptor.RunValidators(typedObject, attribute->Load(), typedValue);
                if (tmpTypedValue) {
                    attribute->Store(std::move(*tmpTypedValue), sharedWrite);
                }
                removeViaNode = false;
            }
            if (removeViaNode) {
                auto newValue = NYTree::ConvertToNode(attribute->Load());
                NYTree::SyncYPathRemove(newValue, path, /*recursive*/ true, force);
                auto newTypedValue = ParseScalarAttributeFromYsonNode<TTypedValue>(schema, newValue);
                descriptor.RunValidators(typedObject, attribute->Load(), &newTypedValue);
                attribute->Store(std::move(newTypedValue), sharedWrite);
            }
        };
}

template <
    class TTypedObject,
    class TTypedValue,
    CScalarOrAggregatedAttributeDescriptor<TTypedObject, TTypedValue> TDescriptor
>
void TScalarAttributeSchemaBuilder::InitLocker(const TDescriptor& descriptor)
{
    Schema_->Locker_ =
        [&descriptor] (
            TTransaction* /*transaction*/,
            TObject* object,
            const NYPath::TYPath& /*path*/,
            NTableClient::ELockType lockType)
        {
            auto* typedObject = object->As<TTypedObject>();
            auto* attribute = descriptor.AttributeGetter(typedObject);

            attribute->Lock(lockType);
        };
}

template <class TDescriptor>
void TScalarAttributeSchemaBuilder::InitListContainsExpressionBuilder(const TDescriptor& descriptor)
{
    Schema_->ListContainsExpressionBuilder_ =
        [&descriptor] (
            IQueryContext* context,
            NQueryClient::NAst::TLiteralExpression* literalExpr,
            const NYPath::TYPath& path,
            EAttributeExpressionContext expressionContext) -> NQueryClient::NAst::TExpressionPtr
        {
            const TScalarAttributeIndexDescriptor* indexDescriptor = context->GetIndexDescriptor();
            if (!indexDescriptor || !indexDescriptor->Repeated) {
                return nullptr;
            }

            const auto& attributeSchemas = indexDescriptor->IndexedAttributeDescriptors;
            YT_VERIFY(attributeSchemas.size() == 1);
            if (attributeSchemas[0].AttributeDescriptor->Field != descriptor.Field) {
                return nullptr;
            }

            return context->New<NQueryClient::NAst::TBinaryOpExpression>(
                NQueryClient::TSourceLocation(),
                NQueryClient::EBinaryOp::Equal,
                NQueryClient::NAst::TExpressionList{
                    context->GetScalarAttributeExpression(
                        descriptor.Field,
                        path,
                        expressionContext,
                        descriptor.Field->Type),
                },
                NQueryClient::NAst::TExpressionList{
                    literalExpr,
                });
        };
}

template <class TOwner, class TForeign, template<class, class> class TReferenceDescriptor>
void TScalarAttributeSchemaBuilder::SetReferenceCommon(
    const TReferenceDescriptor<TOwner, TForeign>& descriptor)
{
    Schema_->SetOpaque();

    for (auto keyAttributeDescriptor : GetKeyAttributeDescriptors(descriptor.KeyStorageDescriptor)) {
        keyAttributeDescriptor->BeforeStoreObservers.Add([&descriptor] (TObject* owner, bool sharedWrite) {
            if (sharedWrite) {
                THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments,
                    "Reference field cannot be updated with shared write lock")
                    << TErrorAttribute("owner",
                        NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(TOwner::Type))
                    << TErrorAttribute("foreign",
                        NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(TForeign::Type));
            }
            descriptor.ForwardAttributeGetter(owner->As<TOwner>())->BeforeKeyStore();
        });
        keyAttributeDescriptor->BeforeMutableLoadObservers.Add([&descriptor] (TObject* owner, bool sharedWrite) {
            if (sharedWrite) {
                THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidRequestArguments,
                    "Reference field cannot be updated with shared write lock")
                    << TErrorAttribute("owner",
                        NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(TOwner::Type))
                    << TErrorAttribute("foreign",
                        NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(TForeign::Type));
            }
            descriptor.ForwardAttributeGetter(owner->As<TOwner>())->BeforeKeyMutableLoad();
        });
    }

    Schema_->DefaultValueGetter_ =
        [] {
            return NYTree::GetEphemeralNodeFactory()->CreateEntity();
        };

    Schema_->Preloader_ =
        [&descriptor] (const TObject* owner, const NYPath::TYPath&) {
            auto* typedOwner = const_cast<TOwner*>(owner->As<TOwner>());
            auto* forwardAttribute = descriptor.ForwardAttributeGetter(typedOwner);
            forwardAttribute->ScheduleLoad();
        };

    Schema_->StoreScheduledGetter_  =
        [=] (const TObject* owner) {
            auto* typedOwner = const_cast<TOwner*>(owner->As<TOwner>());
            auto* forwardAttribute = descriptor.ForwardAttributeGetter(typedOwner);
            return forwardAttribute->IsStoreScheduled();
        };

    Schema_->ChangedGetterType_ = EChangedGetterType::Default;
    Schema_->ChangedGetter_ =
        [=] (const TObject* owner, const NYPath::TYPath& path) {
            THROW_ERROR_EXCEPTION_UNLESS(path.empty(),
                "Cannot collect changes at the non-empty path %v: reference attribute is not composite",
                path);
            auto* typedOwner = const_cast<TOwner*>(owner->As<TOwner>());
            auto* forwardAttribute = descriptor.ForwardAttributeGetter(typedOwner);
            return forwardAttribute->IsChanged();
        };
}

template <class TOwner, class TForeign>
void TScalarAttributeSchemaBuilder::SetViewCommon()
{
    Schema_->SetOpaque();
    Schema_->SetView();
    Schema_->SetUpdatePolicy(NYT::NOrm::NServer::NObjects::EUpdatePolicy::OpaqueReadOnly);
    Schema_->SetConstantChangedGetter(false);
    Schema_->ViewObjectType_ = TForeign::Type;

    Schema_->DefaultValueGetter_ =
        [] {
            return NYTree::GetEphemeralNodeFactory()->CreateEntity();
        };
}

template <class TTypedObject, class TTypedValue, class TDescriptor>
void TScalarAttributeSchemaBuilder::AddPostValidatorIfNecessary(const TDescriptor& /*descriptor*/)
{ }

template <
    class TTypedObject,
    CStringValidatableValue TTypedValue,
    CScalarOrAggregatedAttributeDescriptor<TTypedObject, TTypedValue> TDescriptor
>
void TScalarAttributeSchemaBuilder::AddPostValidatorIfNecessary(const TDescriptor& descriptor)
{
    auto postInitializer = [&descriptor, schema = Schema_] {
        auto resolvedElement = NYson::ResolveProtobufElementByYPath(
            schema->TypeHandler_->GetRootProtobufType(), schema->GetPath());
        int type;

        if constexpr (std::is_same_v<TTypedValue, TString>) {
            auto* ptr = std::get_if<std::unique_ptr<NYson::TProtobufScalarElement>>(&resolvedElement.Element);
            YT_VERIFY(ptr);

            type = static_cast<int>((*ptr)->Type);
        } else if constexpr (std::is_same_v<TTypedValue, std::vector<TString>>) {
            auto* repeatedPtr = std::get_if<std::unique_ptr<NYson::TProtobufRepeatedElement>>(
                &resolvedElement.Element);
            YT_VERIFY(repeatedPtr);
            auto* valueTypePtr = std::get_if<std::unique_ptr<NYson::TProtobufScalarElement>>(
                &((*repeatedPtr)->Element));
            YT_VERIFY(valueTypePtr);

            type = static_cast<int>((*valueTypePtr)->Type);
        } else if constexpr (NMpl::IsSpecialization<TTypedValue, THashMap>) {
            if (std::is_same_v<typename TTypedValue::key_type, TString>) {
                auto* mapPtr = std::get_if<std::unique_ptr<NYson::TProtobufMapElement>>(&resolvedElement.Element);
                YT_VERIFY(mapPtr);

                type = static_cast<int>((*mapPtr)->KeyElement.Type);
            }
        } else {
            // Poor man's static_assert(false).
            static_assert(std::is_same_v<TTypedValue, TString>);
        }

        if (type == NProtoBuf::FieldDescriptor::TYPE_STRING) {
            descriptor.AddValidator(
                [schema = std::move(schema)] (TTransaction*, const TTypedObject* typedObject, const TTypedValue& typedValue) {
                    NDetail::ValidateString(typedValue,
                        typedObject->GetSession()->GetConfigsSnapshot().ObjectManager->Utf8Check,
                        schema->FormatTypePath());
                });
        } else {
            YT_VERIFY(type == NProtoBuf::FieldDescriptor::TYPE_BYTES);
        }
    };

    Schema_->PostInitializers_.push_back(std::move(postInitializer));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
