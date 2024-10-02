#include "attribute_schema_builder.h"

#include "private.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

void TScalarAttributeSchemaBuilder::InitTimestampExpressionBuilder(const TScalarAttributeDescriptorBase& descriptor)
{
    auto keyFields = Schema_->TypeHandler_->GetTable()->GetKeyFields(/*filterEvaluatedFields*/ true);
    bool isInTableKey = std::find(keyFields.begin(), keyFields.end(), descriptor.Field) != keyFields.end();

    Schema_->TimestampExpressionBuilder_ =
        [&descriptor, isInTableKey] (IQueryContext* context, const NYPath::TYPath& path)
            -> NQueryClient::NAst::TExpressionPtr
        {
            if (isInTableKey) {
                return context->New<NQueryClient::NAst::TLiteralExpression>(NQueryClient::TSourceLocation{}, ui64{0});
            } else {
                return context->GetScalarTimestampExpression(descriptor.Field, path);
            }
        };
}

////////////////////////////////////////////////////////////////////////////////

TScalarAttributeSchema* TScalarAttributeSchemaBuilder::SetAnnotationsAttribute()
{
    Schema_->Annotations_ = true;
    Schema_->SetUpdatePolicy(EUpdatePolicy::Updatable);

    Schema_->AttributeGetter_ =
        [] (const TObject* object) -> const TAttributeBase* {
            return &object->Annotations();
        };

    Schema_->StoreScheduledGetter_ =
        [] (const TObject* object) {
            return object->Annotations().IsStoreScheduled();
        };

    Schema_->ValueSetter_ =
        [] (
            TTransaction* /*transaction*/,
            TObject* object,
            const NYPath::TYPath& path,
            const NYTree::INodePtr& value,
            bool recursive,
            std::optional<bool> sharedWrite,
            EAggregateMode aggregateMode,
            const TTransactionCallContext& /*transactionCallContext*/)
        {
            THROW_ERROR_EXCEPTION_IF(sharedWrite && *sharedWrite || aggregateMode != EAggregateMode::Unspecified,
                NClient::EErrorCode::InvalidRequestArguments,
                "%Qv annotations cannot be updated with shared write lock / aggregate mode",
                object->GetDisplayName());

            auto* attribute = &object->Annotations();

            NYPath::TTokenizer tokenizer(path);

            if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
                for (const auto& pair : attribute->LoadAll()) {
                    attribute->Store(pair.first, NYson::TYsonString());
                }
                for (const auto& [key, value] : value->AsMap()->GetChildren()) {
                    attribute->Store(key, NYson::ConvertToYsonString(value));
                }
            } else {
                tokenizer.Expect(NYPath::ETokenType::Slash);

                tokenizer.Advance();
                tokenizer.Expect(NYPath::ETokenType::Literal);
                auto key = tokenizer.GetLiteralValue();

                NYson::TYsonString updatedYson;
                if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
                    updatedYson = NYson::ConvertToYsonString(value);
                } else {
                    NYTree::INodePtr existingNode;
                    auto existingYson = attribute->Load(key);
                    if (existingYson) {
                        try {
                            existingNode = NYTree::ConvertToNode(existingYson);
                        } catch (const std::exception& ex) {
                            THROW_ERROR_EXCEPTION(
                                "Error parsing value of annotation %Qv of object %v",
                                key,
                                object->GetKey())
                                << ex;
                        }
                    } else {
                        if (!recursive) {
                            THROW_ERROR_EXCEPTION("%v has no annotation %Qv",
                                object->GetDisplayName(),
                                key);
                        }
                        existingNode = NYTree::GetEphemeralNodeFactory()->CreateMap();
                    }

                    // TODO(babenko): optimize
                    SyncYPathSet(
                        existingNode,
                        NYPath::TYPath(tokenizer.GetInput()),
                        NYson::ConvertToYsonString(value),
                        recursive);
                    updatedYson = NYson::ConvertToYsonString(existingNode);
                }

                attribute->Store(key, updatedYson);
            }
        };

    auto parseAnnotationKey = [] (const NYPath::TYPath& path) {
        NYPath::TTokenizer tokenizer(path);

        if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
            THROW_ERROR_EXCEPTION("Cannot compute timestamp for the whole /annotations");
        }
        tokenizer.Expect(NYPath::ETokenType::Slash);

        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Literal);

        return TString(tokenizer.GetToken());
    };

    Schema_->TimestampPregetter_ =
        [parseAnnotationKey] (
            const TObject* object,
            const NYPath::TYPath& path)
        {
            auto key = parseAnnotationKey(path);
            object->Annotations().ScheduleLoadTimestamp(key);
        };

    Schema_->TimestampGetter_ =
        [parseAnnotationKey] (
            const TObject* object,
            const NYPath::TYPath& path)
        {
            auto key = parseAnnotationKey(path);
            return object->Annotations().LoadTimestamp(key);
        };

    Schema_->Remover_ =
        [] (
            TTransaction* /*transaction*/,
            TObject* object,
            const NYPath::TYPath& path,
            bool force)
        {
            NYPath::TTokenizer tokenizer(path);

            auto* attribute = &object->Annotations();

            if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
                for (const auto& pair : attribute->LoadAll()) {
                    attribute->Store(pair.first, NYson::TYsonString());
                }
                return;
            }
            tokenizer.Expect(NYPath::ETokenType::Slash);

            tokenizer.Advance();
            tokenizer.Expect(NYPath::ETokenType::Literal);
            auto key = tokenizer.GetLiteralValue();

            NYson::TYsonString updatedYson;
            if (tokenizer.Advance() != NYPath::ETokenType::EndOfStream) {
                auto existingYson = attribute->Load(key);
                if (!existingYson) {
                    THROW_ERROR_EXCEPTION("%v has no annotation %Qv",
                        object->GetDisplayName(),
                        key);
                }

                NYTree::INodePtr existingNode;
                try {
                    existingNode = NYTree::ConvertToNode(existingYson);
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Error parsing value of annotation %Qv of %v",
                        key,
                        object->GetDisplayName())
                        << ex;
                }

                // TODO(babenko): optimize
                SyncYPathRemove(existingNode, NYPath::TYPath(tokenizer.GetInput()), /*recursive*/ true, force);
                updatedYson = NYson::ConvertToYsonString(existingNode);
            }
            attribute->Store(key, updatedYson);
        };

    Schema_->Preloader_ =
        [] (const TObject* object, const NYPath::TYPath&) {
            object->Annotations().ScheduleLoadAll();
        };

    Schema_->UpdatePreloader_ =
        [] (TTransaction* /*transaction*/, const TObject* object, const TUpdateRequest& request) {
            auto path = GetUpdateRequestPath(request);
            NYPath::TTokenizer tokenizer(path);

            if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
                object->Annotations().ScheduleLoadAll();
            } else {
                tokenizer.Expect(NYPath::ETokenType::Slash);

                tokenizer.Advance();
                tokenizer.Expect(NYPath::ETokenType::Literal);
                auto key = tokenizer.GetLiteralValue();
                object->Annotations().ScheduleLoad(key);
            }
        };

    Schema_->ExpressionBuilder_ =
        [] (
            IQueryContext* context,
            const NYPath::TYPath& path,
            EAttributeExpressionContext)
        {
            THROW_ERROR_EXCEPTION_IF(path.empty(),
                "Querying /annotations as a whole is not supported");

            NYPath::TTokenizer tokenizer(path);
            tokenizer.Advance();
            tokenizer.Expect(NYPath::ETokenType::Slash);
            tokenizer.Advance();
            tokenizer.Expect(NYPath::ETokenType::Literal);

            auto name = tokenizer.GetLiteralValue();
            auto suffixPath = NYPath::TYPath(tokenizer.GetSuffix());

            auto attrExpr = context->GetAnnotationExpression(name);
            if (suffixPath.empty()) {
                return attrExpr;
            }

            return NQueryClient::NAst::TExpressionPtr(context->New<NQueryClient::NAst::TFunctionExpression>(
                NQueryClient::TSourceLocation(),
                "try_get_any",
                NQueryClient::NAst::TExpressionList{
                    std::move(attrExpr),
                    context->New<NQueryClient::NAst::TLiteralExpression>(
                        NQueryClient::TSourceLocation(),
                        suffixPath)
                }));
        };

    Schema_->SetValueGetter<TObject>(
        [] (
            TTransaction* /*transaction*/,
            const TObject* object,
            NYson::IYsonConsumer* consumer)
        {
            auto annotations = object->Annotations().LoadAll();
            NYTree::BuildYsonFluently(consumer)
                .DoMapFor(annotations, [] (auto fluent, const auto& pair) {
                    fluent.Item(pair.first).Value(pair.second);
                });
        });

    return Schema_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
