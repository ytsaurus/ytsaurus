#include "attribute_schema.h"

#include "helpers.h"
#include "object_manager.h"
#include "type_handler.h"

#include <yp/server/lib/objects/type_info.h>

#include <yt/core/ypath/tokenizer.h>

#include <yt/core/ytree/fluent.h>

#include <util/string/join.h>

namespace NYP::NServer::NObjects {

using namespace NAccessControl;

using namespace NYT::NYTree;
using namespace NYT::NYPath;
using namespace NYT::NQueryClient::NAst;

using NYT::NYson::TYsonString;
using NYT::NYson::IYsonConsumer;
using NYT::NQueryClient::TSourceLocation;

////////////////////////////////////////////////////////////////////////////////

TAttributeSchema* TAttributeSchema::SetAnnotationsAttribute()
{
    Annotations_ = true;
    Updatable_ = true;

    ValueSetter_ =
        [=] (
            TTransaction* /*transaction*/,
            TObject* object,
            const TYPath& path,
            const INodePtr& value,
            bool recursive)
        {
            auto* attribute = &object->Annotations();

            NYPath::TTokenizer tokenizer(path);

            if (tokenizer.Advance() == ETokenType::EndOfStream) {
                for (const auto& pair : attribute->LoadAll()) {
                    attribute->Store(pair.first, TYsonString());
                }
                for (const auto& pair : value->AsMap()->GetChildren()) {
                    attribute->Store(pair.first, ConvertToYsonString(pair.second));
                }
            } else {
                tokenizer.Expect(ETokenType::Slash);

                tokenizer.Advance();
                tokenizer.Expect(ETokenType::Literal);
                auto key = tokenizer.GetLiteralValue();

                TYsonString updatedYson;
                if (tokenizer.Advance() == ETokenType::EndOfStream) {
                    updatedYson = ConvertToYsonString(value);
                } else {
                    INodePtr existingNode;
                    auto existingYson = attribute->Load(key);
                    if (existingYson) {
                        try {
                            existingNode = ConvertToNode(existingYson);
                        } catch (const std::exception& ex) {
                            THROW_ERROR_EXCEPTION("Error parsing value of annotation %Qv of object %Qv",
                                key,
                                object->GetId())
                                    << ex;
                        }
                    } else {
                        if (!recursive) {
                            THROW_ERROR_EXCEPTION("%v %v has no annotation %Qv",
                                GetCapitalizedHumanReadableTypeName(object->GetType()),
                                GetObjectDisplayName(object),
                                key);
                        }
                        existingNode = GetEphemeralNodeFactory()->CreateMap();
                    }

                    // TODO(babenko): optimize
                    SyncYPathSet(
                        existingNode,
                        TYPath(tokenizer.GetInput()),
                        ConvertToYsonString(value),
                        recursive);
                    updatedYson = ConvertToYsonString(existingNode);
                }

                attribute->Store(key, updatedYson);
            }
        };

    auto parseAnnotationKey = [] (const TYPath& path) {
        NYPath::TTokenizer tokenizer(path);

        if (tokenizer.Advance() == ETokenType::EndOfStream) {
            THROW_ERROR_EXCEPTION("Cannot compute timestamp for the whole /annotations");
        }
        tokenizer.Expect(ETokenType::Slash);

        tokenizer.Advance();
        tokenizer.Expect(ETokenType::Literal);

        return TString(tokenizer.GetToken());
    };

    TimestampPregetter_ =
        [=] (
            TTransaction* /*transaction*/,
            TObject* object,
            const TYPath& path)
        {
            auto key = parseAnnotationKey(path);
            object->Annotations().ScheduleLoadTimestamp(key);
        };

    TimestampGetter_ =
        [=] (
            TTransaction* /*transaction*/,
            TObject* object,
            const TYPath& path)
        {
            auto key = parseAnnotationKey(path);
            return object->Annotations().LoadTimestamp(key);
        };

    Remover_ =
        [=] (
            TTransaction* /*transaction*/,
            TObject* object,
            const TYPath& path)
        {
            TTokenizer tokenizer(path);

            if (tokenizer.Advance() == ETokenType::EndOfStream) {
                THROW_ERROR_EXCEPTION("Attribute %Qv cannot be removed",
                    GetPath());
            }
            tokenizer.Expect(ETokenType::Slash);

            tokenizer.Advance();
            tokenizer.Expect(ETokenType::Literal);
            auto key = tokenizer.GetLiteralValue();

            auto* attribute = &object->Annotations();

            TYsonString updatedYson;
            if (tokenizer.Advance() != ETokenType::EndOfStream) {
                auto existingYson = attribute->Load(key);
                if (!existingYson) {
                    THROW_ERROR_EXCEPTION("%v %v has no annotation %Qv",
                        GetCapitalizedHumanReadableTypeName(object->GetType()),
                        GetObjectDisplayName(object),
                        key);
                }

                INodePtr existingNode;
                try {
                    existingNode = ConvertToNode(existingYson);
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Error parsing value of annotation %Qv of %v %v",
                        key,
                        GetHumanReadableTypeName(object->GetType()),
                        GetObjectDisplayName(object))
                        << ex;
                }

                // TODO(babenko): optimize
                SyncYPathRemove(existingNode, TYPath(tokenizer.GetInput()));
                updatedYson = ConvertToYsonString(existingNode);
            }

            attribute->Store(key, updatedYson);
        };


    ExpressionBuilder_ =
        [=] (
            IQueryContext* context,
            const TYPath& path)
        {
            if (path.empty()) {
                THROW_ERROR_EXCEPTION("Querying /annotations as a whole is not supported");
            }

            TTokenizer tokenizer(path);
            tokenizer.Advance();
            tokenizer.Expect(ETokenType::Slash);
            tokenizer.Advance();
            tokenizer.Expect(ETokenType::Literal);

            auto name = tokenizer.GetLiteralValue();
            auto suffixPath = TYPath(tokenizer.GetSuffix());

            auto attrExpr = context->GetAnnotationExpression(name);
            if (suffixPath.empty()) {
                return attrExpr;
            }

            return TExpressionPtr(New<TFunctionExpression>(
                TSourceLocation(),
                "try_get_any",
                TExpressionList{
                    std::move(attrExpr),
                    New<TLiteralExpression>(
                        TSourceLocation(),
                        suffixPath)
                }));
        };

    Preevaluator_ =
        [=] (
            TTransaction* /*transaction*/,
            TObject* object)
        {
            object->Annotations().ScheduleLoadAll();
        };

    Evaluator_ =
        [=] (
            TTransaction* /*transaction*/,
            TObject* object,
            IYsonConsumer* consumer)
        {
            auto annotations = object->Annotations().LoadAll();
            BuildYsonFluently(consumer)
                .DoMapFor(annotations, [&] (auto fluent, const auto& pair) {
                    fluent.Item(pair.first).Value(pair.second);
                });
        };

    return this;
}

TAttributeSchema* TAttributeSchema::SetIdAttribute()
{
    InitExpressionBuilder(
        TypeHandler_->GetIdField(),
        TEmptyPathValidator::Run);
    return this;
}

TAttributeSchema* TAttributeSchema::SetParentIdAttribute()
{
    InitExpressionBuilder(
        TypeHandler_->GetParentIdField(),
        TEmptyPathValidator::Run);
    return this;
}

TAttributeSchema* TAttributeSchema::SetControlAttribute()
{
    Control_ = true;
    return this;
}

TAttributeSchema::TAttributeSchema(
    IObjectTypeHandler* typeHandler,
    TObjectManager* objectManager,
    const TString& name)
    : TypeHandler_(typeHandler)
    , ObjectManager_(objectManager)
    , Name_(name)
{ }

bool TAttributeSchema::IsComposite() const
{
    return Composite_;
}

TAttributeSchema* TAttributeSchema::SetOpaque()
{
    Opaque_ = true;
    return this;
}

bool TAttributeSchema::IsOpaque() const
{
    return Opaque_;
}

bool TAttributeSchema::IsControl() const
{
    return Control_;
}

bool TAttributeSchema::IsAnnotationsAttribute() const
{
    return Annotations_;
}

const TString& TAttributeSchema::GetName() const
{
    return Name_;
}

NYPath::TYPath TAttributeSchema::GetPath() const
{
    SmallVector<const TAttributeSchema*, 4> parents;
    const auto* current = this;
    while (current->GetParent()) {
        if (!current->IsEtc()) {
            parents.push_back(current);
        }
        current = current->GetParent();
    }
    TStringBuilder builder;
    for (auto it = parents.rbegin(); it != parents.rend(); ++it) {
        builder.AppendChar('/');
        builder.AppendString(ToYPathLiteral((*it)->GetName()));
    }
    return builder.Flush();
}

TAttributeSchema* TAttributeSchema::GetParent() const
{
    return Parent_;
}

void TAttributeSchema::SetParent(TAttributeSchema* parent)
{
    YT_VERIFY(!Parent_);
    Parent_ = parent;
}

TAttributeSchema* TAttributeSchema::SetComposite()
{
    YT_VERIFY(!Etc_);
    Composite_ = true;
    return this;
}

TAttributeSchema* TAttributeSchema::SetExtensible()
{
    Extensible_ = true;
    return this;
}

bool TAttributeSchema::IsExtensible() const
{
    if (!ObjectManager_->AreExtensibleAttributesEnabled()) {
        return false;
    }
    const auto* current = this;
    while (current) {
        if (current->Extensible_) {
            return true;
        }
        current = current->GetParent();
    }
    return false;
}

void TAttributeSchema::AddChild(TAttributeSchema* child)
{
    SetComposite();
    child->SetParent(this);
    if (child->IsEtc()) {
        YT_VERIFY(!EtcChild_);
        EtcChild_ = child;
    } else {
        YT_VERIFY(KeyToChild_.emplace(child->GetName(), child).second);
    }
}

TAttributeSchema* TAttributeSchema::AddChildren(const std::vector<TAttributeSchema*>& children)
{
    for (auto* child : children) {
        AddChild(child);
    }
    return this;
}

TAttributeSchema* TAttributeSchema::FindChild(const TString& key) const
{
    auto it = KeyToChild_.find(key);
    return it == KeyToChild_.end() ? nullptr : it->second;
}

TAttributeSchema* TAttributeSchema::FindEtcChild() const
{
    return EtcChild_;
}

TAttributeSchema* TAttributeSchema::GetChildOrThrow(const TString& key) const
{
    auto* child = FindChild(key);
    if (!child) {
        THROW_ERROR_EXCEPTION("Attribute %Qv has no child with key %Qv",
            GetPath(),
            key);
    }
    return child;
}

const THashMap<TString, TAttributeSchema*>& TAttributeSchema::KeyToChild() const
{
    return KeyToChild_;
}

bool TAttributeSchema::HasValueSetter() const
{
    return ValueSetter_.operator bool();
}

void TAttributeSchema::RunValueSetter(
    TTransaction* transaction,
    TObject* object,
    const TYPath& path,
    const INodePtr& value,
    bool recursive)
{
    ValueSetter_(transaction, object, path, value, recursive);
}

bool TAttributeSchema::HasValueGetter() const
{
    return ValueGetter_.operator bool();
}

INodePtr TAttributeSchema::RunValueGetter(TObject* object) const
{
    return ValueGetter_(object);
}

bool TAttributeSchema::HasStoreScheduledGetter() const
{
    return StoreScheduledGetter_.operator bool();
}

bool TAttributeSchema::RunStoreScheduledGetter(TObject* object) const
{
    return StoreScheduledGetter_(object);
}

bool TAttributeSchema::HasInitializer() const
{
    return Initializer_.operator bool();
}

void TAttributeSchema::RunInitializer(
    TTransaction* transaction,
    TObject* object)
{
    Initializer_(transaction, object);
}

void TAttributeSchema::RunUpdatePrehandlers(
    TTransaction* transaction,
    TObject* object)
{
    for (const auto& prehandler : UpdatePrehandlers_) {
        prehandler(transaction, object);
    }
}

void TAttributeSchema::RunUpdateHandlers(
    TTransaction* transaction,
    TObject* object)
{
    for (const auto& handler : UpdateHandlers_) {
        handler(transaction, object);
    }
}

void TAttributeSchema::RunValidators(
    TTransaction* transaction,
    TObject* object)
{
    try {
        for (const auto& validator : Validators_) {
            validator(transaction, object);
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error validating %Qv for %v %v",
            GetPath(),
            GetHumanReadableTypeName(object->GetType()),
            GetObjectDisplayName(object))
            << ex;
    }
}

bool TAttributeSchema::HasRemover() const
{
    return Remover_.operator bool();
}

void TAttributeSchema::RunRemover(TTransaction* transaction, TObject* object, const TYPath& path)
{
    Remover_(transaction, object, path);
}

bool TAttributeSchema::HasPreupdater() const
{
    return Preupdater_.operator bool();
}

void TAttributeSchema::RunPreupdater(
    TTransaction* transaction,
    TObject* object,
    const TUpdateRequest& request)
{
    Preupdater_(transaction, object, request);
}

TAttributeSchema* TAttributeSchema::SetExpressionBuilder(std::function<TExpressionPtr(IQueryContext*)> builder)
{
    ExpressionBuilder_ =
        [=] (IQueryContext* context, const TYPath& path) {
            if (!path.empty()) {
                THROW_ERROR_EXCEPTION("Attribute %Qv can only be queried as a whole",
                    GetPath());
            }
            return builder(context);
        };
    return this;
}

bool TAttributeSchema::HasExpressionBuilder() const
{
    return ExpressionBuilder_.operator bool();
}

NYT::NQueryClient::NAst::TExpressionPtr TAttributeSchema::RunExpressionBuilder(
    IQueryContext* context,
    const TYPath& path)
{
    return ExpressionBuilder_(context, path);
}

bool TAttributeSchema::HasPreevaluator() const
{
    return Preevaluator_.operator bool();
}

void TAttributeSchema::RunPreevaluator(TTransaction* transaction, TObject* object)
{
    Preevaluator_(transaction, object);
}

bool TAttributeSchema::HasEvaluator() const
{
    return Evaluator_.operator bool();
}

void TAttributeSchema::RunEvaluator(
    TTransaction* transaction,
    TObject* object,
    NYson::IYsonConsumer* consumer)
{
    Evaluator_(transaction, object, consumer);
}

bool TAttributeSchema::HasTimestampPregetter() const
{
    return TimestampPregetter_.operator bool();
}

void TAttributeSchema::RunTimestampPregetter(TTransaction* transaction, TObject* object, const TYPath& path)
{
    TimestampPregetter_(transaction, object, path);
}

bool TAttributeSchema::HasTimestampGetter() const
{
    return TimestampGetter_.operator bool();
}

TTimestamp TAttributeSchema::RunTimestampGetter(TTransaction* transaction, TObject* object, const TYPath& path)
{
    return TimestampGetter_(transaction, object, path);
}

bool TAttributeSchema::HasHistoryFilter() const
{
    return HistoryFilter_.operator bool();
}

bool TAttributeSchema::RunHistoryFilter(TObject* object) const
{
    return HistoryFilter_(object);
}

TAttributeSchema* TAttributeSchema::SetMandatory()
{
    Mandatory_ = true;
    return this;
}

bool TAttributeSchema::GetMandatory() const
{
    return Mandatory_;
}

TAttributeSchema* TAttributeSchema::SetUpdatable()
{
    Updatable_ = true;
    return this;
}

bool TAttributeSchema::GetUpdatable() const
{
    return Updatable_;
}

TAttributeSchema* TAttributeSchema::SetEtc()
{
    Etc_ = true;
    return this;
}

bool TAttributeSchema::IsEtc() const
{
    return Etc_;
}

std::vector<TYPath> TAttributeSchema::GetHistoryEnabledAttributePaths() const
{
    std::vector<TYPath> result;
    GetHistoryEnabledAttributePathsImpl(&result);
    return result;
}

IMapNodePtr TAttributeSchema::GetHistoryEnabledAttributes(TObject* object) const
{
    auto result = GetEphemeralNodeFactory()->CreateMap();
    GetHistoryEnabledAttributesImpl(result, object, false);
    return result;
}

bool TAttributeSchema::HasStoreScheduledHistoryEnabledAttributes(TObject* object) const
{
    return HasStoreScheduledHistoryEnabledAttributesImpl(object, false);
}

void TAttributeSchema::GetHistoryEnabledAttributePathsImpl(std::vector<TYPath>* result) const
{
    if (HasHistoryFilter()) {
        result->push_back(GetPath());
    }

    if (IsComposite()) {
        if (EtcChild_) {
            EtcChild_->GetHistoryEnabledAttributePathsImpl(result);
        }
        for (const auto& [key, child] : KeyToChild_) {
            child->GetHistoryEnabledAttributePathsImpl(result);
        }
    }
}

void TAttributeSchema::GetHistoryEnabledAttributesImpl(
    IMapNodePtr result,
    TObject* object,
    bool hasParentHistoryEnabledAttribute) const
{
    hasParentHistoryEnabledAttribute |= (HasHistoryFilter() && RunHistoryFilter(object));

    if (IsComposite()) {
        if (EtcChild_) {
            EtcChild_->GetHistoryEnabledAttributesImpl(
                result,
                object,
                hasParentHistoryEnabledAttribute);
        }
        for (const auto& [key, child] : KeyToChild_) {
            child->GetHistoryEnabledAttributesImpl(
                result,
                object,
                hasParentHistoryEnabledAttribute);
        }
    } else if (hasParentHistoryEnabledAttribute && HasValueGetter()) {
        const TString path = GetPath();
        ForceYPath(result, path);
        SetNodeByYPath(result, path, RunValueGetter(object));
    }
}

bool TAttributeSchema::HasStoreScheduledHistoryEnabledAttributesImpl(
    TObject* object,
    bool hasParentHistoryEnabledAttribute) const
{
    hasParentHistoryEnabledAttribute |= (HasHistoryFilter() && RunHistoryFilter(object));

    if (hasParentHistoryEnabledAttribute && HasStoreScheduledGetter() && RunStoreScheduledGetter(object)) {
        return true;
    } else if (IsComposite()) {
        if (EtcChild_ && EtcChild_->HasStoreScheduledHistoryEnabledAttributesImpl(object, hasParentHistoryEnabledAttribute)) {
            return true;
        }
        for (const auto& [key, child] : KeyToChild_) {
            if (child->HasStoreScheduledHistoryEnabledAttributesImpl(object, hasParentHistoryEnabledAttribute)) {
                return true;
            }
        }
    }

    return false;
}

TAttributeSchema* TAttributeSchema::SetReadPermission(EAccessControlPermission permission)
{
    if (permission != EAccessControlPermission::None) {
        Opaque_ = true;
    }
    ReadPermission_ = permission;
    return this;
}

EAccessControlPermission TAttributeSchema::GetReadPermission() const
{
    return ReadPermission_;
}

void TAttributeSchema::InitExpressionBuilder(const TDBField* field, TPathValidator pathValidator)
{
    ExpressionBuilder_ =
        [=] (
            IQueryContext* context,
            const TYPath& path)
        {
            pathValidator(this, path);

            auto expr = context->GetFieldExpression(field);
            if (!path.empty()) {
                expr = New<TFunctionExpression>(
                    TSourceLocation(),
                    "try_get_any",
                    TExpressionList{
                        std::move(expr),
                        New<TLiteralExpression>(
                            TSourceLocation(),
                            path)
                    });
            }

            return expr;
        };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

