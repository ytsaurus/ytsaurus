#include "attribute_schema.h"

#include "helpers.h"
#include "object_manager.h"
#include "type_handler.h"

#include <yp/server/lib/objects/type_info.h>

#include <yt/core/ypath/tokenizer.h>

#include <yt/core/ytree/exception_helpers.h>
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

void ValidateAttributePath(const NYPath::TYPath& attributePath)
{
    NYPath::TTokenizer tokenizer(attributePath);

    while (tokenizer.Advance() != NYPath::ETokenType::EndOfStream) {
        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Literal);
    }
}

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

            return TExpressionPtr(context->New<TFunctionExpression>(
                TSourceLocation(),
                "try_get_any",
                TExpressionList{
                    std::move(attrExpr),
                    context->New<TLiteralExpression>(
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
    YT_VERIFY(!Composite_);
    Etc_ = true;
    return this;
}

bool TAttributeSchema::IsEtc() const
{
    return Etc_;
}

////////////////////////////////////////////////////////////////////////////////

THistoryEnabledAttributeSchema& THistoryEnabledAttributeSchema::SetPath(NYPath::TYPath path)
{
    Path = std::move(path);
    return *this;
}

TAttributeSchema* TAttributeSchema::EnableHistory(
    THistoryEnabledAttributeSchema schema)
{
    ValidateAttributePath(schema.Path);
    YT_VERIFY(!HistoryEnabledAttribute_);
    HistoryEnabledAttribute_ = std::move(schema);
    return this;
}

std::vector<TYPath> TAttributeSchema::GetHistoryEnabledAttributePaths() const
{
    std::vector<TYPath> result;
    FillHistoryEnabledAttributePaths(&result);
    return result;
}

INodePtr TAttributeSchema::GetHistoryEnabledAttributes(TObject* object) const
{
    return GetHistoryEnabledAttributesImpl(object, false);
}

bool TAttributeSchema::HasHistoryEnabledAttributeForStore(TObject* object) const
{
    return HasHistoryEnabledAttributeForStoreImpl(object, false);
}

////////////////////////////////////////////////////////////////////////////////

void TAttributeSchema::FillHistoryEnabledAttributePaths(std::vector<TYPath>* result) const
{
    if (HistoryEnabledAttribute_) {
        result->push_back(GetPath() + HistoryEnabledAttribute_->Path);
    }

    if (IsComposite()) {
        if (EtcChild_) {
            EtcChild_->FillHistoryEnabledAttributePaths(result);
        }
        for (const auto& [key, child] : KeyToChild_) {
            child->FillHistoryEnabledAttributePaths(result);
        }
    }
}

INodePtr TAttributeSchema::GetHistoryEnabledAttributesImpl(
    TObject* object,
    bool hasHistoryEnabledParentAttribute) const
{
    bool hasHistoryEnabledAttribute = hasHistoryEnabledParentAttribute;
    if (HistoryEnabledAttribute_) {
        hasHistoryEnabledAttribute = true;
    }

    if (IsComposite()) {
        if (HistoryEnabledAttribute_) {
            YT_VERIFY(HistoryEnabledAttribute_->Path.empty());
        }

        INodePtr result;
        auto ensureMapResult = [&result] {
            if (!result) {
                result = GetEphemeralNodeFactory()->CreateMap();
            }
            YT_VERIFY(result->GetType() == ENodeType::Map);
        };

        if (EtcChild_) {
            result = EtcChild_->GetHistoryEnabledAttributesImpl(
                object,
                hasHistoryEnabledAttribute);
        }

        for (const auto& [key, child] : KeyToChild_) {
            auto subresult = child->GetHistoryEnabledAttributesImpl(
                object,
                hasHistoryEnabledAttribute);
            if (subresult) {
                ensureMapResult();
                YT_VERIFY(result->AsMap()->AddChild(key, subresult));
            }
        }

        return result;
    }

    if (!hasHistoryEnabledAttribute || !HasValueGetter()) {
        return nullptr;
    }

    auto value = RunValueGetter(object);
    // Check the case of scalar attribute of the pointer type (e.g. /labels).
    if (!value) {
        value = GetEphemeralNodeFactory()->CreateEntity();
    }

    static const NYPath::TYPath EmptyAttributePath;
    const auto& path = hasHistoryEnabledParentAttribute
        ? EmptyAttributePath
        : HistoryEnabledAttribute_->Path;

    if (!path) {
        return value;
    }

    auto result = GetEphemeralNodeFactory()->CreateMap();

    // Supposing path is consistent with the data model.
    ForceYPath(result, path);

    static const TNodeWalkOptions WalkOptions{
        .MissingAttributeHandler = [] (const TString& /* key */) {
            return GetEphemeralNodeFactory()->CreateEntity();
        },
        .MissingChildKeyHandler = [] (const IMapNodePtr& /* node */, const TString& /* key */) {
            return GetEphemeralNodeFactory()->CreateEntity();
        },
        .MissingChildIndexHandler = [] (const IListNodePtr& /* node */, int /* index */) {
            return GetEphemeralNodeFactory()->CreateEntity();
        },
        .NodeCannotHaveChildrenHandler = [] (const INodePtr& node) {
            if (node->GetType() != ENodeType::Entity) {
                ThrowCannotHaveChildren(node);
            }
            return GetEphemeralNodeFactory()->CreateEntity();
        }};

    // Implicitly destroys the previous value of the #value variable, and so
    // detaches found node from its parent and allows to reuse it.
    value = WalkNodeByYPath(value, path, WalkOptions);

    SetNodeByYPath(result, path, value);

    return result;
}

bool TAttributeSchema::HasHistoryEnabledAttributeForStoreImpl(
    TObject* object,
    bool hasAcceptedByHistoryFilterParentAttribute) const
{
    if (HistoryEnabledAttribute_) {
        const auto& valueFilter = HistoryEnabledAttribute_->ValueFilter;
        if (!valueFilter || valueFilter(object)) {
            hasAcceptedByHistoryFilterParentAttribute = true;
        }
    }

    if (hasAcceptedByHistoryFilterParentAttribute &&
        HasStoreScheduledGetter() &&
        RunStoreScheduledGetter(object))
    {
        return true;
    } else if (IsComposite()) {
        if (EtcChild_ &&
            EtcChild_->HasHistoryEnabledAttributeForStoreImpl(
                object,
                hasAcceptedByHistoryFilterParentAttribute))
        {
            return true;
        }
        for (const auto& [key, child] : KeyToChild_) {
            if (child->HasHistoryEnabledAttributeForStoreImpl(
                    object,
                    hasAcceptedByHistoryFilterParentAttribute))
            {
                return true;
            }
        }
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

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
                expr = context->New<TFunctionExpression>(
                    TSourceLocation(),
                    "try_get_any",
                    TExpressionList{
                        std::move(expr),
                        context->New<TLiteralExpression>(
                            TSourceLocation(),
                            path)
                    });
            }

            return expr;
        };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
