#include "attribute_schema.h"
#include "type_handler.h"
#include "helpers.h"

#include <yt/core/ypath/tokenizer.h>

#include <yt/core/ytree/fluent.h>

namespace NYP {
namespace NServer {
namespace NObjects {

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

    Setter_ =
        [=] (
            const TTransactionPtr& transaction,
            TObject* object,
            const TYPath& path,
            const INodePtr& value,
            bool recursive)
        {
            auto* attribute = &object->Annotations();

            NYPath::TTokenizer tokenizer(path);

            if (tokenizer.Advance() == ETokenType::EndOfStream) {
                for (const auto& pair : attribute->LoadAll()) {
                    attribute->Store(pair.first, Null);
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
                    auto maybeExistingYson = attribute->Load(key);
                    if (maybeExistingYson) {
                        try {
                            existingNode = ConvertToNode(*maybeExistingYson);
                        } catch (const std::exception& ex) {
                            THROW_ERROR_EXCEPTION("Error parsing value of annotation %Qv of object %Qv",
                                key,
                                object->GetId())
                                    << ex;
                        }
                    } else {
                        if (!recursive) {
                            THROW_ERROR_EXCEPTION("%v %Qv has no annotation %Qv",
                                GetCapitalizedHumanReadableTypeName(object->GetType()),
                                object->GetId(),
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

    Remover_ =
        [=] (
            const TTransactionPtr& transaction,
            TObject* object,
            const TYPath& path)
        {
            TTokenizer tokenizer(path);

            if (tokenizer.Advance() == ETokenType::EndOfStream) {
                THROW_ERROR_EXCEPTION("Attribute %v cannot be removed",
                    GetPath());
            }
            tokenizer.Expect(ETokenType::Slash);

            tokenizer.Advance();
            tokenizer.Expect(ETokenType::Literal);
            auto key = tokenizer.GetLiteralValue();

            auto* attribute = &object->Annotations();

            TNullable<TYsonString> maybeUpdatedYson;
            if (tokenizer.Advance() != ETokenType::EndOfStream) {
                auto maybeExistingYson = attribute->Load(key);
                if (!maybeExistingYson) {
                    THROW_ERROR_EXCEPTION("%v %Qv has no annotation %Qv",
                        GetCapitalizedHumanReadableTypeName(object->GetType()),
                        object->GetId(),
                        key);
                }

                INodePtr existingNode;
                try {
                    existingNode = ConvertToNode(*maybeExistingYson);
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Error parsing value of annotation %Qv of %v %Qv",
                        key,
                        GetLowercaseHumanReadableTypeName(object->GetType()),
                        object->GetId())
                        << ex;
                }

                // TODO(babenko): optimize
                SyncYPathRemove(existingNode, TYPath(tokenizer.GetInput()));
                maybeUpdatedYson = ConvertToYsonString(existingNode);
            }

            attribute->Store(key, maybeUpdatedYson);
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
            const TTransactionPtr& transaction,
            TObject* object)
        {
            object->Annotations().ScheduleLoadAll();
        };

    Evaluator_ =
        [=] (
            const TTransactionPtr& transaction,
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

TAttributeSchema* TAttributeSchema::SetParentAttribute()
{
    InitExpressionBuilder(TypeHandler_->GetParentIdField());
    return this;
}

TAttributeSchema* TAttributeSchema::SetControlAttribute()
{
    Opaque_ = true;
    return this;
}

TAttributeSchema::TAttributeSchema(
    IObjectTypeHandler* typeHandler,
    const TString& name)
    : TypeHandler_(typeHandler)
    , Name_(name)
{ }

bool TAttributeSchema::IsComposite() const
{
    return Composite_;
}

bool TAttributeSchema::IsOpaque() const
{
    return Opaque_;
}

bool TAttributeSchema::IsAnnotationsAttribute() const
{
    return Annotations_;
}

const TString& TAttributeSchema::GetName() const
{
    return Name_;
}

TString TAttributeSchema::GetPath() const
{
    SmallVector<const TAttributeSchema*, 4> parents;
    const auto* current = this;
    while (current->GetParent()) {
        if (!current->IsFallback()) {
            parents.push_back(current);
        }
        current = current->GetParent();
    }
    if (parents.empty()) {
        return "/";
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
    YCHECK(!Parent_);
    Parent_ = parent;
}

TAttributeSchema* TAttributeSchema::SetComposite()
{
    Composite_ = true;
    return this;
}

void TAttributeSchema::AddChild(TAttributeSchema* child)
{
    Composite_ = true;
    child->SetParent(this);
    if (child->IsFallback()) {
        YCHECK(!FallbackChild_);
        FallbackChild_ = child;
    } else {
        YCHECK(KeyToChild_.emplace(child->GetName(), child).second);
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

TAttributeSchema* TAttributeSchema::FindFallbackChild() const
{
    return FallbackChild_;
}

TAttributeSchema* TAttributeSchema::GetChildOrThrow(const TString& key) const
{
    auto* child = FindChild(key);
    if (!child) {
        THROW_ERROR_EXCEPTION("Attribute %v has no child with key %Qv",
            GetPath(),
            key);
    }
    return child;
}

const THashMap<TString, TAttributeSchema*>& TAttributeSchema::KeyToChild() const
{
    return KeyToChild_;
}

bool TAttributeSchema::HasSetter() const
{
    return Setter_.operator bool();
}

void TAttributeSchema::RunSetter(
    const TTransactionPtr& transaction,
    TObject* object,
    const TYPath& path,
    const INodePtr& value,
    bool recursive)
{
    Setter_(transaction, object, path, value, recursive);
}

bool TAttributeSchema::HasInitializer() const
{
    return Initializer_.operator bool();
}

void TAttributeSchema::RunInitializer(
    const TTransactionPtr& transaction,
    TObject* object)
{
    Initializer_(transaction, object);
}

void TAttributeSchema::RunUpdateHandlers(
    const TTransactionPtr& transaction,
    TObject* object)
{
    for (const auto& handler : UpdateHandlers_) {
        handler(transaction, object);
    }
}

void TAttributeSchema::RunValidators(
    const TTransactionPtr& transaction,
    TObject* object)
{
    for (const auto& validator : Validators_) {
        validator(transaction, object);
    }
}

bool TAttributeSchema::HasRemover() const
{
    return Remover_.operator bool();
}

void TAttributeSchema::RunRemover(const TTransactionPtr& transaction, TObject* object, const TYPath& path)
{
    Remover_(transaction, object, path);
}

bool TAttributeSchema::HasPreloader() const
{
    return Preloader_.operator bool();
}

void TAttributeSchema::RunPreloader(
    const TTransactionPtr& transaction,
    TObject* object,
    const TUpdateRequest& request)
{
    Preloader_(transaction, object, request);
}

TAttributeSchema* TAttributeSchema::SetExpressionBuilder(std::function<TExpressionPtr(IQueryContext*)> builder)
{
    ExpressionBuilder_ =
        [=] (IQueryContext* context, const TYPath& path) {
            if (!path.empty()) {
                THROW_ERROR_EXCEPTION("Attribute %v can only be queried as a whole",
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

void TAttributeSchema::RunPreevaluator(const TTransactionPtr& transaction, TObject* object)
{
    Preevaluator_(transaction, object);
}

bool TAttributeSchema::HasEvaluator() const
{
    return Evaluator_.operator bool();
}

void TAttributeSchema::RunEvaluator(
    const TTransactionPtr& transaction,
    TObject* object,
    NYson::IYsonConsumer* consumer)
{
    Evaluator_(transaction, object, consumer);
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

TAttributeSchema* TAttributeSchema::SetFallback()
{
    Fallback_ = true;
    return this;
}

bool TAttributeSchema::IsFallback() const
{
    return Fallback_;
}

void TAttributeSchema::InitExpressionBuilder(
    const TDBField* field,
    const char* udfFormatter)
{
    ExpressionBuilder_ =
        [=] (
            IQueryContext* context,
            const TYPath& path)
        {
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

            if (udfFormatter) {
                expr = New<TFunctionExpression>(
                    TSourceLocation(),
                    udfFormatter,
                    TExpressionList{std::move(expr)});
            }

            return expr;
        };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP

