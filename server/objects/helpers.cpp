#include "helpers.h"
#include "transaction.h"
#include "type_handler.h"
#include "attribute_schema.h"
#include "db_schema.h"

#include <yt/ytlib/query_client/ast.h>
#include <yt/ytlib/query_client/query_preparer.h>
#include <yt/ytlib/query_client/query_common.h>

#include <yt/core/ypath/tokenizer.h>

#include <yt/core/yson/writer.h>
#include <yt/core/yson/forwarding_consumer.h>

#include <yt/core/ytree/ypath_client.h>

namespace NYP {
namespace NServer {
namespace NObjects {

using namespace NYT::NYPath;
using namespace NYT::NYson;
using namespace NYT::NQueryClient::NAst;
using namespace NYT::NTableClient;

using NYT::NQueryClient::TSourceLocation;

////////////////////////////////////////////////////////////////////////////////

TResolveResult ResolveAttribute(
    IObjectTypeHandler* typeHandler,
    const TYPath& path)
{
    NYPath::TTokenizer tokenizer(path);

    auto* current = typeHandler->GetRootAttributeSchema();
    try {
        while (true) {
            if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
                break;
            }
            if (!current->IsComposite()) {
                break;
            }
            auto remainingPath = tokenizer.GetInput();
            tokenizer.Expect(NYPath::ETokenType::Slash);
            tokenizer.Advance();
            tokenizer.Expect(NYPath::ETokenType::Literal);
            const auto& key = tokenizer.GetLiteralValue();
            auto* child = current->FindChild(key);
            if (child) {
                current = child;
            } else {
                auto* fallbackChild = current->FindFallbackChild();
                if (fallbackChild) {
                    return {fallbackChild, TYPath(remainingPath)};
                } else {
                    THROW_ERROR_EXCEPTION("Attribute %v has no child with key %Qv",
                        current->GetPath(),
                        key);
                }
            }
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing attribute path %v",
            path)
            << ex;
    }

    return {current, TYPath(tokenizer.GetInput())};
}

namespace {

TExpressionPtr BuildSelector(
    IQueryContext* context,
    TAttributeSchema* attribute,
    const TYPath& path);

TExpressionPtr BuildCompositeGetter(
    IQueryContext* context,
    TAttributeSchema* attribute)
{
    TExpressionList args;
    for (const auto& pair : attribute->KeyToChild()) {
        const auto& key = pair.first;
        auto* child = pair.second;
        args.push_back(New<TLiteralExpression>(TSourceLocation(), key));
        if (!child->HasExpressionBuilder()) {
            THROW_ERROR_EXCEPTION("Attribute %v cannot be queried",
                child->GetPath());
        }
        args.push_back(BuildSelector(context, child, TYPath()));
    }

    static const TString MakeMapName("make_map");
    return New<TFunctionExpression>(
        TSourceLocation(),
        MakeMapName,
        std::move(args));
}

TExpressionPtr BuildSelector(
    IQueryContext* context,
    TAttributeSchema* attribute,
    const TYPath& path)
{
    if (attribute->IsComposite()) {
        YCHECK(path.empty());
        return BuildCompositeGetter(context, attribute);
    }

    if (!attribute->HasExpressionBuilder()) {
        THROW_ERROR_EXCEPTION("Attribute %v cannot be queried",
            attribute->GetPath());
    }
    return attribute->RunExpressionBuilder(context, path);
}

TExpressionPtr BuildAttributeSelector(
    IObjectTypeHandler* typeHandler,
    IQueryContext* context,
    const TYPath& path)
{
    auto resolveResult = ResolveAttribute(typeHandler, path);
    return BuildSelector(context, resolveResult.Attribute, resolveResult.SuffixPath);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TAttributeFetcher::TAttributeFetcher(
    IObjectTypeHandler* typeHandler,
    const TResolveResult& resolveResult,
    TTransactionPtr transaction,
    TAttributeFetcherContext* fetcherContext,
    IQueryContext* queryContext)
    : TypeHandler_(typeHandler)
    , RootResolveResult_(resolveResult)
    , Transaction_(std::move(transaction))
    , FetcherContext_(fetcherContext)
    , StartIndex_(static_cast<int>(fetcherContext->SelectExprs.size()))
{
    DoPrepare(RootResolveResult_, queryContext);
}

void TAttributeFetcher::Prefetch(TUnversionedRow row)
{
    DoPrefetch(row, RootResolveResult_);
}

TYsonString TAttributeFetcher::Fetch(TUnversionedRow row)
{
    TString valueYson;
    TStringOutput valueOutput(valueYson);
    TYsonWriter valueWriter(&valueOutput);
    CurrentIndex_ = StartIndex_;
    DoFetch(row, RootResolveResult_, &valueWriter);
    return TYsonString(std::move(valueYson));
}

EAttributeFetchMethod TAttributeFetcher::GetFetchMethod(const TResolveResult& resolveResult)
{
    auto* attribute = resolveResult.Attribute;
    if (attribute->IsComposite()) {
        return EAttributeFetchMethod::Composite;
    }
    if (attribute->IsAnnotationsAttribute() && resolveResult.SuffixPath.empty()) {
        return EAttributeFetchMethod::Evaluator;
    }
    if (attribute->HasExpressionBuilder()) {
        return EAttributeFetchMethod::ExpressionBuilder;
    }
    if (attribute->HasEvaluator()) {
        return EAttributeFetchMethod::Evaluator;
    }
    THROW_ERROR_EXCEPTION("Attribute %v cannot be fetched",
        attribute->GetPath());
}

void TAttributeFetcher::DoPrepare(
    const TResolveResult& resolveResult,
    IQueryContext* queryContext)
{
    auto* attribute = resolveResult.Attribute;
    switch (GetFetchMethod(resolveResult)) {
        case EAttributeFetchMethod::Composite: {
            for (const auto& pair : attribute->KeyToChild()) {
                auto* childAttribute = pair.second;
                if (!childAttribute->IsOpaque()) {
                    DoPrepare({childAttribute, {}}, queryContext);
                }
            }

            auto* fallbackChild = attribute->FindFallbackChild();
            if (fallbackChild) {
                DoPrepare({fallbackChild, {}}, queryContext);
            }
            break;
        }

        case EAttributeFetchMethod::ExpressionBuilder: {
            auto expr = attribute->RunExpressionBuilder(queryContext, resolveResult.SuffixPath);
            FetcherContext_->SelectExprs.push_back(std::move(expr));
            break;
        }

        case EAttributeFetchMethod::Evaluator:
            if (FetcherContext_->ObjectIdIndex < 0) {
                FetcherContext_->ObjectIdIndex = static_cast<int>(FetcherContext_->SelectExprs.size());
                FetcherContext_->SelectExprs.push_back(queryContext->GetFieldExpression(TypeHandler_->GetIdField()));
                if (TypeHandler_->GetParentType() != EObjectType::Null) {
                    FetcherContext_->ParentIdIndex = static_cast<int>(FetcherContext_->SelectExprs.size());
                    FetcherContext_->SelectExprs.push_back(queryContext->GetFieldExpression(TypeHandler_->GetParentIdField()));
                }
            }
            break;

        default:
            Y_UNREACHABLE();
    }
}

void TAttributeFetcher::DoPrefetch(
    TUnversionedRow row,
    const TResolveResult& resolveResult)
{
    auto* attribute = resolveResult.Attribute;
    switch (GetFetchMethod(resolveResult)) {
        case EAttributeFetchMethod::Composite: {
            for (const auto& pair : attribute->KeyToChild()) {
                auto* childAttribute = pair.second;
                if (!childAttribute->IsOpaque()) {
                    DoPrefetch(row, {childAttribute, {}});
                }
            }

            auto* fallbackChild = attribute->FindFallbackChild();
            if (fallbackChild) {
                DoPrefetch(row, {fallbackChild, {}});
            }
            break;
        }

        case EAttributeFetchMethod::ExpressionBuilder:
            break;

        case EAttributeFetchMethod::Evaluator:
            if (attribute->HasPreevaluator()) {
                auto objectId = FromDbValue<TObjectId>(row[FetcherContext_->ObjectIdIndex]);
                auto parentId = TypeHandler_->GetParentType() == EObjectType::Null
                    ? TObjectId()
                    : FromDbValue<TObjectId>(row[FetcherContext_->ParentIdIndex]);
                auto* object = Transaction_->GetObject(TypeHandler_->GetType(), objectId, parentId);
                attribute->RunPreevaluator(Transaction_, object);
            }
            break;

        default:
            Y_UNREACHABLE();
    }
}

void TAttributeFetcher::DoFetch(
    TUnversionedRow row,
    const TResolveResult& resolveResult,
    IYsonConsumer* consumer)
{
    auto* attribute = resolveResult.Attribute;
    switch (GetFetchMethod(resolveResult)) {
        case EAttributeFetchMethod::Composite: {
            consumer->OnBeginMap();
            for (const auto& pair : attribute->KeyToChild()) {
                auto* childAttribute = pair.second;
                if (!childAttribute->IsOpaque()) {
                    consumer->OnKeyedItem(pair.first);
                    DoFetch(row, { childAttribute, {}}, consumer);
                }
            }

            auto* fallbackChild = attribute->FindFallbackChild();
            if (fallbackChild) {
                class TUnwrappingConsumer
                    : public TForwardingYsonConsumer
                {
                public:
                    explicit TUnwrappingConsumer(IYsonConsumer* underlying)
                        : Underlying_(underlying)
                    { }

                    virtual void OnMyBeginMap() override
                    {
                        Forward(Underlying_, nullptr, NYson::EYsonType::MapFragment);
                    }

                    virtual void OnMyEndMap() override
                    { }

                private:
                    IYsonConsumer* const Underlying_;
                } unwrappingConsumer(consumer);
                DoFetch(row, {fallbackChild, {}}, &unwrappingConsumer);
            }

            consumer->OnEndMap();
            break;
        }

        case EAttributeFetchMethod::ExpressionBuilder: {
            const auto& value = row[CurrentIndex_++];
            DbValueToYson(value, consumer);
            break;
        }

        case EAttributeFetchMethod::Evaluator: {
            auto objectId = FromDbValue<TObjectId>(row[FetcherContext_->ObjectIdIndex]);
            auto parentId = TypeHandler_->GetParentType() == EObjectType::Null
                ? TObjectId()
                : FromDbValue<TObjectId>(row[FetcherContext_->ParentIdIndex]);
            auto* object = Transaction_->GetObject(TypeHandler_->GetType(), objectId, parentId);
            if (resolveResult.SuffixPath.empty()) {
                attribute->RunEvaluator(Transaction_, object, consumer);
            } else {
                // TODO(babenko): optimize
                TString valueYson;
                TStringOutput valueOutput(valueYson);
                TYsonWriter valueWriter(&valueOutput);
                attribute->RunEvaluator(Transaction_, object, &valueWriter);
                auto valueNode = NYTree::ConvertToNode(TYsonString(std::move(valueYson)));
                consumer->OnRaw(NYTree::SyncYPathGet(valueNode, resolveResult.SuffixPath));
            }
            if (CurrentIndex_ == FetcherContext_->ObjectIdIndex) {
                ++CurrentIndex_;
            }
            if (CurrentIndex_ == FetcherContext_->ParentIdIndex) {
                ++CurrentIndex_;
            }
            break;
        }

        default:
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

TExpressionPtr BuildFilterExpression(
    IObjectTypeHandler* typeHandler,
    IQueryContext* context,
    const TObjectFilter& filter)
{
    class TQueryRewriter
    {
    public:
        TQueryRewriter(
            IObjectTypeHandler* typeHandler,
            IQueryContext* context)
            : TypeHandler_(typeHandler)
            , Context_(context)
        { }

        TExpressionPtr Run(const TExpressionPtr& expr)
        {
            TExpressionPtr expr_(expr);
            Visit(&expr_);
            return expr_;
        }

    private:
        IObjectTypeHandler* const TypeHandler_;
        IQueryContext* const Context_;

        void Visit(TExpressionPtr* expr)
        {
            if ((*expr)->As<TLiteralExpression>()) {
                // Do nothing.
            } else if (auto* typedExpr = (*expr)->As<TReferenceExpression>()) {
                RewriteReference(expr, typedExpr->Reference);
            } else if ((*expr)->As<TAliasExpression>()) {
                // Do nothing.
            } else if (auto* typedExpr = (*expr)->As<TFunctionExpression>()) {
                Visit(typedExpr->Arguments);
            } else if (auto* typedExpr = (*expr)->As<TUnaryOpExpression>()) {
                Visit(typedExpr->Operand);
            } else if (auto* typedExpr = (*expr)->As<TBinaryOpExpression>()) {
                Visit(typedExpr->Lhs);
                Visit(typedExpr->Rhs);
            } else if (auto* typedExpr = (*expr)->As<TInExpression>()) {
                Visit(typedExpr->Expr);
            } else {
                Y_UNREACHABLE();
            }
        }

        void Visit(TExpressionList& list)
        {
            for (auto& expr : list) {
                Visit(&expr);
            }
        }

        void RewriteReference(TExpressionPtr* expr, const TReference& ref)
        {
            if (ref.TableName) {
                THROW_ERROR_EXCEPTION("Table references are not supported");
            }
            *expr = BuildAttributeSelector(TypeHandler_, Context_, ref.ColumnName);
        }
    } rewriter(typeHandler, context);

    auto parsedQuery = NQueryClient::ParseSource(filter.Query, NQueryClient::EParseMode::Expression);
    auto queryExpr = parsedQuery->AstHead.Ast.As<TExpressionPtr>();
    return rewriter.Run(queryExpr);
}

TExpressionPtr BuildAndExpression(
    TExpressionPtr lhs,
    TExpressionPtr rhs)
{
    if (lhs && !rhs) {
        return lhs;
    }
    if (rhs && !lhs) {
        return rhs;
    }
    if (!lhs && !rhs) {
        return New<TLiteralExpression>(TSourceLocation(), TLiteralValue(true));
    }
    return New<TBinaryOpExpression>(
        TSourceLocation(),
        NQueryClient::EBinaryOp::And,
        TExpressionList{ std::move(lhs) },
        TExpressionList{ std::move(rhs) });
}

////////////////////////////////////////////////////////////////////////////////

TStringBuf GetCapitalizedHumanReadableTypeName(EObjectType type)
{
    switch (type) {
        case EObjectType::Node:
            return AsStringBuf("Node");
        case EObjectType::PodSet:
            return AsStringBuf("Pod set");
        case EObjectType::Pod:
            return AsStringBuf("Pod");
        case EObjectType::Resource:
            return AsStringBuf("Resource");
        case EObjectType::NetworkProject:
            return AsStringBuf("Network project");
        case EObjectType::Endpoint:
            return AsStringBuf("Endpoint");
        case EObjectType::EndpointSet:
            return AsStringBuf("Endpoint set");
        case EObjectType::NodeSegment:
            return AsStringBuf("Node segment");
        default:
            Y_UNREACHABLE();
    }
}

TStringBuf GetLowercaseHumanReadableTypeName(EObjectType type)
{
    switch (type) {
        case EObjectType::Node:
            return AsStringBuf("node");
        case EObjectType::PodSet:
            return AsStringBuf("pod set");
        case EObjectType::Pod:
            return AsStringBuf("pod");
        case EObjectType::Resource:
            return AsStringBuf("resource");
        case EObjectType::NetworkProject:
            return AsStringBuf("network project");
        case EObjectType::Endpoint:
            return AsStringBuf("endpoint");
        case EObjectType::EndpointSet:
            return AsStringBuf("endpoint set");
        case EObjectType::NodeSegment:
            return AsStringBuf("node segment");
        default:
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP

