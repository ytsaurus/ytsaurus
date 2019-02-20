#include "helpers.h"
#include "transaction.h"
#include "type_handler.h"
#include "attribute_schema.h"
#include "db_schema.h"
#include "pod.h"
#include "node.h"

#include <yt/ytlib/query_client/ast.h>
#include <yt/ytlib/query_client/query_preparer.h>
#include <yt/ytlib/query_client/query_common.h>

#include <yt/core/ypath/tokenizer.h>

#include <yt/core/yson/writer.h>
#include <yt/core/yson/forwarding_consumer.h>

#include <yt/core/ytree/ypath_client.h>

namespace NYP::NServer::NObjects {

using namespace NAccessControl;

using namespace NYT::NYPath;
using namespace NYT::NYson;
using namespace NYT::NQueryClient::NAst;
using namespace NYT::NTableClient;

using NYT::NQueryClient::TSourceLocation;

////////////////////////////////////////////////////////////////////////////////

TResolveResult ResolveAttribute(
    IObjectTypeHandler* typeHandler,
    const TYPath& path,
    TResolvePermissions* permissions)
{
    NYPath::TTokenizer tokenizer(path);

    auto addReadPermission = [&] (TAttributeSchema* attribute) {
        auto permission = attribute->GetReadPermission();
        if (permission == EAccessControlPermission::None) {
            return;
        }
        if (!permissions) {
            THROW_ERROR_EXCEPTION("Attribute %v cannot be referenced since it requires special read permission %Qlv",
                attribute->GetPath(),
                permission);
        }
        auto& readPermissions = permissions->ReadPermissions;
        if (std::find(readPermissions.begin(), readPermissions.end(), permission) == readPermissions.end()) {
            readPermissions.push_back(permission);
        }
    };

    std::vector<EAccessControlPermission> readPermissions;
    auto* current = typeHandler->GetRootAttributeSchema();
    try {
        while (true) {
            addReadPermission(current);
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

void ValidateHasExpressionBuilder(TAttributeSchema *attribute)
{
    if (!attribute->HasExpressionBuilder()) {
        THROW_ERROR_EXCEPTION("Attribute %v cannot be queried",
            attribute->GetPath());
    }
}

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
        ValidateHasExpressionBuilder(child);
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
    ValidateHasExpressionBuilder(attribute);
    return attribute->RunExpressionBuilder(context, path);
}

TExpressionPtr BuildAttributeSelector(
    IQueryContext* context,
    const TYPath& path)
{
    auto resolveResult = ResolveAttribute(context->GetTypeHandler(), path);
    return BuildSelector(context, resolveResult.Attribute, resolveResult.SuffixPath);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TAttributeFetcherContext::TAttributeFetcherContext(IQueryContext* queryContext)
    : QueryContext_(queryContext)
{ }

void TAttributeFetcherContext::AddSelectExpression(TExpressionPtr expr)
{
    SelectExprs_.push_back(std::move(expr));
}

const TExpressionList& TAttributeFetcherContext::GetSelectExpressions() const
{
    return SelectExprs_;
}

void TAttributeFetcherContext::WillNeedObject()
{
    if (ObjectIdIndex_ >= 0) {
        return;
    }
    ObjectIdIndex_ = static_cast<int>(SelectExprs_.size());
    auto* typeHandler = QueryContext_->GetTypeHandler();
    SelectExprs_.push_back(QueryContext_->GetFieldExpression(typeHandler->GetIdField()));
    if (typeHandler->GetParentType() != EObjectType::Null) {
        ParentIdIndex_ = static_cast<int>(SelectExprs_.size());
        SelectExprs_.push_back(QueryContext_->GetFieldExpression(typeHandler->GetParentIdField()));
    }
}

TObject* TAttributeFetcherContext::GetObject(
    TTransaction* transaction,
    TUnversionedRow row) const
{
    YCHECK(ObjectIdIndex_ >= 0);
    auto* typeHandler = QueryContext_->GetTypeHandler();
    auto objectId = FromUnversionedValue<TObjectId>(row[ObjectIdIndex_]);
    auto parentId = typeHandler->GetParentType() == EObjectType::Null
        ? TObjectId()
        : FromUnversionedValue<TObjectId>(row[ParentIdIndex_]);
    return transaction->GetObject(typeHandler->GetType(), objectId, parentId);
}

TUnversionedValue TAttributeFetcherContext::RetrieveNextValue(TUnversionedRow row, int* currentIndex) const
{
    if (*currentIndex == ObjectIdIndex_) {
        ++(*currentIndex);
    }
    if (*currentIndex == ParentIdIndex_) {
        ++(*currentIndex);
    }
    return row[(*currentIndex)++];
}

////////////////////////////////////////////////////////////////////////////////

TAttributeFetcher::TAttributeFetcher(
    const TResolveResult& resolveResult,
    TTransaction* transaction,
    TAttributeFetcherContext* fetcherContext,
    IQueryContext* queryContext)
    : RootResolveResult_(resolveResult)
    , Transaction_(std::move(transaction))
    , FetcherContext_(fetcherContext)
    , StartIndex_(static_cast<int>(fetcherContext->GetSelectExpressions().size()))
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
            FetcherContext_->AddSelectExpression(std::move(expr));
            break;
        }

        case EAttributeFetchMethod::Evaluator:
            FetcherContext_->WillNeedObject();
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
                auto* object = FetcherContext_->GetObject(Transaction_, row);
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

                    virtual void OnMyEntity() override
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
            const auto& value = FetcherContext_->RetrieveNextValue(row, &CurrentIndex_);
            UnversionedValueToYson(value, consumer);
            break;
        }

        case EAttributeFetchMethod::Evaluator: {
            auto* object = FetcherContext_->GetObject(Transaction_, row);
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
            break;
        }

        default:
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

TExpressionPtr BuildFilterExpression(
    IQueryContext* context,
    const TObjectFilter& filter)
{
    class TQueryRewriter
    {
    public:
        explicit TQueryRewriter(IQueryContext* context)
            : Context_(context)
        { }

        TExpressionPtr Run(const TExpressionPtr& expr)
        {
            TExpressionPtr expr_(expr);
            Visit(&expr_);
            return expr_;
        }

    private:
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
            } else if (auto* typedExpr = (*expr)->As<TBetweenExpression>()) {
                Visit(typedExpr->Expr);
            } else if (auto* typedExpr = (*expr)->As<TTransformExpression>()) {
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
            *expr = BuildAttributeSelector(Context_, ref.ColumnName);
        }
    } rewriter(context);

    auto parsedQuery = NQueryClient::ParseSource(filter.Query, NQueryClient::EParseMode::Expression);
    const auto& queryExpr = std::get<TExpressionPtr>(parsedQuery->AstHead.Ast);
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
        case EObjectType::Schema:
            return AsStringBuf("Schema");
        case EObjectType::Node:
            return AsStringBuf("Node");
        case EObjectType::PodSet:
            return AsStringBuf("Pod set");
        case EObjectType::Pod:
            return AsStringBuf("Pod");
        case EObjectType::Resource:
            return AsStringBuf("Resource");
        case EObjectType::ResourceCache:
            return AsStringBuf("Resource cache");
        case EObjectType::DynamicResource:
            return AsStringBuf("Dynamic resource");
        case EObjectType::NetworkProject:
            return AsStringBuf("Network project");
        case EObjectType::ReplicaSet:
            return AsStringBuf("Replica set");
        case EObjectType::Endpoint:
            return AsStringBuf("Endpoint");
        case EObjectType::EndpointSet:
            return AsStringBuf("Endpoint set");
        case EObjectType::NodeSegment:
            return AsStringBuf("Node segment");
        case EObjectType::VirtualService:
            return AsStringBuf("Virtual service");
        case EObjectType::User:
            return AsStringBuf("User");
        case EObjectType::Group:
            return AsStringBuf("Group");
        case EObjectType::InternetAddress:
            return AsStringBuf("Internet address");
        case EObjectType::Account:
            return AsStringBuf("Account");
        case EObjectType::DnsRecordSet:
            return AsStringBuf("DNS record set");
        case EObjectType::NetworkModule:
            return AsStringBuf("Network module");
        default:
            Y_UNREACHABLE();
    }
}

TStringBuf GetLowercaseHumanReadableTypeName(EObjectType type)
{
    switch (type) {
        case EObjectType::Schema:
            return AsStringBuf("schema");
        case EObjectType::Node:
            return AsStringBuf("node");
        case EObjectType::PodSet:
            return AsStringBuf("pod set");
        case EObjectType::Pod:
            return AsStringBuf("pod");
        case EObjectType::Resource:
            return AsStringBuf("resource");
        case EObjectType::ResourceCache:
            return AsStringBuf("resource cache");
        case EObjectType::DynamicResource:
            return AsStringBuf("dynamic resource");
        case EObjectType::NetworkProject:
            return AsStringBuf("network project");
        case EObjectType::ReplicaSet:
            return AsStringBuf("replica set");
        case EObjectType::Endpoint:
            return AsStringBuf("endpoint");
        case EObjectType::EndpointSet:
            return AsStringBuf("endpoint set");
        case EObjectType::NodeSegment:
            return AsStringBuf("node segment");
        case EObjectType::VirtualService:
            return AsStringBuf("virtual service");
        case EObjectType::User:
            return AsStringBuf("user");
        case EObjectType::Group:
            return AsStringBuf("group");
        case EObjectType::InternetAddress:
            return AsStringBuf("internet address");
        case EObjectType::Account:
            return AsStringBuf("account");
        case EObjectType::DnsRecordSet:
            return AsStringBuf("DNS record set");
        case EObjectType::NetworkModule:
            return AsStringBuf("network module");
        default:
            Y_UNREACHABLE();
    }
}

TString GetObjectDisplayName(const TObject* object)
{
    return object->MetaOther().Load().has_name()
        ? Format("%Qv (id %Qv)", object->MetaOther().Load().name(), object->GetId())
        : Format("%Qv", object->GetId());
}

////////////////////////////////////////////////////////////////////////////////

TObjectId GenerateUuid()
{
    return ToString(TGuid::Create());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

