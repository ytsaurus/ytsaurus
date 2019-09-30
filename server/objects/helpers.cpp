#include "helpers.h"

#include "attribute_schema.h"
#include "db_schema.h"
#include "group.h"
#include "node.h"
#include "pod.h"
#include "transaction.h"
#include "type_handler.h"
#include "user.h"

#include <yp/server/lib/objects/object_filter.h>
#include <yp/server/lib/query_helpers/query_rewriter.h>

#include <yt/ytlib/query_client/ast.h>
#include <yt/ytlib/query_client/query_preparer.h>
#include <yt/ytlib/query_client/query_common.h>

#include <yt/core/ypath/tokenizer.h>

#include <yt/core/yson/writer.h>
#include <yt/core/yson/forwarding_consumer.h>

#include <yt/core/ytree/ypath_client.h>

#include <util/string/hex.h>

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

    auto* current = typeHandler->GetRootAttributeSchema();
    try {
        NYson::TResolveProtobufElementByYPathOptions options;

        while (true) {
            addReadPermission(current);
            if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
                break;
            }
            if (current->IsExtensible()) {
                options.AllowUnknownYsonFields = true;
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
                auto* etcChild = current->FindEtcChild();
                if (etcChild) {
                    addReadPermission(etcChild);
                    return {etcChild, TYPath(remainingPath)};
                } else {
                    THROW_ERROR_EXCEPTION("Attribute %v has no child with key %Qv",
                        current->GetPath(),
                        key);
                }
            }
        }
        // Validate path in ProtoBuf schema.
        NYson::ResolveProtobufElementByYPath(typeHandler->GetRootProtobufType(), path, options);
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
    for (const auto& [key, childAttribute] : attribute->KeyToChild()) {
        if (!childAttribute->IsOpaque() && !childAttribute->IsControl()) {
            args.push_back(New<TLiteralExpression>(TSourceLocation(), key));
            ValidateHasExpressionBuilder(childAttribute);
            args.push_back(BuildSelector(context, childAttribute, TYPath()));
        }
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
        YT_VERIFY(path.empty());
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
{
    auto* typeHandler = QueryContext_->GetTypeHandler();
    ObjectIdIndex_ = RegisterField(typeHandler->GetIdField());
    if (typeHandler->GetParentType() != EObjectType::Null) {
        ParentIdIndex_ = RegisterField(typeHandler->GetParentIdField());
    }
}

int TAttributeFetcherContext::RegisterField(const TDBField* field)
{
    SelectExprs_.push_back(QueryContext_->GetFieldExpression(field));
    return static_cast<int>(SelectExprs_.size()) - 1;
}

void TAttributeFetcherContext::AddSelectExpression(TExpressionPtr expr)
{
    SelectExprs_.push_back(std::move(expr));
}

const TExpressionList& TAttributeFetcherContext::GetSelectExpressions() const
{
    return SelectExprs_;
}

TObjectId TAttributeFetcherContext::GetObjectId(TUnversionedRow row) const
{
    return FromUnversionedValue<TObjectId>(row[ObjectIdIndex_]);
}

TObjectId TAttributeFetcherContext::GetParentId(TUnversionedRow row) const
{
    auto* typeHandler = QueryContext_->GetTypeHandler();
    auto objectId = FromUnversionedValue<TObjectId>(row[ObjectIdIndex_]);
    return typeHandler->GetParentType() == EObjectType::Null
        ? TObjectId()
        : FromUnversionedValue<TObjectId>(row[ParentIdIndex_]);
}

TObject* TAttributeFetcherContext::GetObject(
    TTransaction* transaction,
    TUnversionedRow row) const
{
    auto* typeHandler = QueryContext_->GetTypeHandler();
    return transaction->GetObject(
        typeHandler->GetType(),
        GetObjectId(row),
        GetParentId(row));
}

std::vector<TObject*> TAttributeFetcherContext::GetObjects(
    TTransaction* transaction,
    TRange<TUnversionedRow> rows) const
{
    std::vector<TObject*> objects;
    objects.reserve(rows.size());
    for (auto row : rows) {
        objects.push_back(GetObject(transaction, row));
    }
    return objects;
}

TUnversionedValue TAttributeFetcherContext::RetrieveNextValue(TUnversionedRow row, int* currentIndex) const
{
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
            for (const auto& [key, childAttribute] : attribute->KeyToChild()) {
                if (!childAttribute->IsOpaque() && !childAttribute->IsControl()) {
                    DoPrepare({childAttribute, {}}, queryContext);
                }
            }

            auto* etcChild = attribute->FindEtcChild();
            if (etcChild) {
                DoPrepare({etcChild, {}}, queryContext);
            }
            break;
        }

        case EAttributeFetchMethod::ExpressionBuilder: {
            auto expr = attribute->RunExpressionBuilder(queryContext, resolveResult.SuffixPath);
            FetcherContext_->AddSelectExpression(std::move(expr));
            break;
        }

        case EAttributeFetchMethod::Evaluator:
            break;

        default:
            YT_ABORT();
    }
}

void TAttributeFetcher::DoPrefetch(
    TUnversionedRow row,
    const TResolveResult& resolveResult)
{
    auto* attribute = resolveResult.Attribute;
    switch (GetFetchMethod(resolveResult)) {
        case EAttributeFetchMethod::Composite: {
            for (const auto& [key, childAttribute] : attribute->KeyToChild()) {
                if (!childAttribute->IsOpaque() && !childAttribute->IsControl()) {
                    DoPrefetch(row, {childAttribute, {}});
                }
            }

            auto* etcChild = attribute->FindEtcChild();
            if (etcChild) {
                DoPrefetch(row, {etcChild, {}});
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
            YT_ABORT();
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
            for (const auto& [key, childAttribute] : attribute->KeyToChild()) {
                if (childAttribute->IsControl()) {
                    continue;
                }
                consumer->OnKeyedItem(key);
                if (childAttribute->IsOpaque()) {
                    consumer->OnEntity();
                } else {
                    DoFetch(row, {childAttribute, {}}, consumer);
                }
            }

            auto* etcChild = attribute->FindEtcChild();
            if (etcChild) {
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
                DoFetch(row, {etcChild, {}}, &unwrappingConsumer);
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
                auto finalNode = NYTree::FindNodeByYPath(valueNode, resolveResult.SuffixPath);
                if (finalNode) {
                    NYTree::VisitTree(finalNode, consumer, false);
                } else {
                    consumer->OnEntity();
                }
            }
            break;
        }

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

TExpressionPtr BuildFilterExpression(
    IQueryContext* context,
    const TObjectFilter& filter)
{
    auto parsedQuery = NQueryClient::ParseSource(filter.Query, NQueryClient::EParseMode::Expression);
    const auto& queryExpr = std::get<TExpressionPtr>(parsedQuery->AstHead.Ast);

    auto referenceMapping = [context] (const TReference& ref) {
        if (ref.TableName) {
            THROW_ERROR_EXCEPTION("Table references are not supported");
        }
        return BuildAttributeSelector(context, ref.ColumnName);
    };
    NQueryHelpers::TQueryRewriter rewriter(std::move(referenceMapping));

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

TString GetObjectDisplayName(const TObject* object)
{
    return object->MetaEtc().Load().has_name()
        ? Format("%Qv (id %Qv)", object->MetaEtc().Load().name(), object->GetId())
        : Format("%Qv", object->GetId());
}

////////////////////////////////////////////////////////////////////////////////

TObjectId GenerateUuid()
{
    return ToString(TGuid::Create());
}

////////////////////////////////////////////////////////////////////////////////

void ValidateSubjectExists(TTransaction* transaction, const TObjectId& subjectId)
{
    auto* user = transaction->GetUser(subjectId);
    if (user && user->DoesExist()) {
        return;
    }
    auto* group = transaction->GetGroup(subjectId);
    if (group && group->DoesExist()) {
        return;
    }
    THROW_ERROR_EXCEPTION(
        NClient::NApi::EErrorCode::NoSuchObject,
        "Subject %Qv does not exist",
        subjectId)
        << TErrorAttribute("object_id", subjectId);
}

////////////////////////////////////////////////////////////////////////////////

TTimestamp GetBarrierTimestamp(const std::vector<NYT::NApi::TTabletInfo>& tabletInfos)
{
    YT_VERIFY(!tabletInfos.empty());
    auto result = tabletInfos[0].BarrierTimestamp;
    for (size_t i = 1; i < tabletInfos.size(); ++i) {
        result = std::min(result, tabletInfos[i].BarrierTimestamp);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
