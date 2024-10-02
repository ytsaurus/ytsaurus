#include "helpers.h"

#include "attribute_schema.h"
#include "db_schema.h"
#include "fetchers.h"
#include "object_manager.h"
#include "object_reflection.h"
#include "private.h"
#include "transaction.h"
#include "type_handler.h"

#include <yt/yt/orm/server/master/bootstrap.h>
#include <yt/yt/orm/server/master/yt_connector.h>

#include <yt/yt/orm/library/query/computed_fields_filter.h>
#include <yt/yt/orm/library/query/filter_introspection.h>
#include <yt/yt/orm/library/query/query_rewriter.h>

#include <yt/yt/orm/client/objects/object_filter.h>

#include <yt/yt/library/query/base/ast.h>
#include <yt/yt/library/query/base/query_common.h>
#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/yson/writer.h>
#include <yt/yt/core/yson/ypath_filtering_consumer.h>

#include <yt/yt/core/ytree/ypath_client.h>

#include <util/string/hex.h>

#include <google/protobuf/descriptor.h>

namespace NYT::NOrm::NServer::NObjects {

using namespace NYT::NYPath;
using namespace NYT::NYson;
using namespace NYT::NYTree;
using namespace NYT::NQueryClient::NAst;
using namespace NYT::NTableClient;

using NYT::NQueryClient::TSourceLocation;

using NYT::NYson::EEnumYsonStorageType;

using google::protobuf::FieldDescriptor;

////////////////////////////////////////////////////////////////////////////////

TValuePresentConsumer::TValuePresentConsumer(IYsonConsumer* underlying)
    : Underlying_(underlying)
{ }

bool TValuePresentConsumer::IsValuePresent()
{
    return ValuePresent_;
}

void TValuePresentConsumer::OnStringScalar(TStringBuf value)
{
    Underlying_->OnStringScalar(value);
    ValuePresent_ = true;
}

void TValuePresentConsumer::OnInt64Scalar(i64 value)
{
    Underlying_->OnInt64Scalar(value);
    ValuePresent_ = true;
}

void TValuePresentConsumer::OnUint64Scalar(ui64 scalar)
{
    Underlying_->OnUint64Scalar(scalar);
    ValuePresent_ = true;
}

void TValuePresentConsumer::OnDoubleScalar(double value)
{
    Underlying_->OnDoubleScalar(value);
    ValuePresent_ = true;
}

void TValuePresentConsumer::OnBooleanScalar(bool value)
{
    Underlying_->OnBooleanScalar(value);
    ValuePresent_ = true;
}

void TValuePresentConsumer::OnEntity()
{
    Underlying_->OnEntity();
    ValuePresent_ = true;
}

void TValuePresentConsumer::OnBeginList()
{
    Underlying_->OnBeginList();
    ValuePresent_ = true;
}

void TValuePresentConsumer::OnListItem()
{
    Underlying_->OnListItem();
    ValuePresent_ = true;
}

void TValuePresentConsumer::OnEndList()
{
    Underlying_->OnEndList();
    ValuePresent_ = true;
}

void TValuePresentConsumer::OnBeginMap()
{
    Underlying_->OnBeginMap();
    ValuePresent_ = true;
}

void TValuePresentConsumer::OnKeyedItem(TStringBuf key)
{
    Underlying_->OnKeyedItem(key);
    ValuePresent_ = true;
}

void TValuePresentConsumer::OnEndMap()
{
    Underlying_->OnEndMap();
    ValuePresent_ = true;
}

void TValuePresentConsumer::OnBeginAttributes()
{
    Underlying_->OnBeginAttributes();
    ValuePresent_ = true;
}

void TValuePresentConsumer::OnEndAttributes()
{
    Underlying_->OnEndAttributes();
    ValuePresent_ = true;
}

void TValuePresentConsumer::OnRaw(TStringBuf yson, EYsonType type)
{
    Underlying_->OnRaw(yson, type);
    ValuePresent_ = true;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

void ValidateHasExpressionBuilder(const TAttributeSchema *attribute)
{
    const auto* scalarAttribute = attribute->TryAsScalar();
    THROW_ERROR_EXCEPTION_UNLESS(scalarAttribute && scalarAttribute->HasExpressionBuilder(),
        "Attribute %v cannot be queried",
        attribute->FormatPathEtc());
}

TExpressionPtr BuildSelector(
    IQueryContext* context,
    const TAttributeSchema* attribute,
    const TYPath& path);

TExpressionPtr BuildCompositeGetter(
    IQueryContext* context,
    const TCompositeAttributeSchema* attribute)
{
    TExpressionList args;
    for (const auto& [key, childAttribute] : attribute->KeyToChild()) {
        if (!childAttribute->IsOpaque() && !childAttribute->IsControl()) {
            args.push_back(context->New<TLiteralExpression>(TSourceLocation(), TString(key)));
            ValidateHasExpressionBuilder(childAttribute);
            args.push_back(BuildSelector(context, childAttribute, TYPath()));
        }
    }

    static const TString MakeMapName("make_map");
    return context->New<TFunctionExpression>(
        TSourceLocation(),
        MakeMapName,
        std::move(args));
}

TExpressionPtr BuildSelector(
    IQueryContext* context,
    const TAttributeSchema* attribute,
    const TYPath& path)
{
    if (auto* compositeSchema = attribute->TryAsComposite()) {
        YT_VERIFY(path.empty());
        return BuildCompositeGetter(context, compositeSchema);
    }
    ValidateHasExpressionBuilder(attribute);
    return attribute->AsScalar()->RunExpressionBuilder(context, path, EAttributeExpressionContext::Filter);
}

TExpressionPtr BuildAttributeSelector(
    IQueryContext* context,
    const TYPath& path)
{
    auto resolveResult = ResolveAttributeValidated(context->GetTypeHandler(), path);
    return BuildSelector(context, resolveResult.Attribute, resolveResult.SuffixPath);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

void PrefetchAttributeTimestamp(
    const TResolveAttributeResult& resolveResult,
    const TObject* object)
{
    const auto* attribute = resolveResult.Attribute;
    if (attribute->TryAsComposite()) {
        THROW_ERROR_EXCEPTION_UNLESS(resolveResult.SuffixPath.empty(),
            "Cannot prefetch timestamp for composite attribute %Qv with non-empty suffix path %Qv",
            attribute->GetPath(),
            resolveResult.SuffixPath);
        attribute->ForEachLeafAttribute([object] (const TAttributeSchema* leaf) {
            if (leaf->IsView()) {
                return false;
            }

            PrefetchAttributeTimestamp(TResolveAttributeResult{leaf, TYPath{}}, object);
            return false;
        });
    } else if (auto* scalarAttribute = attribute->AsScalar(); scalarAttribute->HasTimestampPregetter()) {
        scalarAttribute->RunTimestampPregetter(object, resolveResult.SuffixPath);
    }
}

TTimestamp FetchAttributeTimestamp(
    const TResolveAttributeResult& resolveResult,
    const TObject* object)
{
    const auto* attribute = resolveResult.Attribute;
    if (attribute->TryAsComposite()) {
        THROW_ERROR_EXCEPTION_UNLESS(resolveResult.SuffixPath.empty(),
            "Cannot fetch timestamp for composite attribute %Qv with non-empty suffix path %Qv",
            attribute->GetPath(),
            resolveResult.SuffixPath);
        auto result = NullTimestamp;
        attribute->ForEachLeafAttribute([object, &result] (
            const TAttributeSchema* leaf)
        {
            if (leaf->IsView()) {
                return false;
            }
            result = std::max(
                result,
                FetchAttributeTimestamp(TResolveAttributeResult{leaf, TYPath{}}, object));
            return false;
        });
        return result;
    } else if (auto* scalarAttribute = attribute->AsScalar(); scalarAttribute->HasTimestampGetter()) {
        return scalarAttribute->RunTimestampGetter(object, resolveResult.SuffixPath);
    } else {
        return NullTimestamp;
    }
}

////////////////////////////////////////////////////////////////////////////////

NOrm::NQuery::TQueryRewriter MakeQueryRewriter(IQueryContext* context)
{
    auto referenceMapping = [context] (const TReference& ref) {
        if (ref.TableName) {
            THROW_ERROR_EXCEPTION("Table references are not supported");
        }
        return BuildAttributeSelector(context, ref.ColumnName);
    };
    auto functionRewriter = [context] (TFunctionExpression* funcExpr) -> TExpressionPtr
    {
        if ("list_contains" != funcExpr->FunctionName || 2 != funcExpr->Arguments.size()) {
            return nullptr;
        }

        auto* refExpr = funcExpr->Arguments[0]->As<TReferenceExpression>();
        auto* literalExpr = funcExpr->Arguments[1]->As<TLiteralExpression>();
        if (refExpr == nullptr || literalExpr == nullptr) {
            return nullptr;
        }

        auto resolveResult = ResolveAttributeValidated(
            context->GetTypeHandler(),
            refExpr->Reference.ColumnName);

        return resolveResult.Attribute->RunListContainsExpressionBuilder(
            context,
            literalExpr,
            resolveResult.SuffixPath,
            EAttributeExpressionContext::Filter);
    };
    return NOrm::NQuery::TQueryRewriter(context, std::move(referenceMapping), std::move(functionRewriter));
}

TExpressionPtr RewriteExpression(
    TObjectsHolder* holder,
    const TString& expression,
    NOrm::NQuery::TQueryRewriter rewriter)
{
    auto parsedQuery = NQueryClient::ParseSource(expression, NQueryClient::EParseMode::Expression);
    auto queryExpr = std::get<TExpressionPtr>(parsedQuery->AstHead.Ast);
    holder->Merge(std::move(parsedQuery->AstHead));
    return rewriter.Run(queryExpr);
}

TExpressionPtr RewriteExpression(
    IQueryContext* context,
    const TString& expression)
{
    NOrm::NQuery::TQueryRewriter rewriter = MakeQueryRewriter(context);
    return RewriteExpression(context, expression, rewriter);
}

TStringBuilder BuildSelectQuery(
    const NYPath::TYPath& tablePath,
    const TDBFields& fieldsToSelect,
    const TObjectKey& key,
    const TDBFields& keyFields)
{
    TStringBuilder builder;

    {
        TDelimitedStringBuilderWrapper wrapper(&builder, ", ");
        for (const auto* field : fieldsToSelect) {
            wrapper->AppendString(FormatId(field->Name));
        }
    }
    builder.AppendFormat(" from %v where ", FormatId(tablePath));
    {
        YT_VERIFY(keyFields.size() == key.size());
        TDelimitedStringBuilderWrapper wrapper(&builder, " and ");
        for (size_t i = 0; i < keyFields.size(); ++i) {
            wrapper->AppendFormat("%v = %v",
                FormatId(keyFields[i]->Name),
                FormatLiteralValue(key[i].AsLiteralValue()));
        }
    }

    return builder;
}

TFilterResolveResult ResolveFilterExpression(TExpressionPtr filter, IQueryContext* context, bool isComputed)
{
    if (!filter) {
        return {};
    }

    std::vector<TString> fullPaths;
    std::vector<TResolveAttributeResult> resolvedAttributes = CollectAttributes(context, FormatExpression({filter}));

    if (!isComputed) {
        filter = RewriteExpression(context, FormatExpression({filter}));
    }

    fullPaths.reserve(resolvedAttributes.size());
    for (const auto& attribute : resolvedAttributes) {
        fullPaths.push_back(attribute.Attribute->GetPath() + attribute.SuffixPath);
    }

    return {filter, std::move(fullPaths), std::move(resolvedAttributes)};
}

std::pair<TExpressionPtr, TExpressionPtr> BuildFilterExpressions(
    IQueryContext* context,
    const TObjectFilter& filter,
    bool computedFilterEnabled)
{
    auto parsedFilter = NQueryClient::ParseSource(filter.Query, NQueryClient::EParseMode::Expression);
    auto filterExpression = std::get<TExpressionPtr>(parsedFilter->AstHead.Ast);
    context->Merge(std::move(parsedFilter->AstHead));

    auto [nonComputedFilter, computedFilter] = NQuery::SplitFilter(
        context,
        MakeComputedFieldsDetector(context),
        filterExpression);

    THROW_ERROR_EXCEPTION_IF(!computedFilterEnabled && computedFilter,
        "Computed filter is disabled, but used in request with filter %Qv",
        filter.Query);

    return computedFilterEnabled
        ? std::pair<TExpressionPtr, TExpressionPtr>{nonComputedFilter, computedFilter}
        : std::pair<TExpressionPtr, TExpressionPtr>{nonComputedFilter, nullptr};
}

TExpressionList RewriteExpressions(
    IQueryContext* context,
    const std::vector<TString>& expressions)
{
    NOrm::NQuery::TQueryRewriter rewriter = MakeQueryRewriter(context);
    TExpressionList result;
    for (const auto& expression : expressions) {
        result.push_back(RewriteExpression(context, expression, rewriter));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

NYTree::INodePtr RunConsumedValueGetter(
    const TScalarAttributeSchema* schema,
    TTransaction* transaction,
    const TObject* object,
    const NYPath::TYPath& path)
{
    auto consumer = NYTree::CreateBuilderFromFactory(GetEphemeralNodeFactory());
    consumer->BeginTree();
    schema->RunValueGetter(transaction, object, consumer.get(), path);
    return consumer->EndTree();
}

////////////////////////////////////////////////////////////////////////////////

EValueType ProtobufToTableValueType(
    NYson::TProtobufScalarElement::TType type,
    EEnumYsonStorageType enumStorageType)
{
    switch (static_cast<int>(type)) {
        case FieldDescriptor::TYPE_DOUBLE:
        case FieldDescriptor::TYPE_FLOAT:
            return EValueType::Double;
        case FieldDescriptor::TYPE_INT64:
        case FieldDescriptor::TYPE_INT32:
        case FieldDescriptor::TYPE_SFIXED32:
        case FieldDescriptor::TYPE_SFIXED64:
        case FieldDescriptor::TYPE_SINT32:
        case FieldDescriptor::TYPE_SINT64:
            return EValueType::Int64;
        case FieldDescriptor::TYPE_UINT64:
        case FieldDescriptor::TYPE_FIXED32:
        case FieldDescriptor::TYPE_FIXED64:
        case FieldDescriptor::TYPE_UINT32:
            return EValueType::Uint64;
        case FieldDescriptor::TYPE_BOOL:
            return EValueType::Boolean;
        case FieldDescriptor::TYPE_STRING:
        case FieldDescriptor::TYPE_BYTES:
            return EValueType::String;
        case FieldDescriptor::TYPE_ENUM:
            switch (enumStorageType) {
                case EEnumYsonStorageType::Int:
                    return EValueType::Int64;
                case EEnumYsonStorageType::String:
                    return EValueType::String;
            }
    }

    return EValueType::Any;
}

EAttributeType ValueTypeToAttributeType(EValueType valueType)
{
    switch (valueType) {
        case EValueType::Int64:
            return EAttributeType::Int64;
        case EValueType::Uint64:
            return EAttributeType::Uint64;
        case EValueType::Double:
            return EAttributeType::Double;
        case EValueType::Boolean:
            return EAttributeType::Boolean;
        case EValueType::String:
            return EAttributeType::String;
        default:
            return EAttributeType::Any;
    }
}

EValueType AttributeTypeToValueType(EAttributeType attributeType)
{
    switch (attributeType) {
        case EAttributeType::Int64:
            return EValueType::Int64;
        case EAttributeType::Uint64:
            return EValueType::Uint64;
        case EAttributeType::Double:
            return EValueType::Double;
        case EAttributeType::Boolean:
            return EValueType::Boolean;
        case EAttributeType::String:
            return EValueType::String;
        default:
            return EValueType::Any;
    }
}

TString GetYsonExtractFunction(EValueType type)
{
    const static TString TryGetAny("try_get_any");
    const static TString TryGetDouble("try_get_double");
    const static TString TryGetInt64("try_get_int64");
    const static TString TryGetUint64("try_get_uint64");
    const static TString TryGetBoolean("try_get_boolean");
    const static TString TryGetString("try_get_string");

    switch (type) {
        case EValueType::Double:
            return TryGetDouble;
        case EValueType::Int64:
            return TryGetInt64;
        case EValueType::Uint64:
            return TryGetUint64;
        case EValueType::Boolean:
            return TryGetBoolean;
        case EValueType::String:
            return TryGetString;
        case EValueType::Composite:
        case EValueType::Null:
        case EValueType::Any:
            return TryGetAny;
        case EValueType::Min:
        case EValueType::TheBottom:
        case EValueType::Max:
            YT_LOG_FATAL("Cannot provide extract function for invalid value type (Type: %v)",
                type);
    }
}

////////////////////////////////////////////////////////////////////////////////

TFuture<NTabletClient::TTableMountInfoPtr> GetTableMountInfo(NMaster::TYTConnectorPtr ytConnector, const TDBTable* table)
{
    auto client = ytConnector->GetClient(ytConnector->FormatUserTag());
    auto tablePath = ytConnector->GetTablePath(table);
    return client
        ->GetTableMountCache()
        ->GetTableInfo(tablePath);
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

TStringBuf TryGetNameInsideSquareBrackets(TStringBuf data)
{
    YT_ASSERT(data.StartsWith("["));

    int depth = 1;
    auto it = data.begin() + 1;

    while (it != data.end() && depth > 0) {
        if (*it == '[') {
            ++depth;
        }
        if (*it == ']') {
            --depth;
        }
        ++it;
    }

    if (depth > 0) {
        return {};
    }

    return data.substr(1, it - data.begin() - 2);
}

TStringBuf TryGetNameInsideBackticks(TStringBuf data)
{
    YT_ASSERT(data.StartsWith("`"));

    auto it = data.begin() + 1;

    while (it != data.end() && *it != '`') {
        if (*it == '\\') {
            ++it;

            if (it != data.end()) {
                ++it;
            }
        } else {
            ++it;
        }
    }

    if (it == data.end()) {
        return {};
    }

    return data.substr(1, it - data.begin() - 1);
}

TStringBuf TryGetTableNameFromQuery(TStringBuf query)
{
    TStringBuf fromKeyword = " FROM ";

    auto it = std::search(
        query.begin(), query.end(),
        fromKeyword.begin(), fromKeyword.end(),
        [] (char c1, char c2) {
            return std::toupper(c1) == std::toupper(c2);
        });

    if (it == query.end()) {
        return {};
    }

    it += fromKeyword.size();

    while (it != query.end() && *it != '[' && *it != '`') {
        ++it;
    }

    if (it == query.end()) {
        return {};
    }

    if (*it == '[') {
        return TryGetNameInsideSquareBrackets(query.substr(it - query.begin()));
    }

    if (*it == '`') {
        return TryGetNameInsideBackticks(query.substr(it - query.begin()));
    }

    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

void GetConsumingObjectIdentityMeta(
    NYson::IYsonConsumer* consumer,
    TTransaction* transaction,
    const TObject* object)
{
    const auto* typeHandler = object->GetTypeHandler();
    const auto* metaAttributeSchema = typeHandler->GetMetaAttributeSchema();

    consumer->OnBeginMap();

    // Contains uuid.
    if (auto* etc = metaAttributeSchema->GetEtcChild()) {
        if (!etc->HasValueGetter()) {
            THROW_ERROR_EXCEPTION("Error getting object identity meta: /meta/uuid must have initialized value getter");
        }
        NAttributes::TUnwrappingConsumer etcUnwrappingConsumer(consumer);
        auto filteringEtcConsumer = NYson::CreateYPathFilteringConsumer(
            &etcUnwrappingConsumer,
            metaAttributeSchema->GetDisabledEtcFieldPathsToMetaResponse(),
            EYPathFilteringMode::Blacklist);
        etc->RunValueGetter(transaction, object, filteringEtcConsumer.get());
    } else {
        THROW_ERROR_EXCEPTION("Error getting object identity meta: /meta/uuid not found");
    }
    for (const auto& [childKey, childSchema] : metaAttributeSchema->KeyToChild()) {
        if (!childSchema->IsMetaResponseAttribute()) {
            continue;
        }
        if (childSchema->IsOpaque()) {
            THROW_ERROR_EXCEPTION("Error getting object identity meta: /meta/%v cannot be opaque",
                childKey);
        }
        if (childSchema->TryAsComposite()) {
            THROW_ERROR_EXCEPTION("Error getting object identity meta: /meta/%v cannot be composite",
                childKey);
        }

        if (const auto* scalarChildSchema = childSchema->TryAsScalar(); scalarChildSchema &&
            scalarChildSchema->HasValueGetter())
        {
            consumer->OnKeyedItem(childKey);
            scalarChildSchema->RunValueGetter(transaction, object, consumer, TYPath{});
            continue;
        }
        THROW_ERROR_EXCEPTION(
            "Error getting object identity meta: /meta/%v must have initialized value getter",
            childKey);
    }

    consumer->OnEndMap();
}

NYson::TYsonString GetObjectIdentityMeta(
    TTransaction* transaction,
    const TObject* object)
{
    TYsonStringBuilder builder;
    GetConsumingObjectIdentityMeta(builder.GetConsumer(), transaction, object);
    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

TEventTypeValue GetEventTypeValueByNameOrThrow(const TEventTypeName& eventTypeName)
{
    static const THashMap<TEventTypeName, TEventTypeValue> valueByName{
        {NoneEventTypeName, NoneEventTypeValue},
        {ObjectUpdatedEventTypeName, ObjectUpdatedEventTypeValue},
        {ObjectCreatedEventTypeName, ObjectCreatedEventTypeValue},
        {ObjectRemovedEventTypeName, ObjectRemovedEventTypeValue},
    };
    if (auto it = valueByName.find(eventTypeName); it != valueByName.end()) {
        return it->second;
    }
    THROW_ERROR_EXCEPTION("Could not find event type value with name %v", eventTypeName);
}

TEventTypeName GetEventTypeNameByValueOrThrow(TEventTypeValue eventTypeValue)
{
    static const THashMap<TEventTypeValue, TEventTypeName> nameByValue{
        {NoneEventTypeValue, NoneEventTypeName},
        {ObjectUpdatedEventTypeValue, ObjectUpdatedEventTypeName},
        {ObjectCreatedEventTypeValue, ObjectCreatedEventTypeName},
        {ObjectRemovedEventTypeValue, ObjectRemovedEventTypeName},
    };
    if (auto it = nameByValue.find(eventTypeValue); it != nameByValue.end()) {
        return it->second;
    }
    THROW_ERROR_EXCEPTION("Could not find event type name with value %v", eventTypeValue);
}

////////////////////////////////////////////////////////////////////////////////

bool HasCompositeOrPolymorphicKey(IObjectTypeHandler* typeHandler)
{
    const auto& keyFields = typeHandler->GetKeyFields();
    bool objectHasCompositeKey = keyFields.size() > 1;
    bool objectHasPolymorphicKey = keyFields[0]->Type != EValueType::String;
    return objectHasCompositeKey || objectHasPolymorphicKey;
}

////////////////////////////////////////////////////////////////////////////////

std::set<NYPath::TYPath>::iterator FindPathParent(const std::set<NYPath::TYPath>& paths, const NYPath::TYPath& path)
{
    auto next = paths.upper_bound(path);
    if (next != paths.begin()) {
        next = std::prev(next);
        if (HasPrefix(path, *next)) {
            return next;
        }
    }

    return paths.end();
}

////////////////////////////////////////////////////////////////////////////////

TExpressionPtr CastUint64ToString(
    TExpressionPtr expression,
    IQueryContext* context)
{
    auto uSign = context->New<TLiteralExpression>(TSourceLocation(), "u");
    auto replaceTo = context->New<TLiteralExpression>(TSourceLocation(), "");
    auto ui64Expression = context->New<TFunctionExpression>(
        TSourceLocation(),
        "numeric_to_string",
        TExpressionList{std::move(expression)});
    return context->New<TFunctionExpression>(
        TSourceLocation(),
        "regex_replace_first",
        TExpressionList{
            std::move(uSign),
            std::move(ui64Expression),
            std::move(replaceTo)});
}

TExpressionPtr CastBoolToString(
    TExpressionPtr expression,
    IQueryContext* context)
{
    auto trueExpression = context->New<TLiteralExpression>(TSourceLocation(), "true");
    auto falseExpression = context->New<TLiteralExpression>(TSourceLocation(), "false");
    return context->New<TFunctionExpression>(
        TSourceLocation(),
        "if",
        TExpressionList{
            std::move(expression),
            std::move(trueExpression),
            std::move(falseExpression)});
}

TExpressionPtr CastNumericToString(
    TExpressionPtr expression,
    IQueryContext* context)
{
    return context->New<TFunctionExpression>(
        TSourceLocation(),
        "numeric_to_string",
        TExpressionList{std::move(expression)});
}

////////////////////////////////////////////////////////////////////////////////

TComputedFieldsDetector MakeComputedFieldsDetector(NObjects::IQueryContext* context)
{
    return [context] (NQueryClient::NAst::TReferenceExpressionPtr referenceExpr) -> bool {
        YT_VERIFY(context);
        auto [attribute, suffixPath] = ResolveAttributeValidated(
            context->GetTypeHandler(),
            referenceExpr->Reference.ColumnName);

        return GetFetchMethod(
            attribute,
            /*emptyPath*/ suffixPath.empty(),
            context) != EAttributeFetchMethod::ExpressionBuilder;
    };
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TResolveAttributeResult> CollectAttributes(
    IQueryContext* context,
    const TString& query)
{
    std::vector<TResolveAttributeResult> attributes;
    THashSet<TString> uniqueAttributes;
    NQuery::ExtractFilterAttributeReferences(query, [&attributes, &uniqueAttributes, &context] (TString referenceName) {
        if (uniqueAttributes.contains(referenceName)) {
            return;
        }
        auto resolveResult = ResolveAttributeValidated(
            context->GetTypeHandler(),
            referenceName);
        attributes.push_back(resolveResult);
        uniqueAttributes.insert(referenceName);
    });
    return attributes;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
