#include "expression_evaluator.h"

#include <yt/yt/orm/library/query/helpers.h>
#include <yt/yt/orm/library/query/query_rewriter.h>

#include <yt/yt/orm/library/attributes/attribute_path.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/ypath/tokenizer.h>
#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/library/query/engine_api/query_evaluator.h>

#include <library/cpp/yt/misc/variant.h>

#include <library/cpp/threading/thread_local/thread_local.h>

#include <util/string/cast.h>

#include <util/system/mutex.h>

namespace NYT::NOrm::NQuery {
namespace {

using namespace NQueryClient::NAst;
using namespace NTableClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

using NYson::TYsonStringBuf;

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger Logger("ExpressionEvaluator");

////////////////////////////////////////////////////////////////////////////////

void ValidateAttributeType(EValueType valueType)
{
    switch (valueType) {
        case EValueType::Int64:
        case EValueType::Uint64:
        case EValueType::Double:
        case EValueType::Boolean:
        case EValueType::String:
        case EValueType::Any:
            return;
        case EValueType::Min:
        case EValueType::TheBottom:
        case EValueType::Null:
        case EValueType::Composite:
        case EValueType::Max:
            THROW_ERROR_EXCEPTION("Attribute type %Qlv is not supported", valueType);
    }
}

void ValidateAttributePaths(const std::vector<TTypedAttributePath>& typedAttributePaths)
{
    if (typedAttributePaths.empty()) {
        THROW_ERROR_EXCEPTION("At least one attribute path must be provided");
    }

    for (const auto& typedPath : typedAttributePaths) {
        NAttributes::ValidateAttributePath(typedPath.Path);
    }

    for (size_t i = 0; i < typedAttributePaths.size(); ++i) {
        for (size_t j = 0; j < typedAttributePaths.size(); ++j) {
            if (i == j) {
                continue;
            }
            if (typedAttributePaths[i].Path == typedAttributePaths[j].Path) {
                THROW_ERROR_EXCEPTION(
                    "Attribute paths must be unique, but %Qv is found more than once",
                    typedAttributePaths[i].Path);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TUnversionedValue MakeUnversionedValue(
    EValueType valueType,
    const TNonOwningAttributePayload& payload,
    TObjectsHolder* /*objectsHolder*/)
{
    if (const auto* stringPayloadPtr = std::get_if<TStringBuf>(&payload)) {
        THROW_ERROR_EXCEPTION_UNLESS(valueType == EValueType::String,
            "Values of type %Qlv cannot be parsed from TStringBuf payload",
            valueType);
        return MakeUnversionedStringValue(*stringPayloadPtr);
    }

    const auto& ysonPayload = std::get<TYsonStringBuf>(payload);
    auto ysonPayloadBuf = ysonPayload.AsStringBuf();
    if (!ysonPayloadBuf) {
        THROW_ERROR_EXCEPTION_UNLESS(valueType == EValueType::Any,
            "Values of type %Qlv cannot be parsed from empty payload",
            valueType);
        return MakeUnversionedAnyValue(ysonPayloadBuf);
    }

    TMemoryInput input(ysonPayloadBuf);
    NYson::TYsonPullParser parser(&input, NYson::EYsonType::Node);

    if (parser.IsEntity()) {
        return MakeUnversionedNullValue();
    }

    switch (valueType) {
        case EValueType::String: {
            return MakeUnversionedStringValue(parser.ParseString());
        }
        case EValueType::Int64: {
            return MakeUnversionedInt64Value(parser.ParseInt64());
        }
        case EValueType::Uint64: {
            return MakeUnversionedUint64Value(parser.ParseUint64());
        }
        case EValueType::Double: {
            return MakeUnversionedDoubleValue(parser.ParseDouble());
        }
        case EValueType::Boolean: {
            return MakeUnversionedBooleanValue(parser.ParseBoolean());
        }
        case EValueType::Any: {
            return MakeUnversionedAnyValue(ysonPayloadBuf);
        }
        default:
            // Attribute type validation should have been performed earlier.
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

class TExpressionEvaluatorBase
    : public IExpressionEvaluator
{
public:
    TErrorOr<NQueryClient::TValue> Evaluate(
        const std::vector<TNonOwningAttributePayload>& attributePayloads,
        const TRowBufferPtr& rowBuffer) const override
    {
        try {
            const auto& columns = Schema_->Columns();
            if (attributePayloads.size() != columns.size()) {
                THROW_ERROR_EXCEPTION("Invalid number of attributes: expected %v, but got %v",
                    columns.size(),
                    attributePayloads.size());
            }

            TObjectsHolder temporaryObjectsHolder;
            std::vector<TUnversionedValue> inputValues;
            inputValues.reserve(attributePayloads.size());
            for (int index = 0; index < std::ssize(columns); ++index) {
                inputValues.push_back(MakeUnversionedValue(
                    columns[index].GetWireType(),
                    attributePayloads[index],
                    &temporaryObjectsHolder));
            }

            return EvaluateExpression(TRange(inputValues.data(), inputValues.size()), rowBuffer);
        } catch (const std::exception& ex) {
            return TError("Error evaluating query")
                .With("query", Source_)
                .With(ex);
        }
    }

    TErrorOr<NQueryClient::TValue> Evaluate(
        const TNonOwningAttributePayload& attributePayload,
        const TRowBufferPtr& rowBuffer) const override
    {
        return Evaluate(std::vector<TNonOwningAttributePayload>{attributePayload}, rowBuffer);
    }

protected:
    const TTableSchemaPtr Schema_;

    TExpressionEvaluatorBase(
        std::string source,
        std::vector<TColumnSchema> columns)
        : Schema_(New<TTableSchema>(std::move(columns)))
        , Source_(std::move(source))
    { }

private:
    const std::string Source_;

    virtual NQueryClient::TValue EvaluateExpression(
        TRange<NQueryClient::TValue> inputValues,
        const TRowBufferPtr& rowBuffer) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TExpressionEvaluator
    : public TExpressionEvaluatorBase
{
public:
    TExpressionEvaluator(
        const NQueryClient::TParsedSource& parsedQuery,
        std::vector<TColumnSchema> columns)
        : TExpressionEvaluatorBase(parsedQuery.Source, std::move(columns))
        , EvaluationContext_(NQueryClient::CreateQueryEvaluationContext(parsedQuery, Schema_))
    { }

private:
    const NQueryClient::TQueryEvaluationContextPtr EvaluationContext_;

    NQueryClient::TValue EvaluateExpression(
        TRange<NQueryClient::TValue> inputValues,
        const TRowBufferPtr& rowBuffer) const override
    {
        return EvaluateQuery(*EvaluationContext_, inputValues, rowBuffer);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TThreadSafeExpressionEvaluator
    : public TExpressionEvaluatorBase
{
public:
    TThreadSafeExpressionEvaluator(
        std::unique_ptr<NQueryClient::TParsedSource> parsedQuery,
        std::vector<TColumnSchema> columns)
        : TExpressionEvaluatorBase(parsedQuery->Source, std::move(columns))
    {
        auto context = NQueryClient::CreateQueryEvaluationContext(*parsedQuery, Schema_);
        // TODO(grigminakov): Empty opaque data alone does not guarantee thread safety.
        // Replace this temporary heuristic in YTORM-1835.
        if (context->Variables.GetOpaqueData().Empty()) {
            SharedEvaluationContext_ = std::move(context);
        } else {
            PerThreadContexts_.emplace(std::move(parsedQuery), std::move(context));
        }
    }

private:
    struct TPerThreadContexts
    {
        const std::unique_ptr<const NQueryClient::TParsedSource> ParsedQuery;
        mutable TMutex PreparationMutex;
        ::NThreading::TThreadLocalValue<
            NQueryClient::TQueryEvaluationContextPtr,
            ::NThreading::EThreadLocalImpl::StdThreadLocal> Contexts;

        TPerThreadContexts(
            std::unique_ptr<NQueryClient::TParsedSource> parsedQuery,
            NQueryClient::TQueryEvaluationContextPtr context)
            : ParsedQuery(std::move(parsedQuery))
        {
            Contexts.GetRef() = std::move(context);
        }
    };

    NQueryClient::TQueryEvaluationContextPtr SharedEvaluationContext_;
    std::optional<TPerThreadContexts> PerThreadContexts_;

    NQueryClient::TValue EvaluateExpression(
        TRange<NQueryClient::TValue> inputValues,
        const TRowBufferPtr& rowBuffer) const override
    {
        if (SharedEvaluationContext_) {
            return EvaluateQuery(*SharedEvaluationContext_, inputValues, rowBuffer);
        }

        auto result = EvaluateQuery(GetThreadLocalEvaluationContext(), inputValues, rowBuffer);
        // Literal data may belong to a context that is destroyed when this thread exits.
        if (IsStringLikeType(result.Type)) {
            THROW_ERROR_EXCEPTION_UNLESS(rowBuffer, "A row buffer is required for string-like query results");
            rowBuffer->CaptureValue(&result);
        }
        return result;
    }

    const NQueryClient::TQueryEvaluationContext& GetThreadLocalEvaluationContext() const
    {
        if (const auto& context = PerThreadContexts_->Contexts.GetRef()) {
            return *context;
        }

        auto context = NQueryClient::CreateQueryEvaluationContext(PrepareExpression(), Schema_);
        return *(PerThreadContexts_->Contexts.GetRef() = std::move(context));
    }

    NQueryClient::TConstExpressionPtr PrepareExpression() const
    {
        // Serialize access to the rewritten AST and compile each fresh typed expression independently.
        auto guard = Guard(PerThreadContexts_->PreparationMutex);
        return NQueryClient::PrepareExpression(*PerThreadContexts_->ParsedQuery, *Schema_);
    }
};

////////////////////////////////////////////////////////////////////////////////

std::string GetFakeTableColumnName(TYPathBuf attributePath)
{
    static const std::string Default = "data";
    return attributePath.empty()
        ? Default
        : std::string(attributePath);
}

TReferenceExpressionPtr CreateFakeTableColumnReference(
    TYPathBuf attributePath,
    TObjectsHolder* holder)
{
    return holder->New<TReferenceExpression>(
        NQueryClient::NullSourceLocation,
        GetFakeTableColumnName(attributePath));
}

TTypedAttributePath GetMatchingAttributePath(
    TYPathBuf queryAttributePath,
    const std::vector<TTypedAttributePath>& typedAttributePaths)
{
    std::optional<TTypedAttributePath> result = std::nullopt;
    for (const auto& dataAttributePath : typedAttributePaths) {
        if (NYPath::HasPrefix(queryAttributePath, dataAttributePath.Path)) {
            if (!result || dataAttributePath.Path.size() > result->Path.size()) {
                result = dataAttributePath;
            }
        }
    }
    THROW_ERROR_EXCEPTION_UNLESS(result, "Attribute path %Qv refers to a forbidden attribute",
        queryAttributePath);
    return *result;
}

TExpressionPtr CreateFakeTableAttributeSelector(
    TYPathBuf queryAttributePath,
    const std::vector<TTypedAttributePath>& typedAttributePaths,
    TObjectsHolder* holder)
{
    try {
        auto typedDataAttributePath = GetMatchingAttributePath(queryAttributePath, typedAttributePaths);
        const auto& dataAttributePath = typedDataAttributePath.Path;
        auto queryAttributePathSuffix = queryAttributePath.substr(dataAttributePath.size());

        if (queryAttributePathSuffix.empty()) {
            return CreateFakeTableColumnReference(dataAttributePath, holder);
        }

        auto subtype = typedDataAttributePath.TypeResolver->ResolveType(queryAttributePathSuffix);
        ValidateAttributeType(subtype);
        return holder->New<TFunctionExpression>(
            NQueryClient::NullSourceLocation,
            GetYsonExtractFunction(subtype),
            TExpressionList{
                CreateFakeTableColumnReference(dataAttributePath, holder),
                holder->New<TLiteralExpression>(
                    NQueryClient::NullSourceLocation,
                    std::string(queryAttributePathSuffix))});
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error creating query selector for attribute path %Qv",
            queryAttributePath)
            .With(ex);
    }
}

std::vector<TColumnSchema> CreateColumnsFromPaths(const std::vector<TTypedAttributePath>& typedAttributePaths)
{
    std::vector<TColumnSchema> columns;
    for (const auto& typedPath : typedAttributePaths) {
        auto type = typedPath.TypeResolver->ResolveType();
        ValidateAttributeType(type);
        columns.emplace_back(GetFakeTableColumnName(typedPath.Path), type);
    }
    return columns;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

IExpressionEvaluatorPtr CreateExpressionEvaluator(
    TStringBuf query,
    std::vector<TColumnSchema> columns)
{
    auto parsedQuery = ParseSource(query, NQueryClient::EParseMode::Expression);
    auto queryExpression = std::get<TExpressionPtr>(parsedQuery->AstHead.Ast);
    auto& objectsHolder = parsedQuery->AstHead;

    std::optional<std::string> tableName;
    auto referenceMapping = [&] (const TReference& reference) {
        if (reference.TableName) {
            if (tableName) {
                if (tableName != reference.TableName) {
                    THROW_ERROR_EXCEPTION(
                        "Query %Qv contains conflicting table names: expected %Qv, but got %Qv",
                        query,
                        tableName,
                        reference.TableName);
                }
            } else {
                tableName = reference.TableName;
            }
        }
        return objectsHolder.New<TReferenceExpression>(
            NQueryClient::NullSourceLocation,
            reference.ColumnName);
    };
    TQueryRewriter rewriter(&objectsHolder, std::move(referenceMapping));
    objectsHolder.Ast = rewriter.Run(queryExpression);

    return New<TExpressionEvaluator>(*parsedQuery, std::move(columns));
}

IExpressionEvaluatorPtr CreateOrmExpressionEvaluator(
    std::unique_ptr<NQueryClient::TParsedSource> parsedQuery,
    std::vector<TTypedAttributePath> typedAttributePaths,
    EExpressionEvaluatorMode mode)
{
    ValidateAttributePaths(typedAttributePaths);

    auto columns = CreateColumnsFromPaths(typedAttributePaths);

    auto queryExpression = std::get<TExpressionPtr>(parsedQuery->AstHead.Ast);
    auto& astHead = parsedQuery->AstHead;

    auto referenceMapping = [&] (const TReference& reference) {
        if (reference.TableName) {
            THROW_ERROR_EXCEPTION("Table references are not supported");
        }
        return CreateFakeTableAttributeSelector(TYPath(reference.ColumnName), typedAttributePaths, &astHead);
    };
    TQueryRewriter rewriter(&astHead, std::move(referenceMapping));
    astHead.Ast = rewriter.Run(queryExpression);

    if (mode == EExpressionEvaluatorMode::ThreadSafe) {
        return New<TThreadSafeExpressionEvaluator>(std::move(parsedQuery), std::move(columns));
    }
    return New<TExpressionEvaluator>(*parsedQuery, std::move(columns));
}

IExpressionEvaluatorPtr CreateOrmExpressionEvaluator(
    std::unique_ptr<NQueryClient::TParsedSource> parsedQuery,
    std::vector<TYPath> attributePaths,
    EExpressionEvaluatorMode mode)
{
    std::vector<TTypedAttributePath> typedAttributePaths;
    typedAttributePaths.reserve(attributePaths.size());

    for (auto& path : attributePaths) {
        typedAttributePaths.push_back(TTypedAttributePath{
            .Path = std::move(path),
            .TypeResolver = GetTypeResolver(NTableClient::EValueType::Any),
        });
    }

    return CreateOrmExpressionEvaluator(
        std::move(parsedQuery),
        std::move(typedAttributePaths),
        mode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
