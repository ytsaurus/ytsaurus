#include "expression_evaluator.h"

#include "query_evaluator.h"

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

#include <library/cpp/yt/misc/variant.h>

#include <util/string/cast.h>

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
            "Values of type %Qlv can not be parsed from TStringBuf payload",
            valueType);
        return MakeUnversionedStringValue(*stringPayloadPtr);
    }

    const auto& ysonPayload = std::get<TYsonStringBuf>(payload);
    auto ysonPayloadBuf = ysonPayload.AsStringBuf();
    if (!ysonPayloadBuf) {
        THROW_ERROR_EXCEPTION_UNLESS(valueType == EValueType::Any,
            "Values of type %Qlv can not be parsed from empty payload",
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

class TExpressionEvaluator
    : public IExpressionEvaluator
{
public:
    TExpressionEvaluator(
        std::unique_ptr<NQueryClient::TParsedSource> parsedQuery,
        std::vector<TColumnSchema> columns)
        : ParsedQuery_(std::move(parsedQuery))
        , Columns_(std::move(columns))
        , EvaluationContext_(CreateQueryEvaluationContext(
            *ParsedQuery_,
            CreateTableSchema()))
    { }

    TErrorOr<NQueryClient::TValue> Evaluate(
        const std::vector<TNonOwningAttributePayload>& attributePayloads,
        const TRowBufferPtr& rowBuffer) const override
    {
        try {
            if (attributePayloads.size() != Columns_.size()) {
                THROW_ERROR_EXCEPTION("Invalid number of attributes: expected %v, but got %v",
                    Columns_.size(),
                    attributePayloads.size());
            }

            TObjectsHolder temporaryObjectsHolder;
            std::vector<TUnversionedValue> inputValues;
            inputValues.reserve(attributePayloads.size());
            for (size_t index = 0; index < Columns_.size(); ++index) {
                inputValues.push_back(MakeUnversionedValue(
                    Columns_[index].GetWireType(),
                    attributePayloads[index],
                    &temporaryObjectsHolder));
            }

            return EvaluateQuery(
                *EvaluationContext_,
                TRange(inputValues.data(), inputValues.size()),
                rowBuffer);
        } catch (const std::exception& ex) {
            return TError("Error evaluating query")
                << TErrorAttribute("query", ParsedQuery_->Source)
                << ex;
        }
    }

    TErrorOr<NQueryClient::TValue> Evaluate(
        const TNonOwningAttributePayload& attributePayload,
        const TRowBufferPtr& rowBuffer) const override
    {
        return Evaluate(std::vector<TNonOwningAttributePayload>{attributePayload}, rowBuffer);
    }

private:
    const std::unique_ptr<NQueryClient::TParsedSource> ParsedQuery_;
    const std::vector<TColumnSchema> Columns_;
    const std::unique_ptr<TQueryEvaluationContext> EvaluationContext_;

    TTableSchemaPtr CreateTableSchema()
    {
        return New<TTableSchema>(Columns_);
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
            << ex;
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

    return New<TExpressionEvaluator>(
        std::move(parsedQuery),
        std::move(columns));
}

IExpressionEvaluatorPtr CreateOrmExpressionEvaluator(
    std::unique_ptr<NQueryClient::TParsedSource> parsedQuery,
    std::vector<TTypedAttributePath> typedAttributePaths)
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

    return New<TExpressionEvaluator>(
        std::move(parsedQuery),
        std::move(columns));
}

IExpressionEvaluatorPtr CreateOrmExpressionEvaluator(std::unique_ptr<NQueryClient::TParsedSource> parsedQuery, std::vector<TYPath> attributePaths)
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
        std::move(typedAttributePaths));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
