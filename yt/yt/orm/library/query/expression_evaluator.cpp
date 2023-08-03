#include "expression_evaluator.h"
#include "query_evaluator.h"
#include "query_rewriter.h"

#include <yt/yt/orm/library/attributes/helpers.h>

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

using namespace NQueryClient::NAst;
using namespace NTableClient;
using namespace NYPath;

using NYson::TYsonStringBuf;

////////////////////////////////////////////////////////////////////////////////

namespace {

inline const NLogging::TLogger Logger("ExpressionEvaluator");

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
        default:
            YT_ABORT();
    }
}

TUnversionedValue MakeUnversionedValue(
    EValueType valueType,
    const TNonOwningAttributePayload& payload,
    TObjectsHolder* objectsHolder)
{
    if (valueType == EValueType::String) {
        return Visit(
            payload,
            [&] (const TYsonStringBuf& ysonPayload) {
                // Currently, it's impossible to create a non-owning string view from TYsonString.
                // Since TUnversionedValue itself is non-owning, an external holder is needed.
                auto* regularString = objectsHolder->New<TString>();
                *regularString = NYson::ConvertFromYsonString<TString>(ysonPayload);
                return MakeUnversionedStringValue(*regularString);
            },
            [&] (const TStringBuf& stringPayload) {
                return MakeUnversionedStringValue(stringPayload);
            });
    }

    const auto& ysonPayload = std::invoke([&] () -> const TYsonStringBuf& {
        if (const auto* ysonPayloadPtr = std::get_if<TYsonStringBuf>(&payload)) {
            return *ysonPayloadPtr;
        }
        THROW_ERROR_EXCEPTION("Values of type %Qlv can be parsed from yson payload only",
            valueType);
    });

    switch (valueType) {
        case EValueType::Int64: {
            return MakeUnversionedInt64Value(NYson::ConvertFromYsonString<i64>(ysonPayload));
        }
        case EValueType::Uint64: {
            return MakeUnversionedUint64Value(NYson::ConvertFromYsonString<ui64>(ysonPayload));
        }
        case EValueType::Double: {
            return MakeUnversionedDoubleValue(NYson::ConvertFromYsonString<double>(ysonPayload));
        }
        case EValueType::Boolean: {
            return MakeUnversionedBooleanValue(NYson::ConvertFromYsonString<bool>(ysonPayload));
        }
        case EValueType::Any: {
            return MakeUnversionedAnyValue(ysonPayload.AsStringBuf());
        }
        default:
            // Attribute type validation should have been performed earlier.
            YT_ABORT();
    }
}

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////

namespace {

class TExpressionEvaluator
    : public IExpressionEvaluator
{
public:
    TExpressionEvaluator(
        TString query,
        std::vector<TTypedAttributePath> typedAttributePaths)
        : Query_(std::move(query))
        , TypedAttributePaths_(std::move(typedAttributePaths))
    {
        ValidateAttributePaths();

        EvaluationContext_ = CreateFakeTableQueryEvaluationContext(Query_);
    }

    TErrorOr<NQueryClient::TValue> Evaluate(
        const std::vector<TNonOwningAttributePayload>& attributePayloads,
        TRowBufferPtr rowBuffer) override
    {
        try {
            if (!rowBuffer) {
                rowBuffer = New<TRowBuffer>(TRowBufferTag());
            }
            if (attributePayloads.size() != TypedAttributePaths_.size()) {
                THROW_ERROR_EXCEPTION("Invalid number of attributes: expected %v, but got %v",
                    TypedAttributePaths_.size(),
                    attributePayloads.size());
            }

            TObjectsHolder temporaryObjectsHolder;
            std::vector<TUnversionedValue> inputValues;
            inputValues.reserve(attributePayloads.size());
            for (size_t index = 0; index < TypedAttributePaths_.size(); ++index) {
                inputValues.push_back(MakeUnversionedValue(
                    TypedAttributePaths_[index].Type,
                    attributePayloads[index],
                    &temporaryObjectsHolder));
            }

            return EvaluateQuery(
                *EvaluationContext_,
                MakeRange(inputValues.data(), inputValues.size()),
                rowBuffer.Get());
        } catch (const std::exception& ex) {
            return TError("Error evaluating query %Qv",
                Query_)
                << TErrorAttribute("query", Query_)
                << ex;
        }
    }

    TErrorOr<NQueryClient::TValue> Evaluate(
        const TNonOwningAttributePayload& attributePayload,
        TRowBufferPtr rowBuffer) override
    {
        return Evaluate(
            std::vector<TNonOwningAttributePayload>{attributePayload},
            std::move(rowBuffer));
    }

    const TString& GetQuery() const override
    {
        return Query_;
    }

private:
    const TString Query_;
    const std::vector<TTypedAttributePath> TypedAttributePaths_;

    std::unique_ptr<TQueryEvaluationContext> EvaluationContext_;
    NYT::TObjectsHolder ObjectsHolder_;

    struct TRowBufferTag
    { };

    void ValidateAttributePaths()
    {
        if (TypedAttributePaths_.empty()) {
            THROW_ERROR_EXCEPTION("At least one attribute path must be provided");
        }

        for (const auto& typedPath : TypedAttributePaths_) {
            ValidateAttributeType(typedPath.Type);
            NAttributes::ValidateAttributePath(typedPath.Path);
        }

        for (size_t i = 0; i < TypedAttributePaths_.size(); ++i) {
            for (size_t j = 0; j < TypedAttributePaths_.size(); ++j) {
                if (i == j) {
                    continue;
                }
                if (NYPath::HasPrefix(TypedAttributePaths_[i].Path, TypedAttributePaths_[j].Path)) {
                    THROW_ERROR_EXCEPTION(
                        "Attribute paths must be independent, but %Qv is a prefix of %Qv",
                        TypedAttributePaths_[i].Path,
                        TypedAttributePaths_[j].Path);
                }
            }
        }
    }

    const TString& GetFakeTableColumnName(const NYPath::TYPath& attributePath) const
    {
        static const TString Default = "data";
        return attributePath.empty()
            ? Default
            : attributePath;
    }

    TReferenceExpressionPtr CreateFakeTableColumnReference(const NYPath::TYPath& attributePath)
    {
        return ObjectsHolder_.New<TReferenceExpression>(
            NYT::NQueryClient::NullSourceLocation,
            GetFakeTableColumnName(attributePath));
    }

    TTypedAttributePath GetMatchingAttributePath(const TYPath& queryAttributePath)
    {
        for (const auto& dataAttributePath : TypedAttributePaths_) {
            if (NYPath::HasPrefix(queryAttributePath, dataAttributePath.Path)) {
                return dataAttributePath;
            }
        }
        THROW_ERROR_EXCEPTION("Attribute path %Qv refers to a forbidden attribute",
            queryAttributePath);
    }

    TExpressionPtr CreateFakeTableAttributeSelector(
        const TYPath& queryAttributePath)
    {
        try {
            auto typedDataAttributePath = GetMatchingAttributePath(queryAttributePath);
            const auto& dataAttributePath = typedDataAttributePath.Path;
            auto queryAttributePathSuffix = queryAttributePath.substr(dataAttributePath.size());

            if (typedDataAttributePath.Type == EValueType::Any) {
                return ObjectsHolder_.New<TFunctionExpression>(
                    NYT::NQueryClient::NullSourceLocation,
                    "try_get_any",
                    TExpressionList{
                        CreateFakeTableColumnReference(dataAttributePath),
                        ObjectsHolder_.New<TLiteralExpression>(
                            NYT::NQueryClient::NullSourceLocation,
                            std::move(queryAttributePathSuffix))});
            }
            if (!queryAttributePathSuffix.Empty()) {
                THROW_ERROR_EXCEPTION(
                    "Attribute path of type %Qlv does not support nested attributes",
                    typedDataAttributePath.Type);
            }
            return CreateFakeTableColumnReference(dataAttributePath);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error creating query selector for attribute path %Qv",
                queryAttributePath)
                << ex;
        }
    }

    NYT::NQueryClient::NAst::TExpressionPtr CreateFakeTableFilterExpression(
        const TString& query)
    {
        auto parsedQuery = ParseSource(query, NYT::NQueryClient::EParseMode::Expression);
        auto queryExpression = std::get<TExpressionPtr>(parsedQuery->AstHead.Ast);

        ObjectsHolder_.Merge(std::move(parsedQuery->AstHead));

        auto referenceMapping = [&] (const TReference& reference) {
            if (reference.TableName) {
                THROW_ERROR_EXCEPTION("Table references are not supported");
            }
            return CreateFakeTableAttributeSelector(reference.ColumnName);
        };
        TQueryRewriter rewriter(std::move(referenceMapping));

        return rewriter.Run(queryExpression);
    }

    TTableSchemaPtr CreateFakeTableSchema()
    {
        std::vector<TColumnSchema> columns;
        for (const auto& typedPath : TypedAttributePaths_) {
            columns.emplace_back(
                GetFakeTableColumnName(typedPath.Path),
                typedPath.Type);
        }
        return New<TTableSchema>(std::move(columns));
    }

    std::unique_ptr<TQueryEvaluationContext> CreateFakeTableQueryEvaluationContext(
        const TString& query)
    {
        return CreateQueryEvaluationContext(
            CreateFakeTableFilterExpression(query),
            CreateFakeTableSchema());
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

IExpressionEvaluatorPtr CreateExpressionEvaluator(
    TString query,
    std::vector<TTypedAttributePath> typedAttributePaths)
{
    return New<TExpressionEvaluator>(
        std::move(query),
        std::move(typedAttributePaths));
}

IExpressionEvaluatorPtr CreateExpressionEvaluator(
    TString query,
    std::vector<TString> attributePaths)
{
    std::vector<TTypedAttributePath> typedAttributePaths;
    typedAttributePaths.reserve(attributePaths.size());

    for (auto& path : attributePaths) {
        typedAttributePaths.push_back(TTypedAttributePath{
            .Path = std::move(path),
            .Type = EValueType::Any,
        });
    }
    return New<TExpressionEvaluator>(
        std::move(query),
        std::move(typedAttributePaths));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
