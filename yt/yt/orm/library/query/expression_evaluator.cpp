#include "expression_evaluator.h"
#include "query_evaluator.h"
#include "query_rewriter.h"

#include <yt/yt/orm/library/attributes/helpers.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/ypath/tokenizer.h>
#include <yt/yt/core/yson/string.h>

#include <yt/yt/library/query/base/query_preparer.h>

#include <util/string/cast.h>

namespace NYT::NOrm::NQuery {

using namespace NQueryClient::NAst;
using namespace NTableClient;
using namespace NYPath;

using NYson::TYsonStringBuf;

////////////////////////////////////////////////////////////////////////////////

namespace {

inline const NLogging::TLogger Logger("ExpressionEvaluator");

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////

namespace {

class TExpressionEvaluator
    : public IExpressionEvaluator
{
public:
    TExpressionEvaluator(
        TString query,
        std::vector<TString> attributePaths)
        : Query_(std::move(query))
        , AttributePaths_(std::move(attributePaths))
    {
        ValidateAttributePaths();

        EvaluationContext_ = CreateFakeTableQueryEvaluationContext(Query_);
    }

    TErrorOr<NQueryClient::TValue> Evaluate(
        const std::vector<TYsonStringBuf>& attributeYsons,
        TRowBufferPtr rowBuffer) override
    {
        try {
            if (!rowBuffer) {
                rowBuffer = New<TRowBuffer>(TRowBufferTag());
            }
            if (attributeYsons.size() != AttributePaths_.size()) {
                THROW_ERROR_EXCEPTION("Invalid number of attributes: expected %v, but got %v",
                    AttributePaths_.size(),
                    attributeYsons.size());
            }
            std::vector<TUnversionedValue> inputValues;
            inputValues.reserve(attributeYsons.size());
            for (auto attributeYson : attributeYsons) {
                inputValues.push_back(MakeUnversionedAnyValue(attributeYson.AsStringBuf()));
            }
            return EvaluateQuery(
                *EvaluationContext_,
                {inputValues.data(), inputValues.size()},
                rowBuffer.Get());
        } catch (const std::exception& ex) {
            return TError("Error evaluating query %Qv",
                Query_)
                << TErrorAttribute("query", Query_)
                << ex;
        }
    }

    TErrorOr<NQueryClient::TValue> Evaluate(
        const TYsonStringBuf& attributeYson,
        TRowBufferPtr rowBuffer) override
    {
        return Evaluate(std::vector<TYsonStringBuf>{attributeYson}, std::move(rowBuffer));
    }

    const TString& GetQuery() const override
    {
        return Query_;
    }

private:
    const TString Query_;
    const std::vector<TString> AttributePaths_;

    std::unique_ptr<TQueryEvaluationContext> EvaluationContext_;
    NYT::TObjectsHolder ObjectsHolder_;

    struct TRowBufferTag
    { };

    void ValidateAttributePaths()
    {
        if (AttributePaths_.empty()) {
            THROW_ERROR_EXCEPTION("At least one attribute path must be provided");
        }

        for (const auto& path : AttributePaths_) {
            NAttributes::ValidateAttributePath(path);
        }

        for (size_t i = 0; i < AttributePaths_.size(); ++i) {
            for (size_t j = 0; j < AttributePaths_.size(); ++j) {
                if (i != j && NYPath::HasPrefix(AttributePaths_[i], AttributePaths_[j])) {
                    THROW_ERROR_EXCEPTION(
                        "Attribute paths must be independent, but %Qv is a prefix of %Qv",
                        AttributePaths_[i],
                        AttributePaths_[j]);
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

    NYPath::TYPath GetMatchingAttributePath(const TYPath& queryAttributePath)
    {
        for (const auto& dataAttributePath : AttributePaths_) {
            if (NYPath::HasPrefix(queryAttributePath, dataAttributePath)) {
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
            auto dataAttributePath = GetMatchingAttributePath(queryAttributePath);
            auto queryAttributePathSuffix = queryAttributePath.substr(dataAttributePath.size());

            return ObjectsHolder_.New<TFunctionExpression>(
                NYT::NQueryClient::NullSourceLocation,
                "try_get_any",
                TExpressionList{
                    CreateFakeTableColumnReference(dataAttributePath),
                    ObjectsHolder_.New<TLiteralExpression>(
                        NYT::NQueryClient::NullSourceLocation,
                        std::move(queryAttributePathSuffix))});
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
        for (const auto& path : AttributePaths_) {
            columns.emplace_back(
                GetFakeTableColumnName(path),
                EValueType::Any);
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
    std::vector<TString> attributePaths)
{
    return New<TExpressionEvaluator>(
        std::move(query),
        std::move(attributePaths));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
