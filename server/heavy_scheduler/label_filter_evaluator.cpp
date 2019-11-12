#include "label_filter_evaluator.h"

#include "private.h"

#include <yp/server/lib/cluster/object.h>
#include <yp/server/lib/cluster/object_filter_evaluator.h>

#include <yp/server/lib/objects/object_filter.h>

#include <yp/server/lib/query_helpers/query_evaluator.h>
#include <yp/server/lib/query_helpers/query_rewriter.h>

#include <yt/ytlib/query_client/ast.h>
#include <yt/ytlib/query_client/query_preparer.h>

#include <yt/client/table_client/row_buffer.h>
#include <yt/client/table_client/schema.h>

#include <yt/core/ypath/tokenizer.h>

namespace NYP::NServer::NHeavyScheduler {

using namespace NCluster;

using namespace NQueryClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

const NYPath::TYPath LabelAttributePrefix = "labels";
const TString LabelsColumnName = "labels";

const TTableSchema& GetFakeTableSchema()
{
    static const TTableSchema schema({
        TColumnSchema(LabelsColumnName, EValueType::Any)
    });
    return schema;
}

NAst::TReferenceExpressionPtr GetLabelsFakeTableColumnReference()
{
    return New<NAst::TReferenceExpression>(
        NullSourceLocation,
        LabelsColumnName);
}

NAst::TExpressionPtr BuildLabelAttributeSelector(const NYPath::TYPath& labelAttributePath)
{
    try {
        NYT::NYPath::TTokenizer tokenizer(labelAttributePath);
        tokenizer.Advance();
        tokenizer.Expect(NYT::NYPath::ETokenType::Slash);

        tokenizer.Advance();
        tokenizer.Expect(NYT::NYPath::ETokenType::Literal);
        if (tokenizer.GetLiteralValue() != LabelAttributePrefix) {
            THROW_ERROR_EXCEPTION("Label attribute must start with /%v, but got %v",
                LabelAttributePrefix,
                labelAttributePath);
        }

        tokenizer.Advance();
        auto labelAttributePathSuffix = ToString(tokenizer.GetInput());

        return NAst::TExpressionPtr(New<NAst::TFunctionExpression>(
            NullSourceLocation,
            "try_get_any",
            NAst::TExpressionList{
                GetLabelsFakeTableColumnReference(),
                New<NAst::TLiteralExpression>(
                    NullSourceLocation,
                    std::move(labelAttributePathSuffix))
            }));
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing label attribute path %v",
            labelAttributePath);
    }
}

NAst::TExpressionPtr BuildFilterExpression(const NObjects::TObjectFilter& filter)
{
    auto parsedQuery = ParseSource(filter.Query, EParseMode::Expression);
    const auto& queryExpression = std::get<NAst::TExpressionPtr>(parsedQuery->AstHead.Ast);

    auto referenceMapping = [] (const NAst::TReference& reference) {
        if (reference.TableName) {
            THROW_ERROR_EXCEPTION("Table references are not supported");
        }
        return BuildLabelAttributeSelector(reference.ColumnName);
    };
    NQueryHelpers::TQueryRewriter rewriter(std::move(referenceMapping));

    return rewriter.Run(queryExpression);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TLabelFilterEvaluator
    : public IObjectFilterEvaluator
{
public:
    virtual NYT::TErrorOr<std::vector<TObject*>> Evaluate(
        NObjects::EObjectType objectType,
        const std::vector<TObject*>& objects,
        const NObjects::TObjectFilter& filter) override
    {
        const auto& query = filter.Query;

        if (query.empty()) {
            return objects;
        }

        YT_LOG_DEBUG("Started filtering objects (Type: %v, Query: %v)",
            objectType,
            query);

        try {
            auto evaluationContext = NQueryHelpers::CreateQueryEvaluationContext(
                BuildFilterExpression(filter),
                GetFakeTableSchema());

            struct TRowBufferTag { };
            auto rowBuffer = New<TRowBuffer>(TRowBufferTag());

            std::vector<NCluster::TObject*> matchingObjects;
            for (auto* object : objects) {
                auto labelsValue = MakeUnversionedAnyValue(object->GetLabels().GetData());

                auto resultValue = NQueryHelpers::EvaluateQuery(
                    evaluationContext,
                    &labelsValue,
                    rowBuffer.Get());

                if (resultValue.Type == EValueType::Boolean && resultValue.Data.Boolean) {
                    matchingObjects.push_back(object);
                }
            }

            YT_LOG_DEBUG("Finished filtering objects (MatchingCount: %v)",
                matchingObjects.size());

            return matchingObjects;
        } catch (const std::exception& ex) {
            return TError("Error filtering %Qlv objects",
                objectType)
                << TErrorAttribute("query", query)
                << ex;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectFilterEvaluatorPtr CreateLabelFilterEvaluator()
{
    return New<TLabelFilterEvaluator>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
