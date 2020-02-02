#include "label_filter_evaluator.h"

#include "private.h"

#include <yp/server/lib/cluster/object.h>
#include <yp/server/lib/cluster/object_filter_evaluator.h>

#include <yp/server/lib/objects/object_filter.h>

#include <yp/server/lib/query_helpers/query_evaluator.h>
#include <yp/server/lib/query_helpers/query_rewriter.h>

#include <yt/client/table_client/row_buffer.h>
#include <yt/client/table_client/schema.h>

namespace NYP::NServer::NHeavyScheduler {

using namespace NCluster;

using namespace NQueryClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

const THashMap<NYPath::TYPath, TString> ColumnNameByAttributePathFirstToken({
    std::make_pair<NYPath::TYPath, TString>("labels", "labels"),
});

std::vector<TColumnSchema> GenerateFakeTableColumns() {
    std::vector<TColumnSchema> columns;
    for (const auto& [attributePathFirstToken, columnName] : ColumnNameByAttributePathFirstToken) {
        columns.emplace_back(columnName, EValueType::Any);
    }
    return columns;
}

const TTableSchema& GetFakeTableSchema()
{
    static const TTableSchema schema(GenerateFakeTableColumns());
    return schema;
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
            NYT::TObjectsHolder holder;

            auto evaluationContext = NQueryHelpers::CreateQueryEvaluationContext(
                NQueryHelpers::BuildFakeTableFilterExpression(
                    &holder,
                    filter.Query,
                    ColumnNameByAttributePathFirstToken),
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
