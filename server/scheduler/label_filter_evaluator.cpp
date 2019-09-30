#include "label_filter_evaluator.h"

#include "private.h"

#include <yp/server/master/bootstrap.h>

#include <yp/server/objects/attribute_schema.h>
#include <yp/server/objects/db_schema.h>
#include <yp/server/objects/helpers.h>
#include <yp/server/objects/object_manager.h>
#include <yp/server/objects/type_handler.h>

#include <yp/server/lib/cluster/object.h>
#include <yp/server/lib/cluster/object_filter_evaluator.h>

#include <yp/server/lib/objects/object_filter.h>

#include <yp/server/lib/query_helpers/query_evaluator.h>

#include <yt/ytlib/query_client/ast.h>

#include <yt/client/table_client/row_buffer.h>

namespace NYP::NServer::NScheduler {

using namespace NObjects;

using namespace NQueryClient;
using namespace NTableClient;

using NQueryClient::TSourceLocation;

////////////////////////////////////////////////////////////////////////////////

namespace {

const TTableSchema& GetLabelsSchema()
{
    static const TTableSchema schema({
        TColumnSchema(ObjectsTable.Fields.Labels.Name, EValueType::Any)
    });
    return schema;
}

class TQueryContext
    : public IQueryContext
{
public:
    explicit TQueryContext(IObjectTypeHandler* typeHandler)
        : TypeHandler_(typeHandler)
    { }

    virtual IObjectTypeHandler* GetTypeHandler() override
    {
        return TypeHandler_;
    }

    virtual NAst::TExpressionPtr GetFieldExpression(const TDBField* field) override
    {
        if (field->Name != ObjectsTable.Fields.Labels.Name) {
            ThrowNotSupported();
        }
        static const auto Expression = New<NAst::TReferenceExpression>(
            TSourceLocation(),
            ObjectsTable.Fields.Labels.Name);
        return Expression;
    }

    virtual NAst::TExpressionPtr GetAnnotationExpression(const TString& /*name*/) override
    {
        ThrowNotSupported();
        YT_ABORT();
    }

private:
    IObjectTypeHandler* const TypeHandler_;

private:
    void ThrowNotSupported()
    {
        THROW_ERROR_EXCEPTION("Only labels are supported during object filtering");
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TLabelFilterEvaluator
    : public NCluster::IObjectFilterEvaluator
{
public:
    explicit TLabelFilterEvaluator(NMaster::TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

    virtual TErrorOr<std::vector<NCluster::TObject*>> Evaluate(
        EObjectType objectType,
        const std::vector<NCluster::TObject*>& objects,
        const TObjectFilter& filter) override
    {
        const auto& query = filter.Query;

        if (query.empty()) {
            return objects;
        }

        YT_LOG_DEBUG("Started filtering objects (Type: %v, Query: %v)",
            objectType,
            query);

        try {
            auto* typeHandler = Bootstrap_->GetObjectManager()->GetTypeHandler(objectType);
            TQueryContext context(typeHandler);
            auto astExpression = BuildFilterExpression(&context, filter);

            auto evaluationContext = NQueryHelpers::CreateQueryEvaluationContext(
                astExpression,
                GetLabelsSchema());

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

private:
    NMaster::TBootstrap* const Bootstrap_;
};

////////////////////////////////////////////////////////////////////////////////

NCluster::IObjectFilterEvaluatorPtr CreateLabelFilterEvaluator(NMaster::TBootstrap* bootstrap)
{
    return New<TLabelFilterEvaluator>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
