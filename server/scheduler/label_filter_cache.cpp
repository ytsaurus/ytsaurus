#include "label_filter_cache.h"
#include "object.h"
#include "private.h"

#include <yp/server/objects/transaction.h>
#include <yp/server/objects/helpers.h>
#include <yp/server/objects/db_schema.h>
#include <yp/server/objects/attribute_schema.h>
#include <yp/server/objects/type_handler.h>

#include <yt/ytlib/query_client/query_preparer.h>
#include <yt/ytlib/query_client/folding_profiler.h>

#include <yt/client/table_client/row_buffer.h>

namespace NYP::NServer::NScheduler {

using namespace NObjects;

using namespace NYT::NTableClient;
using namespace NYT::NQueryClient;
using namespace NYT::NQueryClient::NAst;

using NYT::NQueryClient::TSourceLocation;

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
        static const auto expression = New<NAst::TReferenceExpression>(TSourceLocation(), ObjectsTable.Fields.Labels.Name);
        return expression;
    }

    virtual NYT::NQueryClient::NAst::TExpressionPtr GetAnnotationExpression(const TString& /*name*/) override
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

TLabelFilterCacheBase::TLabelFilterCacheBase(
    IObjectTypeHandler* typeHandler,
    std::vector<TObject*> allObjects)
    : TypeHandler_(typeHandler)
    , AllObjects_(std::move(allObjects))
{ }

TErrorOr<std::vector<TObject*>> TLabelFilterCacheBase::DoGetFilteredObjects(const TString& query)
{
    if (query.empty()) {
        return AllObjects_;
    }

    YT_LOG_DEBUG("Started filtering objects (Type: %v, Query: %v)",
        TypeHandler_->GetType(),
        query);

    try {
        TObjectFilter filter{query};
        TQueryContext queryContext(TypeHandler_);
        auto astExpression = BuildFilterExpression(&queryContext, filter);

        auto expressionSource = FormatExpression(*astExpression);

        auto astHead = TAstHead::MakeExpression();
        astHead.Ast = std::move(astExpression);

        TParsedSource parsedSource(std::move(expressionSource), std::move(astHead));

        const auto& schema = GetLabelsSchema();

        auto expression = PrepareExpression(
            parsedSource,
            schema,
            BuiltinTypeInferrersMap,
            nullptr);

        TCGVariables variables;

        auto evaluator = Profile(
            expression,
            schema,
            nullptr,
            &variables,
            BuiltinFunctionProfilers)();

        struct TRowBufferTag { };
        auto rowBuffer = New<TRowBuffer>(TRowBufferTag());

        std::vector<TObject*> matchingObjects;
        for (auto* object : AllObjects_) {
            auto labelsValue = MakeUnversionedAnyValue(object->GetLabels().GetData());
            // Pre-zero value to avoid garbage after evaluator.
            auto resultValue = MakeUnversionedSentinelValue(EValueType::Null);

            evaluator(
                variables.GetLiteralValues(),
                variables.GetOpaqueData(),
                &resultValue,
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
            TypeHandler_->GetType())
            << TErrorAttribute("query", query)
            << ex;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler

