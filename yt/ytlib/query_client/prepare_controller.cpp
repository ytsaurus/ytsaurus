#include "stdafx.h"
#include "prepare_controller.h"

#include "private.h"
#include "helpers.h"

#include "callbacks.h"

#include "plan_node.h"
#include "plan_visitor.h"
#include "plan_helpers.h"

#include "lexer.h"
#include "parser.hpp"

#include <ytlib/table_client/chunk_meta_extensions.h>

#include <ytlib/new_table_client/chunk_meta_extensions.h>
#include <ytlib/new_table_client/schema.h>

#include <core/concurrency/scheduler.h>

#include <core/ytree/convert.h>

namespace NYT {
namespace NQueryClient {

using namespace NYPath;
using namespace NConcurrency;
using namespace NVersionedTableClient;

static const auto& Logger = QueryClientLogger;
static const int PlanFragmentDepthLimit = 50;

////////////////////////////////////////////////////////////////////////////////

TPrepareController::TPrepareController(
    IPrepareCallbacks* callbacks,
    const Stroka& source,
    i64 inputRowLimit,
    i64 outputRowLimit,
    TTimestamp timestamp)
    : Callbacks_(callbacks)
    , Source_(source)
    , Context_(New<TPlanContext>(timestamp, inputRowLimit, outputRowLimit))
    , Head_(nullptr)
{ }

TPrepareController::~TPrepareController()
{ }

namespace {
class TCheckAndPruneReferences
    : public TPlanVisitor
{
public:
    virtual bool Visit(const TFilterOperator* op) override
    {
        CurrentSourceSchema_ = op->GetSource()->GetTableSchema();
        Traverse(this, op->GetPredicate());
        return true;
    }

    virtual bool Visit(const TGroupOperator* op) override
    {
        // TODO(lukyan): Prune not live aggregate items

        CurrentSourceSchema_ = op->GetSource()->GetTableSchema();
        LiveColumns_.clear();
        for (auto& groupItem : op->GroupItems()) {
            Traverse(this, groupItem.Expression);
        }
        for (auto& aggregateItem : op->AggregateItems()) {
            Traverse(this, aggregateItem.Expression);
        }
        return true;
    }

    virtual bool Visit(const TProjectOperator* op) override
    {
        CurrentSourceSchema_ = op->GetSource()->GetTableSchema();
        LiveColumns_.clear();
        for (auto& projection : op->Projections()) {
            Traverse(this, projection.Expression);
        }
        return true;
    }

    virtual bool Visit(const TReferenceExpression* expr) override
    {
        const auto name = expr->GetColumnName();
        auto* column = CurrentSourceSchema_.FindColumn(name);
        if (!column) {
            THROW_ERROR_EXCEPTION("Undefined reference %s", ~name.Quote());
        }
        LiveColumns_.insert(name);
        return true;
    }

    const std::set<Stroka>& GetLiveColumns()
    {
        return LiveColumns_;
    }

private:
    std::set<Stroka> LiveColumns_;
    TTableSchema CurrentSourceSchema_;

};
} // anonymous namespace

TPlanFragment TPrepareController::Run()
{
    ParseSource();
    CheckDepth();
    GetInitialSplits();
    FillSelectAll();
    MoveAggregateExpressions();
    CheckAndPruneReferences();
    TypecheckExpressions();

    return TPlanFragment(std::move(Context_), Head_);
}

void TPrepareController::ParseSource()
{
    // Hook up with debug information for better error messages.
    Context_->SetSource(Source_);

    TLexer lexer(Context_.Get(), Source_, TParser::token::StrayWillParseQuery);
    TParser parser(lexer, Context_.Get(), &Head_);

    int result = parser.parse();
    if (result != 0) {
        THROW_ERROR_EXCEPTION("Failed to parse query");
    }
}

void TPrepareController::CheckDepth()
{
    std::function<int(const TExpression* op)> getExpressionDepth = [&] (const TExpression* op) -> int {
        if (op->IsA<TLiteralExpression>()) {
            return 1;
        } else if (op->IsA<TReferenceExpression>()) {
            return 1;
        } else if (auto* functionExpr = op->As<TFunctionExpression>()) {
            int maxChildDepth = 0;
            for (const auto& argument : functionExpr->Arguments()) {
                maxChildDepth = std::max(maxChildDepth, getExpressionDepth(argument));
            }
            return maxChildDepth + 1;
        } else if (auto* binaryOpExpr = op->As<TBinaryOpExpression>()) {
            return std::max(
                getExpressionDepth(binaryOpExpr->GetLhs()), 
                getExpressionDepth(binaryOpExpr->GetRhs())) + 1;
        }
        YUNREACHABLE();
    };

    std::function<int(const TOperator* op)> getOperatorDepth = [&] (const TOperator* op) -> int {
        if (op->IsA<TScanOperator>()) {
            return 1;
        } else if (auto* filterOp = op->As<TFilterOperator>()) {
            return std::max(
                getOperatorDepth(filterOp->GetSource()), 
                getExpressionDepth(filterOp->GetPredicate())) + 1;
        } else if (auto* projectOp = op->As<TProjectOperator>()) {
            int maxChildDepth = getOperatorDepth(projectOp->GetSource());
            for (const auto& projection : projectOp->Projections()) {
                maxChildDepth = std::max(maxChildDepth, getExpressionDepth(projection.Expression) + 1);
            }
            return maxChildDepth + 1;
        } else if (auto* groupOp = op->As<TGroupOperator>()) {
            int maxChildDepth = getOperatorDepth(groupOp->GetSource());
            for (const auto& groupItem : groupOp->GroupItems()) {
                maxChildDepth = std::max(maxChildDepth, getExpressionDepth(groupItem.Expression) + 1);
            }
            for (const auto& aggregateItem : groupOp->AggregateItems()) {
                maxChildDepth = std::max(maxChildDepth, getExpressionDepth(aggregateItem.Expression) + 1);
            }
            return maxChildDepth + 1;
        }
        YUNREACHABLE();
    };

    if (getOperatorDepth(Head_) > PlanFragmentDepthLimit) {
        THROW_ERROR_EXCEPTION("Plan fragment depth limit exceeded");
    }
}

void TPrepareController::GetInitialSplits()
{
    Head_ = Apply(
        Context_.Get(),
        Head_,
        [this] (TPlanContext* context, const TOperator* op) -> const TOperator* {
            if (auto* scanOp = op->As<TScanOperator>()) {
                auto tablePath = Context_->GetTablePath();
                LOG_DEBUG("Getting initial data split for %v", tablePath);
                // XXX(sandello): We have just one table at the moment.
                // Will put TParallelAwaiter here in case of multiple tables.
                auto dataSplitOrError = WaitFor(Callbacks_->GetInitialSplit(
                    ~tablePath,
                    Context_));
                THROW_ERROR_EXCEPTION_IF_FAILED(
                    dataSplitOrError,
                    "Failed to get initial data split for table %v",
                    tablePath);

                auto* clonedScanOp = scanOp->Clone(context)->As<TScanOperator>();
                clonedScanOp->DataSplits().clear();
                clonedScanOp->DataSplits().push_back(dataSplitOrError.Value());
                return clonedScanOp;
            }
            return op;
        });
}

void TPrepareController::CheckAndPruneReferences()
{
    TCheckAndPruneReferences visitor;
    Traverse(&visitor, Head_);

    const auto& liveColumns = visitor.GetLiveColumns();

    Head_ = Apply(
        Context_.Get(),
        Head_,
        [&] (TPlanContext* context, const TOperator* op) -> const TOperator* {
            if (auto* scanOp = op->As<TScanOperator>()) {
                YCHECK(scanOp->DataSplits().size() == 1);

                auto schema = GetTableSchemaFromDataSplit(scanOp->DataSplits()[0]);
                auto& columns = schema.Columns();

                columns.erase(
                    std::remove_if(
                        columns.begin(),
                        columns.end(),
                        [&liveColumns] (const TColumnSchema& columnSchema) -> bool {
                            return liveColumns.find(columnSchema.Name) == liveColumns.end();
                        }),
                    columns.end());

                auto* clonedScanOp = scanOp->Clone(context)->As<TScanOperator>();
                SetTableSchema(&clonedScanOp->DataSplits()[0], schema);
                clonedScanOp->GetTableSchema(true);

                return clonedScanOp;
            }
            return op;
        }
    );
}

void TPrepareController::TypecheckExpressions()
{
    Visit(Head_, [this] (const TOperator* op)
    {
        if (auto* filterOp = op->As<TFilterOperator>()) {
            auto actualType = filterOp->GetPredicate()->GetType(filterOp->GetSource()->GetTableSchema());
            auto expectedType = EValueType(EValueType::Boolean);
            if (actualType != expectedType) {
                THROW_ERROR_EXCEPTION("WHERE-clause is not of a valid type")
                    << TErrorAttribute("actual_type", actualType)
                    << TErrorAttribute("expected_type", expectedType);
            }
        }
        if (auto* projectOp = op->As<TProjectOperator>()) {
            const auto& schema = projectOp->GetSource()->GetTableSchema();
            for (auto& projection : projectOp->Projections()) {
                projection.Expression->GetType(schema); // Force typechecking.
            }
        }
        if (auto* groupOp = op->As<TGroupOperator>()) {
            const auto& schema = groupOp->GetSource()->GetTableSchema();
            for (auto& groupItem : groupOp->GroupItems()) {
                groupItem.Expression->GetType(schema); // Force typechecking.
            }
            for (auto& aggregateItem : groupOp->AggregateItems()) {
                aggregateItem.Expression->GetType(schema); // Force typechecking.
            }
        }
    });

    Head_->GetTableSchema();
}

void TPrepareController::FillSelectAll()
{
    // Extract aggregate functions from projections and delegate
    // aggregation to the group operator.
    Head_ = Apply(
        Context_.Get(),
        Head_,
        [&] (TPlanContext* context, const TOperator* op) -> const TOperator* {
            auto* projectOp = op->As<TProjectOperator>();
            if (!projectOp || projectOp->Projections().size() != 0) {
                return op;
            }

            auto* newProjectOp = context->TrackedNew<TProjectOperator>(projectOp->GetSource());
            auto& newProjections = newProjectOp->Projections();
            auto tableSchema = projectOp->GetSource()->GetTableSchema();

            for (auto column : tableSchema.Columns()) {
                newProjections.push_back(TNamedExpression(context->TrackedNew<TReferenceExpression>(NullSourceLocation, column.Name), column.Name));
            }
            
            return newProjectOp;
    });
}

void TPrepareController::MoveAggregateExpressions()
{
    // Extract aggregate functions from projections and delegate
    // aggregation to the group operator.
    Head_ = Apply(
        Context_.Get(),
        Head_,
        [&] (TPlanContext* context, const TOperator* op) -> const TOperator* {
            auto* projectOp = op->As<TProjectOperator>();
            if (!projectOp) {
                return op;
            }
            auto* groupOp = projectOp->GetSource()->As<TGroupOperator>();
            if (!groupOp) {
                return op;
            }

            auto* newGroupOp = new (context) TGroupOperator(context, groupOp->GetSource());
            auto* newProjectOp = new (context) TProjectOperator(context, newGroupOp);

            newGroupOp->GroupItems() = groupOp->GroupItems();

            auto& newProjections = newProjectOp->Projections();
            auto& newAggregateItems = newGroupOp->AggregateItems();

            std::set<Stroka> subexprNames;

            for (auto& projection : projectOp->Projections()) {
                auto newExpr = Apply(
                    context,
                    projection.Expression,
                    [&] (TPlanContext* context, const TExpression* expr) -> const TExpression* {
                        if (auto* functionExpr = expr->As<TFunctionExpression>()) {
                            auto name = functionExpr->GetFunctionName();
                            EAggregateFunctions aggregateFunction;

                            if (name == "SUM") {
                                aggregateFunction = EAggregateFunctions::Sum;
                            } else if (name == "MIN") {
                                aggregateFunction = EAggregateFunctions::Min;
                            } else if (name == "MAX") {
                                aggregateFunction = EAggregateFunctions::Max;
                            } else if (name == "AVG") {
                                aggregateFunction = EAggregateFunctions::Average;
                            } else if (name == "COUNT") {
                                aggregateFunction = EAggregateFunctions::Count;
                            } else {
                                return expr;
                            }

                            if (functionExpr->GetArgumentCount() != 1) {
                                THROW_ERROR_EXCEPTION(
                                    "Aggregate function %v must have exactly one argument",
                                    aggregateFunction)
                                    << TErrorAttribute("source", functionExpr->GetSource());
                            }

                            Stroka subexprName = functionExpr->GetName();

                            if (!subexprNames.count(subexprName)) {
                                subexprNames.insert(subexprName);
                                newAggregateItems.push_back(TAggregateItem(
                                    functionExpr->GetArgument(0),
                                    aggregateFunction,
                                    subexprName));
                            }

                            auto referenceExpr = new (context) TReferenceExpression(
                                context,
                                NullSourceLocation,
                                subexprName);
                            
                            return referenceExpr;
                        }
                        return expr;
                    });
                newProjections.push_back(TNamedExpression(newExpr, projection.Name));
            }

            return newProjectOp;
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

