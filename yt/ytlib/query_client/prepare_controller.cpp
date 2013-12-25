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

#include <core/concurrency/fiber.h>

namespace NYT {
namespace NQueryClient {

using namespace NYPath;
using namespace NConcurrency;
using namespace NVersionedTableClient;

static auto& Logger = QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

TPrepareController::TPrepareController(
    IPrepareCallbacks* callbacks,
    const Stroka& source)
    : Callbacks_(callbacks)
    , Source_(source)
    , Context_(New<TPlanContext>())
    , Head_(nullptr)
{ }

TPrepareController::~TPrepareController()
{ }

namespace {
class TCheckAndPruneReferences
    : public TPlanVisitor
{
public:
    explicit TCheckAndPruneReferences(TPrepareController* controller)
        : Controller_(controller)
    { }

    virtual bool Visit(const TScanOperator* op) override
    {
        CurrentSourceSchema_ = op->GetTableSchema();

        // Scan operators are always visited in the end,
        // because they are leaf nodes.
        auto tableSchema = GetTableSchemaFromDataSplit(op->DataSplit());
        auto& columnSchemas = tableSchema.Columns();
        auto& liveColumns = LiveColumns_;

        columnSchemas.erase(
            std::remove_if(
                columnSchemas.begin(),
                columnSchemas.end(),
                [&liveColumns] (const TColumnSchema& columnSchema) -> bool {
                    if (liveColumns.find(columnSchema.Name) != liveColumns.end()) {
                        LOG_DEBUG(
                            "Keeping column %s in the table schema",
                            ~columnSchema.Name.Quote());
                        return false;
                    } else {
                        LOG_DEBUG(
                            "Prunning column %s from the table schema",
                            ~columnSchema.Name.Quote());
                        return true;
                    }
                }),
            columnSchemas.end());

        auto* mutableOp = op->AsMutable<TScanOperator>();
        SetTableSchema(&mutableOp->DataSplit(), tableSchema);

        return true;
    }

    virtual bool Visit(const TFilterOperator* op) override
    {
        CurrentSourceSchema_ = op->GetSource()->GetTableSchema();
        Traverse(this, op->GetPredicate());
        return true;
    }

    virtual bool Visit(const TGroupOperator* op) override
    {
        CurrentSourceSchema_ = op->GetSource()->GetTableSchema();
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

private:
    TPrepareController* Controller_;
    std::set<Stroka> LiveColumns_;
    TTableSchema CurrentSourceSchema_;

};
} // anonymous namespace

TPlanFragment TPrepareController::Run()
{
    ParseSource();
    GetInitialSplits();
    MoveAggregateExpressions();
    CheckAndPruneReferences();
    TypecheckExpressions();
    return TPlanFragment(std::move(Context_), Head_);
}

void TPrepareController::ParseSource()
{
    // Hook up with debug information for better error messages.
    GetContext()->SetDebugInformation(TDebugInformation(Source_));

    TLexer lexer(GetContext(), Source_);
    TParser parser(lexer, GetContext(), &Head_);

    int result = parser.parse();
    if (result != 0) {
        THROW_ERROR_EXCEPTION("Failed to parse query");
    }
}

void TPrepareController::GetInitialSplits()
{
    Visit(Head_, [this] (const TOperator* op)
    {
        if (auto* scanOp = op->AsMutable<TScanOperator>()) {
            auto& tableDescriptor = GetContext()->GetTableDescriptor();
            LOG_DEBUG("Getting initial data split for %s", ~tableDescriptor.Path);
            // XXX(sandello): We have just one table at the moment.
            // Will put TParallelAwaiter here in case of multiple tables.
            auto dataSplitOrError = WaitFor(
                GetCallbacks()->GetInitialSplit(tableDescriptor.Path));
            THROW_ERROR_EXCEPTION_IF_FAILED(
                dataSplitOrError,
                "Failed to get initial data split for table %s",
                ~tableDescriptor.Path);
            scanOp->DataSplit() = dataSplitOrError.GetValue();
        }
    });
}

void TPrepareController::CheckAndPruneReferences()
{
    TCheckAndPruneReferences visitor(this);
    Traverse(&visitor, Head_);
}

void TPrepareController::TypecheckExpressions()
{
    Visit(Head_, [this] (const TOperator* op)
    {
        if (auto* typedOp = op->As<TFilterOperator>()) {
            auto actualType = typedOp->GetPredicate()->GetType(typedOp->GetSource()->GetTableSchema());
            auto expectedType = EValueType(EValueType::Integer);
            if (actualType != expectedType) {
                THROW_ERROR_EXCEPTION("WHERE-clause is not of valid type")
                    << TErrorAttribute("actual_type", actualType)
                    << TErrorAttribute("expected_type", expectedType);
            }
        }
        if (auto* typedOp = op->As<TProjectOperator>()) {
            for (auto& projection : typedOp->Projections()) {
                projection.Expression->GetType(typedOp->GetSource()->GetTableSchema()); // Force typechecking.
            }
        }
    });
}

void TPrepareController::MoveAggregateExpressions()
{
    // Extract aggregate functions from projections and delegate
    // aggregation to the group operator.
    Head_ = Apply(GetContext(), Head_,
    [&] (TPlanContext* context, const TOperator* op) -> const TOperator*
    {
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

        for (auto& projection : projectOp->Projections()) {
            auto newExpr = Apply(
                context,
                projection.Expression,
                [&] (TPlanContext* context, const TExpression* expr) -> const TExpression*
            {
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
                            "Aggregate function %s must have exactly one argument",
                            ~aggregateFunction.ToString())
                            << TErrorAttribute("source", functionExpr->GetSource());
                    }

                    Stroka subexprName = functionExpr->GetName();
                    auto referenceExpr = new (context) TReferenceExpression(
                        context,
                        NullSourceLocation,
                        subexprName);
                    newAggregateItems.push_back(TAggregateItem(
                        functionExpr->GetArgument(0),
                        aggregateFunction,
                        subexprName));
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

