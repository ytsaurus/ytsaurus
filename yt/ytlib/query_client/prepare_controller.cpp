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
class TCheckAndBindReferences
    : public TPlanVisitor
{
public:
    explicit TCheckAndBindReferences(TPrepareController* controller)
        : Controller_(controller)
    {
        Mode_ = Check;
        LiveColumns_.resize(Controller_->GetContext()->GetTableCount());
    }

    enum { Check, Bind } Mode_;

    virtual bool Visit(const TScanOperator* op) override
    {
        currentSourceSchema_ = op->GetTableSchema();
        if (Mode_ != Check) {
            return true;
        }

        // Scan operators are always visited in the end,
        // because they are leaf nodes.
        auto tableSchema = GetTableSchemaFromDataSplit(op->DataSplit());
        auto& columnSchemas = tableSchema.Columns();
        auto& liveColumns = LiveColumns_[op->GetTableIndex()];

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
        currentSourceSchema_ = op->GetSource()->GetTableSchema();
        Traverse(this, op->GetPredicate());
        return true;
    }

    virtual bool Visit(const TGroupByOperator* op) override
    {
        currentSourceSchema_ = op->GetSource()->GetTableSchema();
        for (auto& groupItem : op->GroupItems()) {
            Traverse(this, groupItem.first);
        }

        for (auto& aggregateItem : op->AggregateItems()) {
            Traverse(this, aggregateItem.Expression);
        }
        return true;
    }

    virtual bool Visit(const TProjectOperator* op) override
    {
        currentSourceSchema_ = op->GetSource()->GetTableSchema();
        for (auto& projection : op->Projections()) {
            Traverse(this, projection.first);
        }
        return true;
    }

    virtual bool Visit(const TReferenceExpression* expr) override
    {
        const auto name = expr->GetColumnName();
        switch (Mode_) {
            case Check: {
                auto column = currentSourceSchema_.FindColumn(name);
                if (!column) {
                    THROW_ERROR_EXCEPTION(
                        "Undefined reference %s",
                        ~name.Quote());
                }
                LiveColumns_[expr->GetTableIndex()].insert(name);
                break;
            }
            case Bind: 
                YUNREACHABLE();
        }

        return true;
    }

private:
    TPrepareController* Controller_;
    std::vector<std::set<Stroka>> LiveColumns_;
    TTableSchema currentSourceSchema_;

};
} // anonymous namespace

TPlanFragment TPrepareController::Run()
{
    ParseSource();
    GetInitialSplits();
    MoveAggregateExpressions();
    CheckAndBindReferences();
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
            auto tableIndex = scanOp->GetTableIndex();
            auto& tableDescriptor = GetContext()->GetTableDescriptorByIndex(tableIndex);
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

void TPrepareController::CheckAndBindReferences()
{
    TCheckAndBindReferences visitor(this);
    visitor.Mode_ = TCheckAndBindReferences::Check;
    Traverse(&visitor, Head_);
    //TODO(lukyan): remove bind mode from TCheckAndBindReferences
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
                projection.first->GetType(typedOp->GetSource()->GetTableSchema()); // Force typechecking.
            }
        }
    });
}

void TPrepareController::MoveAggregateExpressions()
{
    TGroupByOperator::TAggregateItemList aggregateItems;

    Head_ = Apply(GetContext(), Head_, [&] (TPlanContext* context, const TOperator* op) -> const TOperator*
    {
        if (auto* typedOp = op->As<TProjectOperator>()) {
            auto* newProjectOp = new (context) TProjectOperator(context, typedOp->GetSource());
            auto& newProjections = newProjectOp->Projections();

            for (auto& projection : typedOp->Projections()) {
                auto newExpr = Apply(context, projection.first, [&] (TPlanContext* context, const TExpression* expr) -> const TExpression*
                {
                    if (auto* functionExpr = expr->As<TFunctionExpression>()) {
                        if (functionExpr->GetFunctionName() == "sum" && functionExpr->GetArgumentCount() == 1) {
                            
                            Stroka subexprName = EAggregateFunctions(EAggregateFunctions::Sum).ToString() 
                                + " " + functionExpr->GetArgument(0)->GetName(); // TODO(lukyan): wrap into function
                            auto referenceExpr = new (context) TReferenceExpression(
                                context, 
                                NullSourceLocation, 
                                context->GetTableIndexByAlias(""), 
                                subexprName);
                            aggregateItems.push_back(TAggregateItem(functionExpr->GetArgument(0), EAggregateFunctions::Sum, subexprName));
                            return referenceExpr;
                        }
                    }

                    return expr;
                });

                newProjections.push_back(std::make_pair(newExpr, projection.second));
            }

            return newProjectOp;
        }
        return op;
    });

    Head_ = Apply(GetContext(), Head_, [&] (TPlanContext* context, const TOperator* op) -> const TOperator*
    {
        if (auto* typedOp = op->As<TGroupByOperator>()) {
            auto* newGroupByOp = new (context) TGroupByOperator(context, typedOp->GetSource());

            newGroupByOp->AggregateItems().swap(aggregateItems);
            newGroupByOp->GroupItems() = typedOp->GroupItems();

            return newGroupByOp;
        }
        return op;
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

