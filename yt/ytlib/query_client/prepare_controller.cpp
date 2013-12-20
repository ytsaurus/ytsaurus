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
        Traverse(this, op->GetPredicate());
        return true;
    }

    virtual bool Visit(const TProjectOperator* op) override
    {
        for (auto& projection : op->Projections()) {
            Traverse(this, projection);
        }
        return true;
    }

    virtual bool Visit(const TReferenceExpression* expr) override
    {
        const auto& descriptor = Controller_->GetContext()
            ->GetTableDescriptorByIndex(expr->GetTableIndex());
        TScanOperator* op = reinterpret_cast<TScanOperator*>(descriptor.Opaque);
        YCHECK(op);

        const auto name = expr->GetColumnName();

        const auto keyColumns = GetKeyColumnsFromDataSplit(op->DataSplit());
        const auto tableSchema = GetTableSchemaFromDataSplit(op->DataSplit());

        switch (Mode_) {
            case Check: {
                auto column = tableSchema.FindColumn(name);
                if (!column) {
                    THROW_ERROR_EXCEPTION(
                        "Table %s does not have column %s in its schema",
                        ~descriptor.Path,
                        ~name.Quote());
                }
                LiveColumns_[expr->GetTableIndex()].insert(name);
                break;
            }
            case Bind: {
                auto* mutableExpr = expr->AsMutable<TReferenceExpression>();
                const auto& column = tableSchema.GetColumnOrThrow(name);

                mutableExpr->SetCachedType(column.Type);
                mutableExpr->SetIndexInRow(tableSchema.GetColumnIndex(column));

                auto it = std::find_if(
                    keyColumns.begin(),
                    keyColumns.end(),
                    [&name] (const Stroka& keyColumnName) {
                        return name == keyColumnName;
                    });

                if (it != keyColumns.end()) {
                    mutableExpr->SetIndexInKey(
                        std::distance(keyColumns.begin(), it));
                }
            }
        }

        return true;
    }

private:
    TPrepareController* Controller_;
    std::vector<std::set<Stroka>> LiveColumns_;

};
} // anonymous namespace

TPlanFragment TPrepareController::Run()
{
    ParseSource();
    GetInitialSplits();
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
    visitor.Mode_ = TCheckAndBindReferences::Bind;
    Traverse(&visitor, Head_);
}

void TPrepareController::TypecheckExpressions()
{
    Visit(Head_, [this] (const TOperator* op)
    {
        if (auto* typedOp = op->As<TFilterOperator>()) {
            auto actualType = typedOp->GetPredicate()->GetType();
            auto expectedType = EValueType(EValueType::Integer);
            if (actualType != expectedType) {
                THROW_ERROR_EXCEPTION("WHERE-clause is not of valid type")
                    << TErrorAttribute("actual_type", actualType)
                    << TErrorAttribute("expected_type", expectedType);
            }
        }
        if (auto* typedOp = op->As<TProjectOperator>()) {
            for (auto& projection : typedOp->Projections()) {
                projection->GetType(); // Force typechecking.
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

