#include "query_fragment.h"
#include "query_context.h"

#include "ast.h"
#include "ast_visitor.h"

#include "lexer.h"
#include "parser.hpp"

#include "private.h"

#include <ytlib/table_client/chunk_meta_extensions.h>
#include <ytlib/new_table_client/chunk_meta_extensions.h>

#include <core/ytree/convert.h>

#include <core/concurrency/fiber.h>

namespace NYT {
namespace NQueryClient {

using namespace NYT::NYPath;
using namespace NYT::NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = QueryClientLogger;

class TPrepareController
{
public:
    TPrepareController(
        IPrepareCallbacks* callbacks,
        const Stroka& source)
        : Callbacks_(callbacks)
        , Source_(source)
        , Context_(New<TQueryContext>())
        , Head_(nullptr)
    { }

    TQueryFragment Run();

    void ParseSource();
    void GetInitialSplits();
    void CheckAndPruneReferences();
    void TypecheckExpressions();

    IPrepareCallbacks* GetCallbacks()
    {
        return Callbacks_;
    }

    TQueryContext* GetContext()
    {
        return Context_.Get();
    }

private:
    IPrepareCallbacks* Callbacks_;
    const Stroka& Source_;
    TQueryContextPtr Context_;
    TOperator* Head_;

};

////////////////////////////////////////////////////////////////////////////////

namespace {

class TGetInitialSplits
    : public TAstVisitor
{
public:
    explicit TGetInitialSplits(TPrepareController* controller)
        : Controller_(controller)
    { }

    virtual bool Visit(TScanOperator* op) override
    {
        auto& descriptor = Controller_->GetContext()
            ->GetTableDescriptorByIndex(op->GetTableIndex());

        LOG_DEBUG("Getting initial data split for %s", ~descriptor.Path);

        // XXX(sandello): We have just one table at the moment.
        // Will put TParallelAwaiter here in case of multiple tables.
        auto dataSplitOrError = WaitFor(Controller_->GetCallbacks()
            ->GetInitialSplit(descriptor.Path));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            dataSplitOrError,
            "Failed to get initial data split for table %s",
            ~descriptor.Path);

        op->DataSplit() = dataSplitOrError.GetValue();

        return true;
    }

private:
    TPrepareController* Controller_;

};

class TCheckAndPruneReferences
    : public TAstVisitor
{
public:
    explicit TCheckAndPruneReferences(TPrepareController* controller)
        : Controller_(controller)
    {
        LiveColumns_.resize(Controller_->GetContext()->GetTableCount());
    }

    virtual bool Visit(TScanOperator* op) override
    {
        // Scan operators are always visited in the end,
        // because they are leaf nodes.
        auto tableSchema = GetProtoExtension<NVersionedTableClient::NProto::TTableSchemaExt>(op->DataSplit().extensions());
        auto& liveColumns = LiveColumns_[op->GetTableIndex()];

        {
            NVersionedTableClient::NProto::TTableSchemaExt filteredTableSchema;
            for (const auto& columnSchema : tableSchema.columns()) {
                if (liveColumns.find(columnSchema.name()) != liveColumns.end()) {
                    LOG_DEBUG("Keeping column %s in the schema", ~columnSchema.name().Quote());
                    filteredTableSchema.add_columns()->CopyFrom(columnSchema);
                } else {
                    LOG_DEBUG("Prunning column %s from the schema", ~columnSchema.name().Quote());
                }
            }
            SetProtoExtension<NVersionedTableClient::NProto::TTableSchemaExt>(
                op->DataSplit().mutable_extensions(),
                filteredTableSchema);
        }

        return true;
    }

    virtual bool Visit(TFilterOperator* op) override
    {
        Traverse(this, op->GetPredicate());
        return true;
    }

    virtual bool Visit(TProjectOperator* op) override
    {
        for (auto& expression : op->Expressions()) {
            Traverse(this, expression);
        }
        return true;
    }

    virtual bool Visit(TReferenceExpression* expr) override
    {
        auto& descriptor = Controller_->GetContext()
            ->GetTableDescriptorByIndex(expr->GetTableIndex());

        TScanOperator* op = reinterpret_cast<TScanOperator*>(descriptor.Opaque);
        YCHECK(op);

        const auto keyColumns = GetProtoExtension<NTableClient::NProto::TKeyColumnsExt>(op->DataSplit().extensions());
        const auto tableSchema = GetProtoExtension<NVersionedTableClient::NProto::TTableSchemaExt>(op->DataSplit().extensions());

        {
            auto it = std::find_if(
                tableSchema.columns().begin(),
                tableSchema.columns().end(),
                [&expr] (const NVersionedTableClient::NProto::TColumnSchema& columnSchema) {
                    return expr->GetName() == columnSchema.name();
                });

            if (it == tableSchema.columns().end()) {
                TExpression* enclosingExpr = expr;
                while (enclosingExpr->Parent()) {
                    enclosingExpr = enclosingExpr->Parent();
                }

                THROW_ERROR_EXCEPTION(
                    "Table %s does not have column %s in its schema",
                    ~descriptor.Path,
                    ~expr->GetName().Quote())
                    << TErrorAttribute("enclosing_expression", enclosingExpr->GetSource());
            }

            expr->SetType(EColumnType(it->type()));
        }

        {
            auto it = std::find_if(
                keyColumns.names().begin(),
                keyColumns.names().end(),
                [&expr] (const Stroka& name) {
                    return expr->GetName() == name;
                });

            if (it != keyColumns.names().end()) {
                expr->SetKeyIndex(std::distance(keyColumns.names().begin(), it));
            } else {
                expr->SetKeyIndex(-1);
            }
        }

        LiveColumns_[expr->GetTableIndex()].insert(expr->GetName());

        return true;
    }

private:
    TPrepareController* Controller_;
    std::vector<std::set<Stroka>> LiveColumns_;

};

class TTypecheckExpressions
    : public TAstVisitor
{
public:
    explicit TTypecheckExpressions(TPrepareController* /* controller */)
    { }

    virtual bool Visit(TFilterOperator* op) override
    {
        auto actualType = op->GetPredicate()->Typecheck();
        auto expectedType = EColumnType(EColumnType::Integer);
        if (actualType != expectedType) {
            THROW_ERROR_EXCEPTION("WHERE-clause is not of valid type")
                << TErrorAttribute("actual_type", actualType)
                << TErrorAttribute("expected_type", expectedType);
        }
        return true;
    }

    virtual bool Visit(TProjectOperator* op) override
    {
        for (auto& expression : op->Expressions()) {
            expression->Typecheck();
        }
        return true;
    }

};

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////

TQueryFragment TPrepareController::Run()
{
    ParseSource();
    GetInitialSplits();
    CheckAndPruneReferences();
    TypecheckExpressions();
    return TQueryFragment(std::move(Context_), Head_);
}

void TPrepareController::ParseSource()
{
    // Hook up with debug information for better error messages.
    Context_->SetDebugInformation(TDebugInformation(Source_));

    TLexer lexer(Context_.Get(), Source_);
    TParser parser(lexer, Context_.Get(), &Head_);

    int result = parser.parse();
    if (result != 0) {
        THROW_ERROR_EXCEPTION("Failed to parse query");
    }
}

void TPrepareController::GetInitialSplits()
{
    TGetInitialSplits visitor(this);
    Traverse(&visitor, Head_);
}

void TPrepareController::CheckAndPruneReferences()
{
    TCheckAndPruneReferences visitor(this);
    Traverse(&visitor, Head_);
}

void TPrepareController::TypecheckExpressions()
{
    TTypecheckExpressions visitor(this);
    Traverse(&visitor, Head_);
}

////////////////////////////////////////////////////////////////////////////////

TQueryFragment::TQueryFragment(TQueryContextPtr context, TOperator* head)
    : Context_(std::move(context))
    , Head_(head)
{ }

TQueryFragment::~TQueryFragment()
{ }

TQueryFragment PrepareQueryFragment(
    IPrepareCallbacks* callbacks,
    const Stroka& source)
{
    return TPrepareController(callbacks, source).Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

