#include "query_fragment.h"

#include "ast.h"
#include "ast_visitor.h"

#include <ytlib/query_client/lexer.h>
#include <ytlib/query_client/parser.hpp>

#include <ytlib/table_client/chunk_meta_extensions.h>
#include <ytlib/new_table_client/chunk_meta_extensions.h>

#include <core/ytree/convert.h>

#include <core/concurrency/fiber.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NYT::NYPath;
using namespace NYT::NConcurrency;

namespace {

class TPrepareController
{
public:
    explicit TPrepareController(IPreparationHooks* hooks, TQueryFragment* fragment)
        : Hooks_(hooks)
        , Fragment_(fragment)
        , Context_(fragment->GetContext().Get())
    { }

    void Run(const Stroka& source);
    void Parse(const Stroka& source);

    void GetInitialSplit();
    void CheckReferences();
    void Typecheck();

    DEFINE_BYVAL_RO_PROPERTY(IPreparationHooks*, Hooks);
    DEFINE_BYVAL_RO_PROPERTY(TQueryFragment*, Fragment);
    DEFINE_BYVAL_RO_PROPERTY(TQueryContext*, Context);

};

class TGetInitialSplit
    : public TAstVisitor
{
public:
    explicit TGetInitialSplit(TPrepareController* controller)
        : Controller_(controller)
    { }

    virtual bool Visit(TScanOperator* op) override
    {
        auto& descriptor = Controller_->GetContext()
            ->GetTableDescriptorByIndex(op->GetTableIndex());

        // XXX(sandello): We have just one table at the moment.
        // Will put TParallelAwaiter here in case of multiple tables.
        auto dataSplitOrError = WaitFor(Controller_->GetHooks()
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

class TCheckReferences
    : public TAstVisitor
{
public:
    explicit TCheckReferences(TPrepareController* controller)
        : Controller_(controller)
    { }

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

        auto keyColumns = GetProtoExtension<NTableClient::NProto::TKeyColumnsExt>(op->DataSplit().extensions());
        auto tableSchema = GetProtoExtension<NVersionedTableClient::NProto::TTableSchema>(op->DataSplit().extensions());

        {
            auto it = std::find_if(
                keyColumns.values().begin(),
                keyColumns.values().end(),
                [&expr] (const Stroka& name) {
                    return expr->GetName() == name;
                });

            if (it != keyColumns.values().end()) {
                // TODO(sandello): Mark as key column.
            }
        }

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
                    ~EscapeC(expr->GetName().c_str(), expr->GetName().length()))
                    << TErrorAttribute("expression", expr->GetSource())
                    << TErrorAttribute("enclosing_expression", enclosingExpr->GetSource());
            }

            expr->SetType(EColumnType(it->type()));
        }

        return true;
    }

private:
    TPrepareController* Controller_;

};

class TTypecheck
    : public TAstVisitor
{
public:
    explicit TTypecheck(TPrepareController* /*controller*/)
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
        for (auto& expression: op->Expressions()) {
            expression->Typecheck();
        }
        return true;
    }

};

void TPrepareController::Run(const Stroka& source)
{
    Parse(source);
    GetInitialSplit();
    CheckReferences();
    Typecheck();
}

void TPrepareController::Parse(const Stroka& source)
{
    // Hook up with debug information for better error messages.
    Context_->SetDebugInformation(TDebugInformation(source));

    TOperator* head;

    TLexer lexer(Context_, source);
    TParser parser(lexer, Context_, &head);

    int result = parser.parse();
    if (result != 0) {
        THROW_ERROR_EXCEPTION("Failed to parse query");
    }

    Fragment_->SetHead(head);
}

void TPrepareController::GetInitialSplit()
{
    TGetInitialSplit visitor(this);
    Traverse(&visitor, Fragment_->GetHead());
}

void TPrepareController::CheckReferences()
{
    TCheckReferences visitor(this);
    Traverse(&visitor, Fragment_->GetHead());
}

void TPrepareController::Typecheck()
{
    TTypecheck visitor(this);
    Traverse(&visitor, Fragment_->GetHead());
}

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////

TQueryFragment::TQueryFragment(TQueryContextPtr context, TOperator* head)
    : Context_(std::move(context))
    , Head_(head)
{ }

TQueryFragment::~TQueryFragment()
{ }

TQueryFragment PrepareQueryFragment(
    IPreparationHooks* hooks,
    const Stroka& source)
{
    TQueryFragment fragment(New<TQueryContext>());
    TPrepareController controller(hooks, &fragment);

    controller.Run(source);

    return fragment;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

