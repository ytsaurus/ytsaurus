#include "query_fragment.h"

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

namespace {

class TPrepareController
{
public:
    TPrepareController(IPrepareCallbacks* callbacks, TQueryFragment* fragment)
        : Callbacks_(callbacks)
        , Context_(fragment->GetContext().Get())
        , Fragment_(fragment)
    { }

    void Run(const Stroka& source);
    void Parse(const Stroka& source);

    void GetInitialSplit();
    void CheckAndPruneReferences();
    void TypecheckExpressions();

    DEFINE_BYVAL_RO_PROPERTY(IPrepareCallbacks*, Callbacks);
    DEFINE_BYVAL_RO_PROPERTY(TQueryContext*, Context);
    DEFINE_BYVAL_RO_PROPERTY(TQueryFragment*, Fragment);

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

        LOG_DEBUG("Fetching initial split for %s", ~descriptor.Path);

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
        auto tableSchema = GetProtoExtension<NVersionedTableClient::NProto::TTableSchema>(op->DataSplit().extensions());
        auto& liveColumns = LiveColumns_[op->GetTableIndex()];

        {
            NVersionedTableClient::NProto::TTableSchema filteredTableSchema;
            for (const auto& columnSchema : tableSchema.columns()) {
                if (liveColumns.find(columnSchema.name()) != liveColumns.end()) {
                    LOG_DEBUG("Keeping column %s in the schema", ~columnSchema.name().Quote());
                    filteredTableSchema.add_columns()->CopyFrom(columnSchema);
                } else {
                    LOG_DEBUG("Prunning column %s from the schema", ~columnSchema.name().Quote());
                }
            }
            SetProtoExtension<NVersionedTableClient::NProto::TTableSchema>(
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
        const auto tableSchema = GetProtoExtension<NVersionedTableClient::NProto::TTableSchema>(op->DataSplit().extensions());

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
                keyColumns.values().begin(),
                keyColumns.values().end(),
                [&expr] (const Stroka& name) {
                    return expr->GetName() == name;
                });

            if (it != keyColumns.values().end()) {
                expr->SetKeyIndex(std::distance(keyColumns.values().begin(), it));
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
    explicit TTypecheckExpressions(TPrepareController* /*controller*/)
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

void TPrepareController::Run(const Stroka& source)
{
    Parse(source);
    GetInitialSplit();
    CheckAndPruneReferences();
    TypecheckExpressions();
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

void TPrepareController::CheckAndPruneReferences()
{
    TCheckAndPruneReferences visitor(this);
    Traverse(&visitor, Fragment_->GetHead());
}

void TPrepareController::TypecheckExpressions()
{
    TTypecheckExpressions visitor(this);
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
    IPrepareCallbacks* callbacks,
    const Stroka& source)
{
    TQueryFragment fragment(New<TQueryContext>());
    TPrepareController controller(callbacks, &fragment);

    controller.Run(source);

    return fragment;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

