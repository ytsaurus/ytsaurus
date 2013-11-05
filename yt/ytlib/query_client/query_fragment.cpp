#include "query_fragment.h"

#include "private.h"
#include "helpers.h"

#include "callbacks.h"

#include "ast.h"
#include "ast_visitor.h"

#include "lexer.h"
#include "parser.hpp"

#include <ytlib/table_client/chunk_meta_extensions.h>
#include <ytlib/new_table_client/chunk_meta_extensions.h>

#include <ytlib/new_table_client/schema.h>

#include <ytlib/query_client/query_fragment.pb.h>

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
    const TOperator* Head_;

};

////////////////////////////////////////////////////////////////////////////////

namespace {

class TCheckAndPruneReferences
    : public TAstVisitor
{
public:
    explicit TCheckAndPruneReferences(TPrepareController* controller)
        : Controller_(controller)
    {
        LiveColumns_.resize(Controller_->GetContext()->GetTableCount());
    }

    virtual bool Visit(const TScanOperator* op) override
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
                op->AsMutable<TScanOperator>()->DataSplit().mutable_extensions(),
                filteredTableSchema);
        }

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
        auto& descriptor = Controller_->GetContext()
            ->GetTableDescriptorByIndex(expr->GetTableIndex());

        TScanOperator* op = reinterpret_cast<TScanOperator*>(descriptor.Opaque);
        YCHECK(op);

        auto* mutableExpr = expr->AsMutable<TReferenceExpression>();

        const auto keyColumns = GetKeyColumnsFromDataSplit(op->DataSplit());
        const auto tableSchema = GetTableSchemaFromDataSplit(op->DataSplit());

        {
            auto column = tableSchema.FindColumn(expr->GetName());

            if (!column) {
                THROW_ERROR_EXCEPTION(
                    "Table %s does not have column %s in its schema",
                    ~descriptor.Path,
                    ~expr->GetName().Quote());
            }

            mutableExpr->SetCachedType(column->Type);
        }

        {
            auto it = std::find_if(
                keyColumns.begin(),
                keyColumns.end(),
                [&expr] (const Stroka& name) {
                    return expr->GetName() == name;
                });

            if (it != keyColumns.end()) {
                mutableExpr->SetCachedKeyIndex(std::distance(keyColumns.begin(), it));
            } else {
                mutableExpr->SetCachedKeyIndex(-1);
            }
        }

        LiveColumns_[expr->GetTableIndex()].insert(expr->GetName());

        return true;
    }

private:
    TPrepareController* Controller_;
    std::vector<std::set<Stroka>> LiveColumns_;

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
            auto actualType = typedOp->GetPredicate()->Typecheck();
            auto expectedType = EColumnType(EColumnType::Integer);
            if (actualType != expectedType) {
                THROW_ERROR_EXCEPTION("WHERE-clause is not of valid type")
                    << TErrorAttribute("actual_type", actualType)
                    << TErrorAttribute("expected_type", expectedType);
            }
        }
        if (auto* typedOp = op->As<TProjectOperator>()) {
            for (auto& projection : typedOp->Projections()) {
                projection->Typecheck();
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TQueryFragment::TQueryFragment(
    TQueryContextPtr context,
    const TOperator* head,
    const TGuid& guid)
    : Context_(std::move(context))
    , Head_(head)
    , Guid_(guid)
{ }

TQueryFragment::TQueryFragment(const TQueryFragment& other)
    : Context_(other.Context_)
    , Head_(other.Head_)
    , Guid_(other.Guid_)
{ }

TQueryFragment::TQueryFragment(TQueryFragment&& other)
    : Context_(std::move(other.Context_))
    , Head_(std::move(other.Head_))
    , Guid_(std::move(other.Guid_))
{ }

TQueryFragment::~TQueryFragment()
{ }

TQueryFragment PrepareQueryFragment(
    IPrepareCallbacks* callbacks,
    const Stroka& source)
{
    return TPrepareController(callbacks, source).Run();
}

void ToProto(NProto::TQueryFragment* serialized, const TQueryFragment& fragment)
{
    ToProto(serialized->mutable_head(), fragment.GetHead());
    ToProto(serialized->mutable_guid(), fragment.Guid());
}

TQueryFragment FromProto(const NProto::TQueryFragment& serialized)
{
    auto context = New<TQueryContext>();
    return TQueryFragment(
        context,
        FromProto(serialized.head(), context.Get()),
        NYT::FromProto<TGuid>(serialized.guid()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

