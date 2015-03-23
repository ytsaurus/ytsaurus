#include "stdafx.h"
#include "folding_profiler.h"
#include "plan_helpers.h"
#include "function_registry.h"
#include "functions.h"

#include "cg_fragment_compiler.h"

namespace NYT {
namespace NQueryClient {

// Folding profiler computes a strong structural hash used to cache query fragments.

DEFINE_ENUM(EFoldingObjectType,
    (ScanOp)
    (JoinOp)
    (FilterOp)
    (GroupOp)
    (ProjectOp)

    (LiteralExpr)
    (ReferenceExpr)
    (FunctionExpr)
    (UnaryOpExpr)
    (BinaryOpExpr)
    (InOpExpr)

    (NamedExpression)
    (AggregateItem)

    (TableSchema)
);

class TFoldingProfiler
    : private TNonCopyable
{
public:
    TFoldingProfiler(const TFunctionRegistry& functionRegistry);

    TCodegenSource Profile(const TConstQueryPtr& query);
    TCodegenExpression Profile(const TConstExpressionPtr& expr, const TTableSchema& tableSchema);
    void Profile(const TTableSchema& tableSchema, int keySize = std::numeric_limits<int>::max());

    TFoldingProfiler& Set(llvm::FoldingSetNodeID* id);
    TFoldingProfiler& Set(TCGVariables* variables);
    TFoldingProfiler& Set(yhash_set<Stroka>* references);

private:
    TCodegenExpression Profile(const TNamedItem& namedExpression, const TTableSchema& schema);
    std::pair<TCodegenExpression, TCodegenAggregate> Profile(const TAggregateItem& aggregateItem, const TTableSchema& schema);

    void Fold(int numeric);
    void Fold(const char* str);
    void Refer(const TReferenceExpression* referenceExpr);

    llvm::FoldingSetNodeID* Id_ = nullptr;
    TCGVariables* Variables_ = nullptr;
    yhash_set<Stroka>* References_ = nullptr;
    const TFunctionRegistry& FunctionRegistry_;
};

////////////////////////////////////////////////////////////////////////////////

TFoldingProfiler::TFoldingProfiler(
    const TFunctionRegistry& functionRegistry)
    : FunctionRegistry_(functionRegistry)
{ }

TFoldingProfiler& TFoldingProfiler::Set(llvm::FoldingSetNodeID* id)
{
    Id_ = id;
    return *this;
}

TFoldingProfiler& TFoldingProfiler::Set(TCGVariables* variables)
{
    Variables_ = variables;
    return *this;
}

TFoldingProfiler& TFoldingProfiler::Set(yhash_set<Stroka>* references)
{
    References_ = references;
    return *this;
}

TCodegenSource TFoldingProfiler::Profile(const TConstQueryPtr& query)
{
    Fold(static_cast<int>(EFoldingObjectType::ScanOp));
    Profile(query->TableSchema);
    TCodegenSource codegenSource = &CodegenScanOp;

    TTableSchema schema = query->TableSchema;

    if (auto joinClause = query->JoinClause.Get()) {
        Fold(static_cast<int>(EFoldingObjectType::JoinOp));

        Profile(schema);
        Profile(joinClause->ForeignTableSchema);

        for (const auto& column : joinClause->JoinColumns) {
            Fold(column.c_str());
        }

        if (auto selfFilter = ExtractPredicateForColumnSubset(query->WhereClause, schema)) {
            codegenSource = MakeCodegenFilterOp(Profile(selfFilter, schema), std::move(codegenSource));
        }

        codegenSource = MakeCodegenJoinOp(joinClause->JoinColumns, schema, std::move(codegenSource));

        schema = joinClause->JoinedTableSchema;
    }

    if (query->WhereClause) {
        Fold(static_cast<int>(EFoldingObjectType::FilterOp));
        codegenSource = MakeCodegenFilterOp(Profile(query->WhereClause, schema), std::move(codegenSource));
    }

    if (auto groupClause = query->GroupClause.Get()) {
        Fold(static_cast<int>(EFoldingObjectType::GroupOp));

        std::vector<TCodegenExpression> codegenGroupExprs;
        std::vector<std::pair<TCodegenExpression, TCodegenAggregate>> codegenAggregates;

        for (const auto& groupItem : groupClause->GroupItems) {
            codegenGroupExprs.push_back(Profile(groupItem, schema));
        }

        for (const auto& aggregateItem : groupClause->AggregateItems) {
            codegenAggregates.push_back(Profile(aggregateItem, schema));
        }

        codegenSource = MakeCodegenGroupOp(
            std::move(codegenGroupExprs),
            std::move(codegenAggregates),
            std::move(codegenSource));

        schema = groupClause->GetTableSchema();
    }

    if (auto projectClause = query->ProjectClause.Get()) {
        Fold(static_cast<int>(EFoldingObjectType::ProjectOp));

        std::vector<TCodegenExpression> codegenProjectExprs;

        for (const auto& item : projectClause->Projections) {
            codegenProjectExprs.push_back(Profile(item, schema));
        }

        codegenSource = MakeCodegenProjectOp(std::move(codegenProjectExprs), std::move(codegenSource));
        schema = query->ProjectClause->GetTableSchema();
    }

    return codegenSource;
}

TCodegenExpression TFoldingProfiler::Profile(const TConstExpressionPtr& expr, const TTableSchema& schema)
{
    Fold(static_cast<ui16>(expr->Type));
    if (auto literalExpr = expr->As<TLiteralExpression>()) {
        Fold(static_cast<int>(EFoldingObjectType::LiteralExpr));
        Fold(static_cast<ui16>(TValue(literalExpr->Value).Type));
        
        int index = Variables_
            ? Variables_->ConstantsRowBuilder.AddValue(TValue(literalExpr->Value))
            : -1;

        return MakeCodegenLiteralExpr(index, literalExpr->Type);
    } else if (auto referenceExpr = expr->As<TReferenceExpression>()) {
        Fold(static_cast<int>(EFoldingObjectType::ReferenceExpr));
        Fold(referenceExpr->ColumnName.c_str());
        Refer(referenceExpr);

        auto column = referenceExpr->ColumnName;
        return MakeCodegenReferenceExpr(
            schema.GetColumnIndexOrThrow(column),
            referenceExpr->Type,
            column.c_str());
    } else if (auto functionExpr = expr->As<TFunctionExpression>()) {
        Fold(static_cast<int>(EFoldingObjectType::FunctionExpr));
        Fold(functionExpr->FunctionName.c_str());

        std::vector<TCodegenExpression> codegenArgs;
        for (const auto& argument : functionExpr->Arguments) {
            codegenArgs.push_back(Profile(argument, schema));
        }

        return MakeCodegenFunctionExpr(
            functionExpr->FunctionName,
            std::move(codegenArgs),
            functionExpr->Type,
            "{" + functionExpr->GetName() + "}",
            FunctionRegistry_);
    } else if (auto unaryOp = expr->As<TUnaryOpExpression>()) {
        Fold(static_cast<int>(EFoldingObjectType::UnaryOpExpr));
        Fold(static_cast<int>(unaryOp->Opcode));

        return MakeCodegenUnaryOpExpr(
            unaryOp->Opcode,
            Profile(unaryOp->Operand, schema),
            unaryOp->Type,
            "{" + unaryOp->GetName() + "}");
    } else if (auto binaryOp = expr->As<TBinaryOpExpression>()) {
        Fold(static_cast<int>(EFoldingObjectType::BinaryOpExpr));
        Fold(static_cast<int>(binaryOp->Opcode));

        return MakeCodegenBinaryOpExpr(
            binaryOp->Opcode,
            Profile(binaryOp->Lhs, schema),
            Profile(binaryOp->Rhs, schema),
            binaryOp->Type,
            "{" + binaryOp->GetName() + "}");
    } else if (auto inOp = expr->As<TInOpExpression>()) {
        Fold(static_cast<int>(EFoldingObjectType::InOpExpr));

        std::vector<TCodegenExpression> codegenArgs;
        for (const auto& argument : inOp->Arguments) {
            codegenArgs.push_back(Profile(argument, schema));
        }

        int index = -1;
        if (Variables_) {
            index = Variables_->LiteralRows.size();
            Variables_->LiteralRows.push_back(inOp->Values);
        }

        return MakeCodegenInOpExpr(codegenArgs, index);
    }

    YUNREACHABLE();
}

void TFoldingProfiler::Profile(const TTableSchema& tableSchema, int keySize)
{
    Fold(static_cast<int>(EFoldingObjectType::TableSchema));
    for (int index = 0; index < tableSchema.Columns().size() && index < keySize; ++index) {
        const auto& column = tableSchema.Columns()[index];
        Fold(static_cast<ui16>(column.Type));
        Fold(column.Name.c_str());
        if (column.Expression) {
            Fold(column.Expression.Get().c_str());
        }
    }
}

TCodegenExpression TFoldingProfiler::Profile(const TNamedItem& namedExpression, const TTableSchema& schema)
{
    Fold(static_cast<int>(EFoldingObjectType::NamedExpression));
    Fold(namedExpression.Name.c_str());

    return Profile(namedExpression.Expression, schema);
}

std::pair<TCodegenExpression, TCodegenAggregate> TFoldingProfiler::Profile(
    const TAggregateItem& aggregateItem,
    const TTableSchema& schema)
{
    Fold(static_cast<int>(EFoldingObjectType::AggregateItem));
    Fold(static_cast<int>(aggregateItem.AggregateFunction));
    Fold(aggregateItem.Name.c_str());

    return std::make_pair(
        Profile(aggregateItem.Expression, schema),
        MakeCodegenAggregateFunction(
            aggregateItem.AggregateFunction,
            aggregateItem.Expression->Type,
            aggregateItem.Name.c_str()));
}

void TFoldingProfiler::Fold(int numeric)
{
    if (Id_) {
        Id_->AddInteger(numeric);
    }
}

void TFoldingProfiler::Fold(const char* str)
{
    if (Id_) {
        Id_->AddString(str);
    }
}

void TFoldingProfiler::Refer(const TReferenceExpression* referenceExpr)
{
    if (References_) {
        References_->insert(referenceExpr->ColumnName.c_str());
    }
}

////////////////////////////////////////////////////////////////////////////////

TCGQueryCallbackGenerator Profile(
    const TConstQueryPtr& query,
    llvm::FoldingSetNodeID* id,
    TCGVariables* variables,
    yhash_set<Stroka>* references,
    const TFunctionRegistry& functionRegistry)
{
    TFoldingProfiler profiler(functionRegistry);
    profiler.Set(id);
    profiler.Set(variables);
    profiler.Set(references);

    return [
            codegenSource = profiler.Profile(query)
        ] () {
            return CodegenEvaluate(std::move(codegenSource));
        };
}

TCGExpressionCallbackGenerator Profile(
    const TConstExpressionPtr& expr,
    const TTableSchema& schema,
    llvm::FoldingSetNodeID* id,
    TCGVariables* variables,
    yhash_set<Stroka>* references,
    const TFunctionRegistry& functionRegistry)
{
    TFoldingProfiler profiler(functionRegistry);
    profiler.Set(variables);
    profiler.Set(references);

    return [
            codegenExpr = profiler.Profile(expr, schema)
        ] () {
            return CodegenExpression(std::move(codegenExpr));
        };
}

void Profile(const TTableSchema& tableSchema, int keySize, llvm::FoldingSetNodeID* id, const TFunctionRegistry& functionRegistry)
{
    TFoldingProfiler profiler(functionRegistry);
    profiler.Set(id);

    profiler.Profile(tableSchema, keySize);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

