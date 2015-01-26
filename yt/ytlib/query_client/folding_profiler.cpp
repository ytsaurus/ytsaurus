#include "stdafx.h"
#include "folding_profiler.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TFoldingProfiler::TFoldingProfiler(
    llvm::FoldingSetNodeID& id,
    TCGBinding& binding,
    TCGVariables& variables,
    yhash_set<Stroka>* references)
    : Id_(id)
    , Binding_(binding)
    , Variables_(variables)
    , References_(references)
{ }

void TFoldingProfiler::Profile(const TConstQueryPtr& query)
{
    Id_.AddInteger(static_cast<int>(EFoldingObjectType::ScanOp));
    Profile(query->TableSchema);

    if (query->Predicate) {
        Id_.AddInteger(static_cast<int>(EFoldingObjectType::FilterOp));
        Profile(query->Predicate);
    }

    if (query->GroupClause) {
        Id_.AddInteger(static_cast<int>(EFoldingObjectType::GroupOp));

        for (const auto& groupItem : query->GroupClause->GroupItems) {
            Profile(groupItem);
        }

        for (const auto& aggregateItem : query->GroupClause->AggregateItems) {
            Profile(aggregateItem);
        }
    }

    if (query->ProjectClause) {
        Id_.AddInteger(static_cast<int>(EFoldingObjectType::ProjectOp));

        for (const auto& projection : query->ProjectClause->Projections) {
            Profile(projection);
        }
    }
}

void TFoldingProfiler::Profile(const TConstExpressionPtr& expr)
{
    Id_.AddInteger(static_cast<ui16>(expr->Type));
    if (auto literalExpr = expr->As<TLiteralExpression>()) {
        Id_.AddInteger(static_cast<int>(EFoldingObjectType::LiteralExpr));
        Id_.AddInteger(static_cast<ui16>(TValue(literalExpr->Value).Type));

        int index = Variables_.ConstantsRowBuilder.AddValue(TValue(literalExpr->Value));
        Binding_.NodeToConstantIndex[literalExpr] = index;
    } else if (auto referenceExpr = expr->As<TReferenceExpression>()) {
        Id_.AddInteger(static_cast<int>(EFoldingObjectType::ReferenceExpr));
        Id_.AddString(referenceExpr->ColumnName.c_str());
        if (References_) {
            References_->insert(referenceExpr->ColumnName);
        }
    } else if (auto functionExpr = expr->As<TFunctionExpression>()) {
        Id_.AddInteger(static_cast<int>(EFoldingObjectType::FunctionExpr));
        Id_.AddString(functionExpr->FunctionName.c_str());

        for (const auto& argument : functionExpr->Arguments) {
            Profile(argument);
        }
    } else if (auto binaryOp = expr->As<TBinaryOpExpression>()) {
        Id_.AddInteger(static_cast<int>(EFoldingObjectType::BinaryOpExpr));
        Id_.AddInteger(static_cast<int>(binaryOp->Opcode));

        Profile(binaryOp->Lhs);
        Profile(binaryOp->Rhs);
    } else if (auto inOp = expr->As<TInOpExpression>()) {
        Id_.AddInteger(static_cast<int>(EFoldingObjectType::InOpExpr));

        for (const auto& argument : inOp->Arguments) {
            Profile(argument);
        }

        int index = Variables_.LiteralRows.size();
        Variables_.LiteralRows.push_back(inOp->Values);
        Binding_.NodeToRows[expr.Get()] = index;

    } else {
        YUNREACHABLE();
    }
}

void TFoldingProfiler::Profile(const TTableSchema& tableSchema)
{
    Id_.AddInteger(static_cast<int>(EFoldingObjectType::TableSchema));
}

void TFoldingProfiler::Profile(const TNamedItem& namedExpression)
{
    Id_.AddInteger(static_cast<int>(EFoldingObjectType::NamedExpression));
    Id_.AddString(namedExpression.Name.c_str());

    Profile(namedExpression.Expression);
}

void TFoldingProfiler::Profile(const TAggregateItem& aggregateItem)
{
    Id_.AddInteger(static_cast<int>(EFoldingObjectType::AggregateItem));
    Id_.AddInteger(static_cast<int>(aggregateItem.AggregateFunction));
    Id_.AddString(aggregateItem.Name.c_str());

    Profile(aggregateItem.Expression);
}

size_t TFoldingHasher::operator ()(const llvm::FoldingSetNodeID& id) const
{
    return id.ComputeHash();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

