#include "stdafx.h"
#include "folding_profiler.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TFoldingProfiler::TFoldingProfiler()
{ }

TFoldingProfiler& TFoldingProfiler::Set(llvm::FoldingSetNodeID& id)
{
    Id_ = &id;
    return *this;
}

TFoldingProfiler& TFoldingProfiler::Set(TCGBinding& binding)
{
    Binding_ = &binding;
    return *this;
}

TFoldingProfiler& TFoldingProfiler::Set(TCGVariables& variables)
{
    Variables_ = &variables;
    return *this;
}

TFoldingProfiler& TFoldingProfiler::Set(yhash_set<Stroka>& references)
{
    References_ = &references;
    return *this;
}

void TFoldingProfiler::Profile(const TConstQueryPtr& query)
{
    Fold(static_cast<int>(EFoldingObjectType::ScanOp));
    Profile(query->TableSchema);

    if (query->Predicate) {
        Fold(static_cast<int>(EFoldingObjectType::FilterOp));
        Profile(query->Predicate);
    }

    if (query->GroupClause) {
        Fold(static_cast<int>(EFoldingObjectType::GroupOp));

        for (const auto& groupItem : query->GroupClause->GroupItems) {
            Profile(groupItem);
        }

        for (const auto& aggregateItem : query->GroupClause->AggregateItems) {
            Profile(aggregateItem);
        }
    }

    if (query->ProjectClause) {
        Fold(static_cast<int>(EFoldingObjectType::ProjectOp));

        for (const auto& projection : query->ProjectClause->Projections) {
            Profile(projection);
        }
    }
}

void TFoldingProfiler::Profile(const TConstExpressionPtr& expr)
{
    Fold(static_cast<ui16>(expr->Type));
    if (auto literalExpr = expr->As<TLiteralExpression>()) {
        Fold(static_cast<int>(EFoldingObjectType::LiteralExpr));
        Fold(static_cast<ui16>(TValue(literalExpr->Value).Type));

        Bind(literalExpr);
    } else if (auto referenceExpr = expr->As<TReferenceExpression>()) {
        Fold(static_cast<int>(EFoldingObjectType::ReferenceExpr));
        Fold(referenceExpr->ColumnName.c_str());

        Refer(referenceExpr);
    } else if (auto functionExpr = expr->As<TFunctionExpression>()) {
        Fold(static_cast<int>(EFoldingObjectType::FunctionExpr));
        Fold(functionExpr->FunctionName.c_str());

        for (const auto& argument : functionExpr->Arguments) {
            Profile(argument);
        }
    } else if (auto binaryOp = expr->As<TBinaryOpExpression>()) {
        Fold(static_cast<int>(EFoldingObjectType::BinaryOpExpr));
        Fold(static_cast<int>(binaryOp->Opcode));

        Profile(binaryOp->Lhs);
        Profile(binaryOp->Rhs);
    } else if (auto inOp = expr->As<TInOpExpression>()) {
        Fold(static_cast<int>(EFoldingObjectType::InOpExpr));

        for (const auto& argument : inOp->Arguments) {
            Profile(argument);
        }

        Bind(inOp);
    } else {
        YUNREACHABLE();
    }
}

void TFoldingProfiler::Profile(const TTableSchema& tableSchema)
{
    Fold(static_cast<int>(EFoldingObjectType::TableSchema));
}

void TFoldingProfiler::Profile(const TNamedItem& namedExpression)
{
    Fold(static_cast<int>(EFoldingObjectType::NamedExpression));
    Fold(namedExpression.Name.c_str());

    Profile(namedExpression.Expression);
}

void TFoldingProfiler::Profile(const TAggregateItem& aggregateItem)
{
    Fold(static_cast<int>(EFoldingObjectType::AggregateItem));
    Fold(static_cast<int>(aggregateItem.AggregateFunction));
    Fold(aggregateItem.Name.c_str());

    Profile(aggregateItem.Expression);
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

void TFoldingProfiler::Bind(const TLiteralExpression* literalExpr)
{
    YCHECK(!Variables_ == !Binding_);

    if (Variables_) {
        int index = Variables_->ConstantsRowBuilder.AddValue(TValue(literalExpr->Value));
        Binding_->NodeToConstantIndex[literalExpr] = index;
    }
}

void TFoldingProfiler::Bind(const TInOpExpression* inOp)
{
    YCHECK(!Variables_ == !Binding_);

    if (Variables_) {
        int index = Variables_->LiteralRows.size();
        Variables_->LiteralRows.push_back(inOp->Values);
        Binding_->NodeToRows[inOp] = index;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

