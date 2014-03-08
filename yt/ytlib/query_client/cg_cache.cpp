#include "cg_cache.h"
#include "cg_fragment.h"

#include "plan_node.h"
#include "plan_fragment.h"

#include <ytlib/new_table_client/unversioned_row.h>

#include <core/misc/cache.h>

#include <llvm/ADT/FoldingSet.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EFoldingObjectType,
    (ScanOp)
    (FilterOp)
    (GroupOp)
    (ProjectOp)

    (IntegerLiteralExpr)
    (DoubleLiteralExpr)
    (ReferenceExpr)
    (FunctionExpr)
    (BinaryOpExpr)

    (NamedExpression)
    (AggregateItem)

    (TableSchema)
);


class TFoldingProfiler
{
public:
    explicit TFoldingProfiler(
        llvm::FoldingSetNodeID& id,
        TFragmentParams& params)
        : Id_(id)
        , Params_(params)
    { }

    void Profile(const TOperator* op);
    void Profile(const TExpression* expr);
    void Profile(const TNamedExpression& namedExpression);
    void Profile(const TAggregateItem& aggregateItem);
    void Profile(const TTableSchema& tableSchema);

private:
    llvm::FoldingSetNodeID& Id_;
    TFragmentParams& Params_;

};

void TFoldingProfiler::Profile(const TOperator* op)
{
    switch (op->GetKind()) {

        case EOperatorKind::Scan: {
            const auto* scanOp = op->As<TScanOperator>();
            Id_.AddInteger(EFoldingObjectType::ScanOp);

            Profile(scanOp->GetTableSchema());

            int index = Params_.DataSplitsArray.size();
            Params_.DataSplitsArray.push_back(scanOp->DataSplits());
            Params_.ScanOpToDataSplits[scanOp] = index;

            break;
        }

        case EOperatorKind::Filter: {
            const auto* filterOp = op->As<TFilterOperator>();
            Id_.AddInteger(EFoldingObjectType::FilterOp);

            Profile(filterOp->GetPredicate());
            Profile(filterOp->GetSource());

            break;
        }

        case EOperatorKind::Project: {
            const auto* projectOp = op->As<TProjectOperator>();
            Id_.AddInteger(EFoldingObjectType::ProjectOp);

            for (const auto& projection : projectOp->Projections()) {
                Profile(projection);
            }

            Profile(projectOp->GetSource());

            break;
        }

        case EOperatorKind::Group: {
            const auto* groupOp = op->As<TGroupOperator>();
            Id_.AddInteger(EFoldingObjectType::GroupOp);

            for (const auto& groupItem : groupOp->GroupItems()) {
                Profile(groupItem);
            }

            for (const auto& aggregateItem : groupOp->AggregateItems()) {
                Profile(aggregateItem);
            }

            Profile(groupOp->GetSource());

            break;
        }

        default:
            YUNREACHABLE();
    }
}

void TFoldingProfiler::Profile(const TExpression* expr)
{
    using NVersionedTableClient::MakeIntegerValue;
    using NVersionedTableClient::MakeDoubleValue;

    switch (expr->GetKind()) {

        case EExpressionKind::IntegerLiteral: {
            const auto* integerLiteralExpr = expr->As<TIntegerLiteralExpression>();
            Id_.AddInteger(EFoldingObjectType::IntegerLiteralExpr);

            int index = Params_.ConstantArray.size();
            Params_.ConstantArray.push_back(MakeIntegerValue<TValue>(integerLiteralExpr->GetValue()));
            Params_.NodeToConstantIndex[expr] = index;

            break;
        }

        case EExpressionKind::DoubleLiteral: {
            const auto* doubleLiteralExpr = expr->As<TDoubleLiteralExpression>();
            Id_.AddInteger(EFoldingObjectType::DoubleLiteralExpr);

            int index = Params_.ConstantArray.size();
            Params_.ConstantArray.push_back(MakeIntegerValue<TValue>(doubleLiteralExpr->GetValue()));
            Params_.NodeToConstantIndex[expr] = index;

            break;
        }

        case EExpressionKind::Reference: {
            const auto* referenceExpr = expr->As<TReferenceExpression>();
            Id_.AddInteger(EFoldingObjectType::ReferenceExpr);
            Id_.AddString(referenceExpr->GetColumnName().c_str());

            break;
        }

        case EExpressionKind::Function: {
            const auto* functionExpr = expr->As<TFunctionExpression>();
            Id_.AddInteger(EFoldingObjectType::FunctionExpr);
            Id_.AddString(functionExpr->GetFunctionName().c_str());

            for (const auto& argument : functionExpr->Arguments()) {
                Profile(argument);
            }

            break;
        }

        case EExpressionKind::BinaryOp: {
            const auto* binaryOp = expr->As<TBinaryOpExpression>();
            Id_.AddInteger(EFoldingObjectType::BinaryOpExpr);
            Id_.AddInteger(binaryOp->GetOpcode());

            Profile(binaryOp->GetLhs());
            Profile(binaryOp->GetRhs());

            break;
        }

        default:
            YUNREACHABLE();
    }
}

void TFoldingProfiler::Profile(const TTableSchema& tableSchema)
{
    Id_.AddInteger(EFoldingObjectType::TableSchema);
}

void TFoldingProfiler::Profile(const TNamedExpression& namedExpression)
{
    Id_.AddInteger(EFoldingObjectType::NamedExpression);
    Id_.AddString(namedExpression.Name.c_str());

    Profile(namedExpression.Expression);
}

void TFoldingProfiler::Profile(const TAggregateItem& aggregateItem)
{
    Id_.AddInteger(EFoldingObjectType::AggregateItem);
    Id_.AddInteger(aggregateItem.AggregateFunction);
    Id_.AddString(aggregateItem.Name.c_str());

    Profile(aggregateItem.Expression);
}

struct TFoldingHasher
{
    size_t operator ()(const llvm::FoldingSetNodeID& id) const
    {
        return id.ComputeHash();
    }
};

class TCGCache::TCachedCGFragment
    : public TCacheValueBase<
        llvm::FoldingSetNodeID,
        TCachedCGFragment,
        TFoldingHasher>
    , public TCGFragment
{
public:
    TCachedCGFragment(const llvm::FoldingSetNodeID& id)
        : TCacheValueBase(id)
        , TCGFragment()
    { }

};

class TCGCache::TImpl
    : public TSizeLimitedCache<
        llvm::FoldingSetNodeID,
        TCachedCGFragment,
        TFoldingHasher>
{
public:
    TImpl(const int maxCacheSize)
        : TSizeLimitedCache(maxCacheSize)
    { }

    std::pair<TCodegenedFunction, TFragmentParams> Codegen(
        const TPlanFragment& fragment,
        std::function<void(const TPlanFragment&, TCGFragment&, const TFragmentParams&)> compiler)
    {
        llvm::FoldingSetNodeID id;
        TFragmentParams params;

        TFoldingProfiler(id, params).Profile(fragment.GetHead());

        TInsertCookie cookie(id);
        if (BeginInsert(&cookie)) {
            try {
                auto newCGFragment = New<TCachedCGFragment>(id);
                compiler(fragment, *newCGFragment, params);
                newCGFragment->GetCompiledMainFunction();
                cookie.EndInsert(std::move(newCGFragment));
            } catch (const std::exception& ex) {
                cookie.Cancel(ex);
            }
        }

        auto cgFragment = cookie.GetValue().Get().ValueOrThrow();
        YCHECK(cgFragment->IsCompiled());

        return std::make_pair(
            cgFragment->GetCompiledMainFunction(),
            std::move(params));
    }

};

// TODO(sandello): Make configurable.
TCGCache::TCGCache()
    : Impl_(New<TImpl>(100))
{ }

TCGCache::~TCGCache()
{ }

std::pair<TCodegenedFunction, TFragmentParams> TCGCache::Codegen(
    const TPlanFragment& fragment,
    std::function<void(const TPlanFragment&, TCGFragment&, const TFragmentParams&)> compiler)
{
    return Impl_->Codegen(fragment, std::move(compiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

