#pragma once

#include "cg_types.h"
#include "plan_fragment.h"

#include <util/generic/hash_set.h>
#include <util/generic/noncopyable.h>

#include <llvm/ADT/FoldingSet.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

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
    TFoldingProfiler();

    void Profile(const TConstQueryPtr& query);
    void Profile(const TConstExpressionPtr& expr);

    TFoldingProfiler& Set(llvm::FoldingSetNodeID& id);
    TFoldingProfiler& Set(TCGBinding& binding);
    TFoldingProfiler& Set(TCGVariables& variables);
    TFoldingProfiler& Set(yhash_set<Stroka>& references);

private:
    void Profile(const TNamedItem& namedExpression);
    void Profile(const TAggregateItem& aggregateItem);
    void Profile(const TTableSchema& tableSchema);

    void Fold(int numeric);
    void Fold(const char* str);
    void Refer(const TReferenceExpression* referenceExpr);
    void Bind(const TLiteralExpression* literalExpr);
    void Bind(const TInOpExpression* inOp);

    llvm::FoldingSetNodeID* Id_ = nullptr;
    TCGBinding* Binding_ = nullptr;
    TCGVariables* Variables_ = nullptr;
    yhash_set<Stroka>* References_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

// A hasher for llvm::FoldingSetNodeID
template<>
struct hash<llvm::FoldingSetNodeID>
{
    inline size_t operator()(const llvm::FoldingSetNodeID& id) const
    {
        return id.ComputeHash();
    }
};

////////////////////////////////////////////////////////////////////////////////
