#pragma once

#include "cg_types.h"
#include "plan_fragment.h"

#include <util/generic/hash_set.h>

#include <llvm/ADT/FoldingSet.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

// Folding profiler computes a strong structural hash used to cache query fragments.

DEFINE_ENUM(EFoldingObjectType,
    (ScanOp)
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
{
public:
    explicit TFoldingProfiler(
        llvm::FoldingSetNodeID& id,
        TCGBinding& binding,
        TCGVariables& variables,
        yhash_set<Stroka>* references = nullptr);
    void Profile(const TConstQueryPtr& query);
    void Profile(const TConstExpressionPtr& expr);
    void Profile(const TNamedItem& namedExpression);
    void Profile(const TAggregateItem& aggregateItem);
    void Profile(const TTableSchema& tableSchema);

//FIXME .Set(binding).Set(references).Profile.

private:
    llvm::FoldingSetNodeID& Id_;
    TCGBinding& Binding_;
    TCGVariables& Variables_;
    yhash_set<Stroka>* References_;
};

struct TFoldingHasher
{
    size_t operator ()(const llvm::FoldingSetNodeID& id) const;
};


////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

