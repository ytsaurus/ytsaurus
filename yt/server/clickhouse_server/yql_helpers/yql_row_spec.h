#pragma once

#include <yql/ast/yql_expr.h>

#include <mapreduce/yt/node/node.h>

#include <util/generic/map.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYql {

struct TYqlRowSpecInfo {
    bool Parse(const TString& yson, TExprContext& ctx, const TPosition& pos = {});
    bool Parse(const NYT::TNode& attrs, TExprContext& ctx, const TPosition& pos = {});

    TString ToYsonString() const;
    void FillNode(NYT::TNode& attrs) const;

    bool IsSorted() const {
        return !SortedBy.empty();
    }

    bool HasAuxColumns() const;
    void CopySortness(const TYqlRowSpecInfo& from);

    bool StrictSchema = true;
    bool UniqueKeys = false;

    const TStructExprType* Type = nullptr;

    TVector<bool> SortDirections;
    TVector<TString> SortedBy;
    TVector<TString> SortMembers;
    TTypeAnnotationNode::TListType SortedByTypes;
    TMap<TString, TString> DefaultValues;
};

}
