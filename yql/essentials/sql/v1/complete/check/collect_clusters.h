#pragma once

#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>

namespace NSQLComplete {

    THashSet<TString> CollectClusters(const NYql::TExprNode& expr);

} // namespace NSQLComplete
