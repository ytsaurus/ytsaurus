#pragma once

#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/string.h>
#include <util/generic/maybe.h>

namespace NSQLComplete {

    TMaybe<TString> ToCluster(const NYql::TExprNode& node);

    TMaybe<TString> ToTablePath(const NYql::TExprNode& node);

} // namespace NSQLComplete
