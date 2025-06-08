#pragma once

#include <yql/essentials/sql/v1/complete/name/object/schema.h>

#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>

namespace NSQLComplete {

    THashMap<TString, THashMap<TString, TVector<TFolderEntry>>>
    CollectTables(const NYql::TExprNode& expr);

} // namespace NSQLComplete
