#pragma once

#include <yql/essentials/ast/yql_ast.h>

#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NSQLComplete {

    bool CheckComplete(TStringBuf query, NYql::TAstNode& root, TString& error);

} // namespace NSQLComplete
