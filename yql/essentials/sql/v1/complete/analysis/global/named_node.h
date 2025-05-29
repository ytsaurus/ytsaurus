#pragma once

#include "global.h"
#include "parse_tree.h"

namespace NSQLComplete {

    TNamedExpressions CollectNamedNodes(
        SQLv1::Sql_queryContext* ctx,
        antlr4::TokenStream* tokens,
        size_t cursorPosition);

} // namespace NSQLComplete
