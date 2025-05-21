#pragma once

#include "parse_tree.h"

#include <yql/essentials/sql/v1/complete/core/environment.h>

namespace NSQLComplete {

    TMaybe<TValue> Evaluate(antlr4::ParserRuleContext* ctx, const TEnvironment& env);

} // namespace NSQLComplete
