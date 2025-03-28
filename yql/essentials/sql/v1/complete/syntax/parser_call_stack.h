#pragma once

#include <yql/essentials/sql/v1/complete/antlr4/defs.h>

namespace NSQLComplete {

    bool IsLikelyTypeStack(const TParserCallStack& stack);

    std::unordered_set<TRuleId> GetC3PreferredRules();

}
