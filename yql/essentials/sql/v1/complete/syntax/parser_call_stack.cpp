#include "parser_call_stack.h"

#include "sql_antlr4.h"

#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>

namespace NSQLComplete {

    const TVector<TRuleId> KeywordRules = {
        RULE(Keyword),
        RULE(Keyword_expr_uncompat),
        RULE(Keyword_table_uncompat),
        RULE(Keyword_select_uncompat),
        RULE(Keyword_alter_uncompat),
        RULE(Keyword_in_uncompat),
        RULE(Keyword_window_uncompat),
        RULE(Keyword_hint_uncompat),
        RULE(Keyword_as_compat),
        RULE(Keyword_compat),
    };

    const TVector<TRuleId> TypeNameRules = {
        RULE(Type_name_simple),
    };

    bool ContainsRule(TRuleId rule, const TParserCallStack& stack) {
        return Find(stack, rule) != std::end(stack);
    }

    bool IsLikelyTypeStack(const TParserCallStack& stack) {
        return ContainsRule(RULE(Type_name_simple), stack);
    }

    std::unordered_set<TRuleId> GetC3PreferredRules() {
        std::unordered_set<TRuleId> preferredRules;
        preferredRules.insert(std::begin(KeywordRules), std::end(KeywordRules));
        preferredRules.insert(std::begin(TypeNameRules), std::end(TypeNameRules));
        return preferredRules;
    }

}
