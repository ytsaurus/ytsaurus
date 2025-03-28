#pragma once

#include <yql/essentials/sql/v1/complete/antlr4/defs.h>

#include <contrib/libs/antlr4_cpp_runtime/src/Token.h>
#include <contrib/libs/antlr4_cpp_runtime/src/Vocabulary.h>

#include <util/generic/vector.h>

#include <unordered_set>

namespace NSQLComplete {    

    class ISqlGrammar {
    public:
        virtual const antlr4::dfa::Vocabulary& GetVocabulary() const = 0;
        virtual const std::unordered_set<TTokenId>& GetAllTokens() const = 0;
        virtual const std::unordered_set<TTokenId>& GetKeywordTokens() const = 0;
        virtual const TVector<TRuleId>& GetKeywordRules() const = 0;
        virtual const TVector<TRuleId>& GetTypeNameRules() const = 0;
        virtual ~ISqlGrammar() = default;
    };

    const ISqlGrammar& GetSqlGrammar(bool isAnsiLexer);

} // namespace NSQLComplete
