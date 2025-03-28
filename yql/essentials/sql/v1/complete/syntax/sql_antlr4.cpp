#include "sql_antlr4.h"

#include <yql/essentials/sql/v1/format/sql_format.h>

namespace NSQLComplete {

    class TSqlGrammar: public ISqlGrammar {
    public:
        TSqlGrammar()
            : Vocabulary(GetVocabularyP())
            , AllTokens(ComputeAllTokens())
            , KeywordTokens(ComputeKeywordTokens())
        {
        }

        const antlr4::dfa::Vocabulary& GetVocabulary() const override {
            return *Vocabulary;
        }

        const std::unordered_set<TTokenId>& GetAllTokens() const override {
            return AllTokens;
        }

        const std::unordered_set<TTokenId>& GetKeywordTokens() const override {
            return KeywordTokens;
        }

        const TVector<TRuleId>& GetKeywordRules() const override {
            static const TVector<TRuleId> KeywordRules = {
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

            return KeywordRules;
        }

        const TVector<TRuleId>& GetTypeNameRules() const override {
            static const TVector<TRuleId> TypeNameRules = {
                RULE(Type_name_simple),
            };

            return TypeNameRules;
        }

    private:
        static const antlr4::dfa::Vocabulary* GetVocabularyP() {
            return &NALADefaultAntlr4::SQLv1Antlr4Parser(nullptr).getVocabulary();
        }

        std::unordered_set<TTokenId> ComputeAllTokens() {
            const auto& vocabulary = GetVocabulary();

            std::unordered_set<TTokenId> allTokens;

            for (size_t type = 1; type <= vocabulary.getMaxTokenType(); ++type) {
                allTokens.emplace(type);
            }

            return allTokens;
        }

        std::unordered_set<TTokenId> ComputeKeywordTokens() {
            const auto& vocabulary = GetVocabulary();
            const auto keywords = NSQLFormat::GetKeywords();

            auto keywordTokens = GetAllTokens();
            std::erase_if(keywordTokens, [&](TTokenId token) {
                return !keywords.contains(vocabulary.getSymbolicName(token));
            });
            keywordTokens.erase(TOKEN_EOF);

            return keywordTokens;
        }

        const antlr4::dfa::Vocabulary* Vocabulary;
        const std::unordered_set<TTokenId> AllTokens;
        const std::unordered_set<TTokenId> KeywordTokens;
    };

    const ISqlGrammar& GetSqlGrammar() {
        const static TSqlGrammar DefaultSqlGrammar{};
        return DefaultSqlGrammar;
    }

} // namespace NSQLComplete
