#include "lexer.h"

#include "regex.h"

#include <yql/essentials/sql/v1/reflect/sql_reflect.h>

#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/string/subst.h>

#include <regex>

namespace NSQLTranslationV1 {

    using NSQLTranslation::TParsedToken;
    using NSQLTranslation::TParsedTokenList;

    class TRegexLexer: public NSQLTranslation::ILexer {
    public:
        TRegexLexer(bool ansi, NSQLReflect::TLexerGrammar meta)
            : Meta_(std::move(meta))
        {
            for (auto& [token, regex] : GetRegexByComplexTokenMap(Meta_, ansi)) {
                Regexes_.emplace(std::move(token), std::string(regex));
            }
        }

        bool Tokenize(
            const TString& query,
            const TString& queryName,
            const TTokenCallback& onNextToken,
            NYql::TIssues& issues,
            size_t maxErrors) override {
            Y_UNUSED(queryName, issues, maxErrors);

            for (size_t pos = 0; pos < query.size();) {
                TParsedToken matched = Match(query, pos);
                pos += matched.Content.length();
                onNextToken(std::move(matched));
            }

            return true;
        }

    private:
        TParsedToken Match(const TString& query, size_t pos) {
            TParsedTokenList matches;
            MatchKeyword(query, pos, matches);
            MatchPunctuation(query, pos, matches);
            MatchRegex(query, pos, matches);

            auto it = MaxElementBy(matches, [](const TParsedToken& matched) {
                return matched.Content.length();
            });
            Y_ENSURE(it != std::end(matches));
            Y_ENSURE(!it->Content.empty());

            return *it;
        }

        void MatchKeyword(const TString& query, size_t pos, TParsedTokenList& matches) {
            for (const auto& keyword : Meta_.Keywords) {
                if (query.substr(pos, keyword.length()) == keyword) {
                    if (pos + keyword.length() >= query.length() ||
                        !std::isalnum(query[pos + keyword.length()])) {
                        matches.emplace_back(keyword, keyword);
                    }
                }
            }
        }

        void MatchPunctuation(const TString& query, size_t pos, TParsedTokenList& matches) {
            for (const auto& name : Meta_.Punctuation) {
                const auto& content = Meta_.ContentByName.at(name);
                if (query.substr(pos, content.length()) == content) {
                    matches.emplace_back(name, content);
                }
            }
        }

        void MatchRegex(const TString& query, size_t pos, TParsedTokenList& matches) {
            for (const auto& [token, regex] : Regexes_) {
                std::smatch match;
                std::string substring = query.substr(pos);
                if (std::regex_search(substring, match, regex, std::regex_constants::match_continuous)) {
                    matches.emplace_back(token, match.str(0));
                }
            }
        }

        NSQLReflect::TLexerGrammar Meta_;
        THashMap<TString, std::regex> Regexes_;
    };

    NSQLTranslation::ILexer::TPtr MakeRegexLexer(bool ansi) {
        return NSQLTranslation::ILexer::TPtr(
            new TRegexLexer(ansi, NSQLReflect::GetLexerGrammar()));
    }

} // namespace NSQLTranslationV1
