#include "lexer.h"

#include "regex.h"

#include <yql/essentials/sql/v1/reflect/sql_reflect.h>

#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/string/subst.h>

#include <regex>

namespace NSQLTranslationV1 {

    class TRegexLexer: public NSQLTranslation::ILexer {
    private:
        struct TMatchedToken {
            TString Name;
            TString Content;
        };

    public:
        TRegexLexer(bool ansi, NSQLReflect::TGrammarMeta meta)
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
            Y_UNUSED(queryName, issues, maxErrors, onNextToken);
            for (size_t pos = 0; pos < query.size();) {
                TMatchedToken matched = Match(query, pos);
                pos += matched.Content.length();
                onNextToken({
                    .Name = std::move(matched.Name),
                    .Content = std::move(matched.Content),
                });
            }
            return true;
        }

    private:
        TMatchedToken Match(const TString& query, size_t pos) {
            TVector<TMatchedToken> matches;
            MatchKeyword(query, pos, matches);
            MatchPunctuation(query, pos, matches);
            MatchRegex(query, pos, matches);

            auto it = MaxElementBy(matches, [](const TMatchedToken& matched) {
                return matched.Content.length();
            });
            Y_ENSURE(it != std::end(matches));

            return *it;
        }

        void MatchKeyword(const TString& query, size_t pos, TVector<TMatchedToken>& matches) {
            for (const auto& keyword : Meta_.Keywords) {
                if (query.substr(pos, keyword.length()) == keyword) {
                    if (pos + keyword.length() >= query.length() ||
                        !std::isalnum(query[pos + keyword.length()])) {
                        matches.emplace_back(keyword, keyword);
                    }
                }
            }
        }

        void MatchPunctuation(const TString& query, size_t pos, TVector<TMatchedToken>& matches) {
            for (const auto& name : Meta_.Punctuation) {
                const auto& content = Meta_.ContentByName.at(name);
                if (query.substr(pos, content.length()) == content) {
                    matches.emplace_back(name, content);
                }
            }
        }

        void MatchRegex(const TString& query, size_t pos, TVector<TMatchedToken>& matches) {
            for (const auto& [token, regex] : Regexes_) {
                std::smatch match;
                std::string substring = query.substr(pos);
                if (std::regex_search(substring, match, regex, std::regex_constants::match_continuous)) {
                    matches.emplace_back(token, match.str(0));
                }
            }
        }

        NSQLReflect::TGrammarMeta Meta_;
        THashMap<TString, std::regex> Regexes_;
    };

    NSQLTranslation::ILexer::TPtr MakeRegexLexer(bool ansi) {
        return NSQLTranslation::ILexer::TPtr(
            new TRegexLexer(ansi, NSQLReflect::GetGrammarMeta()));
    }

} // namespace NSQLTranslationV1
