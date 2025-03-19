#include "lexer.h"

#include "regex.h"

#include <contrib/libs/re2/re2/re2.h>

#include <yql/essentials/sql/v1/reflect/sql_reflect.h>

#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/string/subst.h>

namespace NSQLTranslationV1 {

    using NSQLTranslation::TParsedToken;
    using NSQLTranslation::TParsedTokenList;

    class TRegexLexer: public NSQLTranslation::ILexer {
    public:
        TRegexLexer(bool ansi, NSQLReflect::TLexerGrammar grammar)
            : Grammar_(std::move(grammar))
        {
            for (auto& [token, regex] : MakeRegexByOtherNameMap(Grammar_, ansi)) {
                OtherRegexes_.emplace(std::move(token), std::move(regex));
            }
        }

        bool Tokenize(
            const TString& query,
            const TString& queryName,
            const TTokenCallback& onNextToken,
            NYql::TIssues& issues,
            size_t maxErrors) override {
            Y_UNUSED(queryName, issues);

            size_t errors = 0;
            for (size_t pos = 0; pos < query.size();) {
                TParsedToken matched = Match(TStringBuf(query, pos));

                if (matched.Name.empty() && maxErrors == errors) {
                    break;
                }

                if (matched.Name.empty()) {
                    pos += 1;
                    errors += 1;
                    continue;
                }

                pos += matched.Content.length();
                onNextToken(std::move(matched));
            }

            return errors == 0;
        }

    private:
        TParsedToken Match(const TStringBuf prefix) {
            TParsedTokenList matches;

            size_t keywordCount = MatchKeyword(prefix, matches);
            MatchPunctuation(prefix, matches);
            size_t otherCount = MatchRegex(prefix, matches);

            auto max = MaxElementBy(matches, [](const TParsedToken& m) {
                return m.Content.length();
            });

            if (max == std::end(matches)) {
                return {};
            }

            auto isMatched = [&](const TStringBuf name) {
                return std::end(matches) != FindIf(matches, [&](const auto& m) {
                           return m.Name == name;
                       });
            };

            Y_ENSURE(
                otherCount <= 1 ||
                (otherCount == 2 && isMatched("DIGITS") && isMatched("INTEGER_VALUE")));

            size_t conflicts = CountIf(matches, [&](const TParsedToken& m) {
                return m.Content.length() == max->Content.length();
            });
            conflicts -= 1;
            Y_ENSURE(
                conflicts == 0 ||
                (conflicts == 1 && keywordCount != 0 && isMatched("ID_PLAIN")) ||
                (conflicts == 1 && isMatched("DIGITS") && isMatched("INTEGER_VALUE")));

            Y_ENSURE(!max->Content.empty());
            return *max;
        }

        bool MatchKeyword(const TStringBuf prefix, TParsedTokenList& matches) {
            size_t count = 0;
            for (const auto& keyword : Grammar_.KeywordNames) {
                if (prefix.substr(0, keyword.length()) == keyword) {
                    matches.emplace_back(keyword, keyword);
                    count += 1;
                }
            }
            return count;
        }

        size_t MatchPunctuation(const TStringBuf prefix, TParsedTokenList& matches) {
            size_t count = 0;
            for (const auto& name : Grammar_.PunctuationNames) {
                const auto& content = Grammar_.BlockByName.at(name);
                if (prefix.substr(0, content.length()) == content) {
                    matches.emplace_back(name, content);
                    count += 1;
                }
            }
            return count;
        }

        size_t MatchRegex(const TStringBuf prefix, TParsedTokenList& matches) {
            size_t count = 0;
            for (const auto& [token, regex] : OtherRegexes_) {
                re2::StringPiece input(prefix.data(), prefix.size());
                if (RE2::Consume(&input, regex)) {
                    matches.emplace_back(token, TString(prefix.data(), input.data()));
                    count += 1;
                }
            }
            return count;
        }

        NSQLReflect::TLexerGrammar Grammar_;
        THashMap<TString, RE2> OtherRegexes_;
    };

    NSQLTranslation::ILexer::TPtr MakeRegexLexer(bool ansi) {
        return NSQLTranslation::ILexer::TPtr(
            new TRegexLexer(ansi, NSQLReflect::LoadLexerGrammar()));
    }

} // namespace NSQLTranslationV1
