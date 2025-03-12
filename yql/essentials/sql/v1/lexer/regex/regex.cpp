#include "regex.h"

#include <util/generic/vector.h>

#include <regex>

#define SUBSTITUTION(name, mode) \
    {#name, name##_##mode}

#define SUBSTITUTIONS(mode)                                         \
    {                                                               \
        #mode, {                                                    \
            SUBSTITUTION(GRAMMAR_STRING_CORE_SINGLE, mode),         \
                SUBSTITUTION(GRAMMAR_STRING_CORE_DOUBLE, mode),     \
                SUBSTITUTION(GRAMMAR_MULTILINE_COMMENT_CORE, mode), \
        }                                                           \
    }

namespace NSQLTranslationV1 {

    struct TRewriteRule {
        std::regex Regex;
        std::string Format;
    };

    TString ReEscaped(TString text) {
        if (text == "\\") {
            return "\\\\";
        }
        if (text == ".") {
            return "\\.";
        }
        if (text == "+") {
            return "\\+";
        }

        SubstGlobal(text, "'\\'", "\\\\");
        SubstGlobal(text, "'``'", "``");
        SubstGlobal(text, "'`'", "`");
        SubstGlobal(text, "'_'", "_");
        SubstGlobal(text, "'/*'", "\\/\\*");
        SubstGlobal(text, "'*/'", "\\*\\/");
        SubstGlobal(text, "'\\n'", "\\n");
        SubstGlobal(text, "'\\r'", "\\r");
        SubstGlobal(text, "'\\t'", "\\t");
        SubstGlobal(text, "'\\u000C' |", "");
        SubstGlobal(text, "' '", "$$$");
        SubstGlobal(text, "'--'", "--");
        return text;
    }

    TVector<TRewriteRule> GetRewriteRules(const TStringBuf mode, const NSQLReflect::TLexerGrammar& grammar) {
        THashMap<TString, THashMap<TString, TString>> Substitutions = {
            SUBSTITUTIONS(DEFAULT),
            SUBSTITUTIONS(ANSI),
        };
        Substitutions["ANSI"]["GRAMMAR_MULTILINE_COMMENT_CORE"] =
            Substitutions["DEFAULT"]["GRAMMAR_MULTILINE_COMMENT_CORE"];

        THashMap<std::string, std::string> rules;

        for (auto& [k, v] : grammar.BlockByName) {
            rules["(\\b" + k + "\\b)"] = "(" + ReEscaped(v) + ")";
        }

        rules["(\\bEOF\\b)"] = "$";

        for (char letter = 'A'; letter <= 'Z'; ++letter) {
            TString lower(char(ToLower(letter)));
            TString upper(char(ToUpper(letter)));
            rules["(\\b" + TString(letter) + "\\b(?![-'\\]]))"] = "[" + lower + upper + "]";
        }

        for (const auto& [k, v] : Substitutions.at(mode)) {
            rules["@" + k + "@"] = ReEscaped(v);
        }

        rules[R"(\((\\\\|.)\))"] = "$1";
        rules[R"(~\((\\\\|\\\w|.) \| (\\\\|\\\w|.)\))"] = "[^$1$2]";
        rules[R"(~(.) )"] = "[^$1]";
        rules[R"('(.)'\.\.'(.)')"] = "[$1-$2]";
        rules[R"(\(\[([^\]\[\(\)]+)\]\))"] = "[$1]";
        rules[R"('(\d)')"] = "$1";
        rules[R"(\\\\ \.)"] = "\\\\ (.|\\n)";
        rules[R"(\(\.\))"] = "(.|\\n)";
        rules[R"(([^\\\(\.])\.([^\.]))"] = "$1(.|\\n)$2";

        TVector<TRewriteRule> result;
        for (auto& [regex, fmt] : rules) {
            result.push_back({
                .Regex = std::regex(regex),
                .Format = fmt,
            });
        }
        return result;
    }

    TString ToRegex(const TString& name, const TStringBuf mode, const NSQLReflect::TLexerGrammar& grammar) {
        TVector<TRewriteRule> rules = GetRewriteRules(mode, grammar);

        TString next = grammar.BlockByName.at(name);

        next = ReEscaped(std::move(next));

        TString prev;
        for (size_t i = 0; i < 16 && prev != next; ++i) {
            prev = next;
            for (const auto& [regex, fmt] : rules) {
                next = std::regex_replace(next.data(), regex, fmt);
            }
        }

        SubstGlobal(next, " ", "");
        SubstGlobal(next, "$$$", " ");

        Y_ENSURE(!next.Contains("~"));
        return next;
    }

    THashMap<TString, TString> MakeRegexByOtherNameMap(const NSQLReflect::TLexerGrammar& grammar, bool ansi) {
        TString mode = "DEFAULT";
        if (ansi) {
            mode = "ANSI";
        }

        THashMap<TString, TString> regexes;
        for (const auto& token : grammar.OtherNames) {
            regexes.emplace(token, ToRegex(token, mode, grammar));
        }
        return regexes;
    }

} // namespace NSQLTranslationV1
