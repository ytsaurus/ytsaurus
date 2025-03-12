#include "regex.h"

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

    void RegexReplace(TString& input, const TStringBuf pattern, const TStringBuf value) {
        std::string patternStr(pattern);
        std::string valueStr(value);

        std::regex re(patternStr);
        input = std::regex_replace(input.data(), re, valueStr);
    }

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

    THashMap<TString, TString> GetRewriteRules(const TStringBuf mode, const NSQLReflect::TLexerGrammar& grammar) {
        THashMap<TString, THashMap<TString, TString>> Substitutions = {
            SUBSTITUTIONS(DEFAULT),
            SUBSTITUTIONS(ANSI),
        };

        THashMap<TString, TString> rules;

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
        rules[R"('(.)'\.\.'(.)')"] = "[$1-$2]";
        rules[R"(\(\[([^\]\[\(\)]+)\]\))"] = "[$1]";
        rules[R"('(\d)')"] = "$1";
        rules[R"(\\\\ \.)"] = "\\\\ (.|\\n)";
        rules[R"(\(\.\))"] = "(.|\\n)";
        rules[R"(([^\\\(\.])\.([^\.]))"] = "$1(.|\\n)$2";

        return rules;
    }

    TString ToRegex(const TString& name, const TStringBuf mode, const NSQLReflect::TLexerGrammar& grammar) {
        TString regex = grammar.BlockByName.at(name);

        regex = ReEscaped(std::move(regex));

        THashMap<TString, TString> rules = GetRewriteRules(mode, grammar);

        TString prev;
        while (prev != regex) {
            prev = regex;
            for (const auto& [k, v] : rules) {
                RegexReplace(regex, k, v);
            }
        }

        SubstGlobal(regex, " ", "");
        SubstGlobal(regex, "$$$", " ");

        Y_ENSURE(!regex.Contains("~"));
        return regex;
    }

    THashMap<TString, TString> GetRegexByComplexTokenMap(const NSQLReflect::TLexerGrammar& grammar, bool ansi) {
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
