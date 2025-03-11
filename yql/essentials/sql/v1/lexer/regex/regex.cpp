#include "regex.h"

#include <regex>

// TODO(vityaman): remove before a code review.
// #include <iostream>

namespace NSQLTranslationV1 {

    namespace {

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

        THashMap<TString, TString> GetRewriteRules(const TStringBuf mode, const NSQLReflect::TLexerGrammar& meta) {
            THashMap<TString, TString> rules;

            for (auto& [k, v] : meta.ContentByName) {
                rules["(\\b" + k + "\\b)"] = "(" + ReEscaped(v) + ")";
            }

            rules["(\\bEOF\\b)"] = "$";

            for (char letter = 'A'; letter <= 'Z'; ++letter) {
                TString lower(char(ToLower(letter)));
                TString upper(char(ToUpper(letter)));
                rules["(\\b" + TString(letter) + "\\b(?![-'\\]]))"] = "[" + lower + upper + "]";
            }

            for (const auto& [k, v] : meta.Substitutions.at(mode)) {
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

            // TODO(vityaman): remove before a code review.
            // std::cout << "Rewrite: " << std::endl;
            // for (const auto& [k, v] : rules) {
            //     std::cout << k << ": " << v << std::endl;
            // }

            return rules;
        }

        TString ToRegex(const TString& name, const TStringBuf mode, const NSQLReflect::TLexerGrammar& meta) {
            TString regex = meta.ContentByName.at(name);

            regex = ReEscaped(std::move(regex));

            // TODO(vityaman): remove before a code review.
            // std::cerr << "Input: " << regex << std::endl;

            THashMap<TString, TString> rules = GetRewriteRules(mode, meta);

            TString prev;
            while (prev != regex) {
                prev = regex;
                for (const auto& [k, v] : rules) {
                    RegexReplace(regex, k, v);
                    // TODO(vityaman): remove before a code review.
                    // std::cerr << "Input: " << regex << std::endl;
                }
            }

            SubstGlobal(regex, " ", "");
            SubstGlobal(regex, "$$$", " ");

            // TODO(vityaman): remove before a code review.
            // std::cerr << "Final: " << regex << std::endl;

            Y_ENSURE(!regex.Contains("~"));
            return regex;
        }

    } // namespace

    THashMap<TString, TString> GetRegexByComplexTokenMap(const NSQLReflect::TLexerGrammar& meta, bool ansi) {
        TString mode = "default";
        if (ansi) {
            mode = "ansi";
        }

        THashMap<TString, TString> regexes;
        for (const auto& token : meta.Other) {
            regexes.emplace(token, ToRegex(token, mode, meta));
        }
        return regexes;
    }

} // namespace NSQLTranslationV1
