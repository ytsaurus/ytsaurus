#include "sql_reflect.h"

#include <library/cpp/testing/unittest/registar.h>

#include <regex>
#include <iostream>

using namespace NSQLReflect;

Y_UNIT_TEST_SUITE(SqlReflectTests) {
    Y_UNIT_TEST(Sandbox) {
        auto meta = GetGrammarMeta();

        UNIT_ASSERT_VALUES_EQUAL(meta.Keywords.contains("SELECT"), true);
        UNIT_ASSERT_VALUES_EQUAL(meta.Keywords.contains("INSERT"), true);
        UNIT_ASSERT_VALUES_EQUAL(meta.Keywords.contains("WHERE"), true);
        UNIT_ASSERT_VALUES_EQUAL(meta.Keywords.contains("COMMIT"), true);

        UNIT_ASSERT_VALUES_EQUAL(meta.Punctuation.contains("LPAREN"), true);
        UNIT_ASSERT_VALUES_EQUAL(meta.Punctuation.contains("MINUS"), true);
        UNIT_ASSERT_VALUES_EQUAL(meta.Punctuation.contains("NAMESPACE"), true);

        UNIT_ASSERT_VALUES_EQUAL(meta.Other.contains("REAL"), true);
        UNIT_ASSERT_VALUES_EQUAL(meta.Other.contains("STRING_VALUE"), true);
        UNIT_ASSERT_VALUES_EQUAL(meta.Other.contains("STRING_MULTILINE"), false);

        UNIT_ASSERT_VALUES_EQUAL(meta.ContentByName.at("LPAREN"), "(");
        UNIT_ASSERT_VALUES_EQUAL(meta.ContentByName.at("MINUS"), "-");
        UNIT_ASSERT_VALUES_EQUAL(meta.ContentByName.at("NAMESPACE"), "::");

        UNIT_ASSERT_VALUES_EQUAL(meta.ContentByName.at("FLOAT_EXP"), "E (PLUS | MINUS)? DECDIGITS");
        UNIT_ASSERT_VALUES_EQUAL(meta.ContentByName.at("STRING_MULTILINE"), "(DOUBLE_COMMAT .*? DOUBLE_COMMAT)+ COMMAT?");
        UNIT_ASSERT_VALUES_EQUAL(
            meta.ContentByName.at("REAL"),
            "(DECDIGITS DOT DIGIT* FLOAT_EXP? | DECDIGITS FLOAT_EXP) (F | P (F ('4' | '8') | N)?)?");
    }
} // Y_UNIT_TEST_SUITE(SqlReflectTests)

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
    SubstGlobal(text, "'/*'", "\\/\\*");
    SubstGlobal(text, "'*/'", "\\*\\/");
    SubstGlobal(text, "'\\n'", "\\n");
    SubstGlobal(text, "'\\r'", "\\r");
    SubstGlobal(text, "'--'", "--");
    return text;
}

THashMap<TString, TString> GetRewriteRules(const TStringBuf mode, const TGrammarMeta& meta) {
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
    rules[R"('(.)'\.\.'(.)')"]= "[$1-$2]";
    rules[R"(\(\[([^\]\[\(\)]+)\]\))"]= "[$1]";
    rules[R"('(\d)')"] = "$1";

    std::cout << "Rewrite: " << std::endl;
    for (const auto& [k, v] : rules) {
        std::cout << k << ": " << v << std::endl;
    }

    return rules;
}

TString ToRegex(const TString& name, const TStringBuf mode, const TGrammarMeta& meta) {
    TString regex = meta.ContentByName.at(name);

    std::cerr << "Input: " << regex << std::endl;
    
    THashMap<TString, TString> rules = GetRewriteRules(mode, meta);

    TString prev;
    while (prev != regex) {
        prev = regex;
        for (const auto& [k, v] : rules) {
            RegexReplace(regex, k, v);
            std::cerr << "Input: " << regex << std::endl;
        }
    }

    SubstGlobal(regex, " ", "");

    std::cerr << "Final: " << regex << std::endl;

    Y_ENSURE(!regex.Contains("~"));
    return regex;
}

Y_UNIT_TEST_SUITE(SqlRegexTests) {
    Y_UNIT_TEST(Sandbox) {
        auto meta = GetGrammarMeta();

        UNIT_ASSERT_VALUES_EQUAL(
            ToRegex("STRING_VALUE", "default", meta),
            "(((('([^'\\\\]|(\\\\.))*'))|((\"([^\"\\\\]|(\\\\.))*\"))|(((@@).*?(@@))+@?))([sS]|[uU]|[yY]|[jJ]|[pP]([tT]|[bB]|[vV])?)?)"
        );

        UNIT_ASSERT_VALUES_EQUAL(
            ToRegex("ID_PLAIN", "default", meta),
            "([a-z]|[A-Z]|'_')([a-z]|[A-Z]|'_'|[0-9])*"
        );

        UNIT_ASSERT_VALUES_EQUAL(
            ToRegex("ID_QUOTED", "default", meta),
            "`(\\\\.|``|[^`\\\\])*`"
        );

        UNIT_ASSERT_VALUES_EQUAL(
            ToRegex("DIGITS", "default", meta),
            "([0-9]+)|(0[xX]([0-9]|[a-f]|[A-F])+)|(0[oO][0-8]+)|(0[bB](0|1)+)"
        );

        UNIT_ASSERT_VALUES_EQUAL(
            ToRegex("REAL", "default", meta),
            "(([0-9]+)(\\.)[0-9]*([eE]((\\+)|-)?([0-9]+))?|([0-9]+)([eE]((\\+)|-)?([0-9]+)))([fF]|[pP]([fF](4|8)|[nN])?)?"
        );

        UNIT_ASSERT_VALUES_EQUAL(
            ToRegex("COMMENT", "default", meta),
            "((\\/\\*.*?\\*\\/)|(--[^\\n\\r]*(\\r\\n?|\\n|$)))"
        );
    }
}
