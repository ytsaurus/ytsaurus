#include "lexer.h"

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/sql/settings/translation_settings.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NSQLTranslationV1;
using NSQLTranslation::Tokenize;
using NSQLTranslation::TParsedTokenList;
using NYql::TIssues;
using NSQLTranslation::SQL_MAX_PARSER_ERRORS;

TString Tokenized(const TString& query) {
    static auto Lexer = MakeRegexLexer(/* ansi = */ false);

    TParsedTokenList tokens;
    TIssues issues;
    Y_ENSURE(Tokenize(*Lexer, query, "Test", tokens, issues, SQL_MAX_PARSER_ERRORS));

    TString out;
    for (auto& token : tokens) {
        out += "(";
        out += token.Name;
        if (token.Name != token.Content) {
            out += " '";
            out += token.Content;
            out += "'";
        }
        out += ") ";
    }
    if (!out.empty()) {
        out.pop_back();
    }
    return out;
}

Y_UNIT_TEST_SUITE(RegexLexerTests) {
    Y_UNIT_TEST(Sandbox) {
        UNIT_ASSERT_VALUES_EQUAL(
            Tokenized(""), 
            "");
        UNIT_ASSERT_VALUES_EQUAL(
            Tokenized("SELECT"), 
            "(SELECT)");
        UNIT_ASSERT_VALUES_EQUAL(
            Tokenized("SELECT *"), 
            "(SELECT) (WS ' ') (ASTERISK '*')");
        UNIT_ASSERT_VALUES_EQUAL(
            Tokenized("SELECT*"),
            "(SELECT) (ASTERISK '*')");
        UNIT_ASSERT_VALUES_EQUAL(
            Tokenized("SELECT * /* yql */ FROM"), 
            "(SELECT) (WS ' ') (ASTERISK '*') (WS ' ') (COMMENT '/* yql */') (WS ' ') (FROM)");
        UNIT_ASSERT_VALUES_EQUAL(
            Tokenized("SELECT `a` FROM my_table"), 
            "(SELECT) (WS ' ') (ID_QUOTED '`a`') (WS ' ') (FROM) (WS ' ') (ID_PLAIN 'my_table')");
    }
} // Y_UNIT_TEST_SUITE(RegexLexerTests)
