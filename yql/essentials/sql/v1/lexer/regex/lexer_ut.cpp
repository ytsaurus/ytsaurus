#include "lexer.h"

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/sql/settings/translation_settings.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NSQLTranslationV1;
using NSQLTranslation::Tokenize;
using NSQLTranslation::TParsedToken;
using NSQLTranslation::TParsedTokenList;
using NYql::TIssues;
using NSQLTranslation::SQL_MAX_PARSER_ERRORS;

TString ToString(TParsedToken token) {
    TString& string = token.Name;
    if (token.Name != token.Content) {
        string += "(";
        string += token.Content;
        string += ")";
    }
    return string;
}

TString Tokenized(const TString& query) {
    static auto Lexer = MakeRegexLexer(/* ansi = */ false);

    TParsedTokenList tokens;
    TIssues issues;
    Y_ENSURE(Tokenize(*Lexer, query, "Test", tokens, issues, SQL_MAX_PARSER_ERRORS));

    TString out;
    for (auto& token : tokens) {
        out += ToString(std::move(token));
        out += " ";
    }
    if (!out.empty()) {
        out.pop_back();
    }
    return out;
}

void Check(TString input, TString expected) {
    TString actual = Tokenized(input);
    UNIT_ASSERT_VALUES_EQUAL(actual, expected);
}

// TODO(vityaman): create some common test suite parametrized by a ILexer
Y_UNIT_TEST_SUITE(RegexLexerTests) {
    Y_UNIT_TEST(Whitespace) {
        Check("", "");
        Check(" ", "WS( )");
        Check("  ", "WS( ) WS( )");
        Check("\n", "WS(\n)");
    }

    Y_UNIT_TEST(SinleLineComment) {
        Check("--yql", "COMMENT(--yql)");
        Check("--  yql ", "COMMENT(--  yql )");
        Check("-- yql\nSELECT", "COMMENT(-- yql\n) SELECT");
        Check("-- yql --", "COMMENT(-- yql --)");
    }

    Y_UNIT_TEST(MultiLineComment) {
        Check("/* yql */", "COMMENT(/* yql */)");
        Check("/* yql */ */", "COMMENT(/* yql */) WS( ) ASTERISK(*) SLASH(/)");
        Check("/* /* yql */", "COMMENT(/* /* yql */)");
        Check("/* yql\n * yql\n */", "COMMENT(/* yql\n * yql\n */)");
    }

    Y_UNIT_TEST(Keyword) {
        Check("SELECT", "SELECT");
        Check("INSERT", "INSERT");
        Check("FROM", "FROM");
    }

    Y_UNIT_TEST(Punctuation) {
        Check(
            "* / + - <|", 
            "ASTERISK(*) WS( ) SLASH(/) WS( ) "
            "PLUS(+) WS( ) MINUS(-) WS( ) STRUCT_OPEN(<|)"
        );
        (Check("SELECT*FROM", "SELECT ASTERISK(*) FROM"));
    }

    Y_UNIT_TEST(IdPlain) {
        Check("variable my_table", "ID_PLAIN(variable) WS( ) ID_PLAIN(my_table)");
    }

    Y_UNIT_TEST(IdQuoted) {
        Check("``", "ID_QUOTED(``)");
        Check("` `", "ID_QUOTED(` `)");
        Check("` `", "ID_QUOTED(` `)");
        Check("`local/table`", "ID_QUOTED(`local/table`)");
    }

    Y_UNIT_TEST(SinleLineString) {
        Check("\"\"", "STRING_VALUE(\"\")");
        Check("\' \'", "STRING_VALUE(\' \')");
        Check("\" \"", "STRING_VALUE(\" \")");
        Check("\"test\"", "STRING_VALUE(\"test\")");
        Check("\"\\\"\"", "STRING_VALUE(\"\\\"\")");
    }

    Y_UNIT_TEST(MultiLineString) {
        Check("@@@@", "STRING_VALUE(@@@@)");
        Check("@@ @@@", "STRING_VALUE(@@ @@@)");
        Check("@@test@@", "STRING_VALUE(@@test@@)");
        Check("@@line1\nline2@@", "STRING_VALUE(@@line1\nline2@@)");
    }

    Y_UNIT_TEST(TODORemove) {
        UNIT_ASSERT_VALUES_EQUAL(
            Tokenized("SELECT *"), 
            "SELECT WS( ) ASTERISK(*)");
        UNIT_ASSERT_VALUES_EQUAL(
            Tokenized("SELECT*"),
            "SELECT ASTERISK(*)");
        UNIT_ASSERT_VALUES_EQUAL(
            Tokenized("SELECT * /* yql */ FROM"), 
            "SELECT WS( ) ASTERISK(*) WS( ) COMMENT(/* yql */) WS( ) FROM");
        UNIT_ASSERT_VALUES_EQUAL(
            Tokenized("SELECT `a` FROM my_table"), 
            "SELECT WS( ) ID_QUOTED(`a`) WS( ) FROM WS( ) ID_PLAIN(my_table)");
    }
} // Y_UNIT_TEST_SUITE(RegexLexerTests)
