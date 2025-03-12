#include "lexer.h"

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/sql/settings/translation_settings.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NSQLTranslationV1;
using NSQLTranslation::SQL_MAX_PARSER_ERRORS;
using NSQLTranslation::Tokenize;
using NSQLTranslation::TParsedToken;
using NSQLTranslation::TParsedTokenList;
using NYql::TIssues;

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
    bool ok = Tokenize(*Lexer, query, "Test", tokens, issues, SQL_MAX_PARSER_ERRORS);

    TString out;
    if (!ok) {
        out = "[INVALID] ";
    }

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
            "PLUS(+) WS( ) MINUS(-) WS( ) STRUCT_OPEN(<|)");
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

    Y_UNIT_TEST(Query) {
        TString query =
            "SELECT\n"
            "  123467,\n"
            "  \"Hello, {name}!\",\n"
            "  (1 + (5 * 1 / 0)),\n"
            "  MIN(identifier),\n"
            "  Bool(field),\n"
            "  Math::Sin(var)\n"
            "FROM `local/test/space/table`\n"
            "JOIN test;";

        TString expected =
            "SELECT WS(\n) "
            "WS( ) WS( ) INTEGER_VALUE(123467) COMMA(,) WS(\n) "
            "WS( ) WS( ) STRING_VALUE(\"Hello, {name}!\") COMMA(,) WS(\n) "
            "WS( ) WS( ) LPAREN(() INTEGER_VALUE(1) WS( ) PLUS(+) WS( ) LPAREN(() INTEGER_VALUE(5) WS( ) "
            "ASTERISK(*) WS( ) INTEGER_VALUE(1) WS( ) SLASH(/) WS( ) INTEGER_VALUE(0) RPAREN()) "
            "RPAREN()) COMMA(,) WS(\n) "
            "WS( ) WS( ) ID_PLAIN(MIN) LPAREN(() ID_PLAIN(identifier) RPAREN()) COMMA(,) WS(\n) "
            "WS( ) WS( ) ID_PLAIN(Bool) LPAREN(() ID_PLAIN(field) RPAREN()) COMMA(,) WS(\n) "
            "WS( ) WS( ) ID_PLAIN(Math) NAMESPACE(::) ID_PLAIN(Sin) LPAREN(() ID_PLAIN(var) RPAREN()) WS(\n) "
            "FROM WS( ) ID_QUOTED(`local/test/space/table`) WS(\n) "
            "JOIN WS( ) ID_PLAIN(test) SEMICOLON(;)";

        Check(query, expected);
    }

    Y_UNIT_TEST(Invalid) {
        Check("\"", "[INVALID]");
        Check("\" SELECT", "[INVALID] WS( ) SELECT");
    }

} // Y_UNIT_TEST_SUITE(RegexLexerTests)
