#include "sql_highlighter.h"

#include <library/cpp/testing/unittest/registar.h>

#define UNIT_ASSERT_HIGHLIGHTED(input, expected) \
    UNIT_ASSERT_VALUES_EQUAL(Mask(h, (input)), (expected))

using namespace NSQLHighlight;

char ToChar(EUnitKind kind) {
    switch (kind) {
        case EUnitKind::Keyword:
            return 'k';
        case EUnitKind::Punctuation:
            return 'p';
        case EUnitKind::Identifier:
            return 'i';
        case EUnitKind::QuotedIdentifier:
            return 'q';
        case EUnitKind::BindParamterIdentifier:
            return 'b';
        case EUnitKind::TypeIdentifier:
            return 't';
        case EUnitKind::FunctionIdentifier:
            return 'f';
        case EUnitKind::Literal:
            return 'l';
        case EUnitKind::StringLiteral:
            return 's';
        case EUnitKind::Comment:
            return 'c';
        case EUnitKind::Whitespace:
            return ' ';
        case EUnitKind::Error:
            return 'e';
    }
}

TString ToMask(const TVector<TToken>& tokens) {
    TString s;
    for (const auto& t : tokens) {
        s += TString(t.Length, ToChar(t.Kind));
    }
    return s;
}

TString Mask(IHighlighter::TPtr& h, TStringBuf text) {
    return ToMask(Tokenize(*h, text));
}

Y_UNIT_TEST_SUITE(SqlHighlighterTests) {
    Y_UNIT_TEST(Blank) {
        auto h = MakeHighlighter(MakeHighlighting(NSQLReflect::LoadLexerGrammar()));
        UNIT_ASSERT_HIGHLIGHTED("", "");
        UNIT_ASSERT_HIGHLIGHTED(" ", " ");
        UNIT_ASSERT_HIGHLIGHTED("   ", "   ");
        UNIT_ASSERT_HIGHLIGHTED("\n", " ");
        UNIT_ASSERT_HIGHLIGHTED("\n\n", "  ");
        UNIT_ASSERT_HIGHLIGHTED("\r\n", "  ");
        UNIT_ASSERT_HIGHLIGHTED("\r", " ");
        UNIT_ASSERT_HIGHLIGHTED("\r\n\n", "   ");
        UNIT_ASSERT_HIGHLIGHTED("\r\n\r\n", "    ");
    }

    Y_UNIT_TEST(Invalid) {
        auto h = MakeHighlighter(MakeHighlighting(NSQLReflect::LoadLexerGrammar()));
        UNIT_ASSERT_HIGHLIGHTED("!", "e");
        UNIT_ASSERT_HIGHLIGHTED("й", "ee");
        UNIT_ASSERT_HIGHLIGHTED("编", "eee");
        UNIT_ASSERT_HIGHLIGHTED("\xF0\x9F\x98\x8A", "eeee");
        UNIT_ASSERT_HIGHLIGHTED("!select", "ekkkkkk");
        UNIT_ASSERT_HIGHLIGHTED("!sselect", "eiiiiiii");
    }

    Y_UNIT_TEST(Keyword) {
        auto h = MakeHighlighter(MakeHighlighting(NSQLReflect::LoadLexerGrammar()));
        UNIT_ASSERT_HIGHLIGHTED("SELECT", "kkkkkk");
        UNIT_ASSERT_HIGHLIGHTED("select", "kkkkkk");
        UNIT_ASSERT_HIGHLIGHTED("ALTER", "kkkkk");
        UNIT_ASSERT_HIGHLIGHTED("GROUP BY", "kkkkk kk");
        UNIT_ASSERT_HIGHLIGHTED("INSERT", "kkkkkk");
    }

    Y_UNIT_TEST(Operation) {
        auto h = MakeHighlighter(MakeHighlighting(NSQLReflect::LoadLexerGrammar()));
        UNIT_ASSERT_HIGHLIGHTED("(1 + 21 / 4)", "pl p ll p lp");
        UNIT_ASSERT_HIGHLIGHTED("(1+21/4)", "plpllplp");
    }

    Y_UNIT_TEST(FunctionIdentifier) {
        auto h = MakeHighlighter(MakeHighlighting(NSQLReflect::LoadLexerGrammar()));
        UNIT_ASSERT_HIGHLIGHTED("MIN", "iii");
        UNIT_ASSERT_HIGHLIGHTED("min", "iii");
        UNIT_ASSERT_HIGHLIGHTED("MIN(123, 65)", "fffplllp llp");
        UNIT_ASSERT_HIGHLIGHTED("minimum", "iiiiiii");
        UNIT_ASSERT_HIGHLIGHTED("MINimum", "iiiiiii");
        UNIT_ASSERT_HIGHLIGHTED("Math::Sin", "fffffffff");
        UNIT_ASSERT_HIGHLIGHTED("Math", "iiii");
        UNIT_ASSERT_HIGHLIGHTED("Math::", "iiiipp");
        UNIT_ASSERT_HIGHLIGHTED("::Sin", "ppiii");
    }

    Y_UNIT_TEST(TypeIdentifier) {
        auto h = MakeHighlighter(MakeHighlighting(NSQLReflect::LoadLexerGrammar()));
        UNIT_ASSERT_HIGHLIGHTED("Bool", "tttt");
        UNIT_ASSERT_HIGHLIGHTED("Bool(value)", "ttttpiiiiip");
    }

    Y_UNIT_TEST(Identifier) {
        auto h = MakeHighlighter(MakeHighlighting(NSQLReflect::LoadLexerGrammar()));
        UNIT_ASSERT_HIGHLIGHTED("test", "iiii");
    }

    Y_UNIT_TEST(QuotedIdentifier) {
        auto h = MakeHighlighter(MakeHighlighting(NSQLReflect::LoadLexerGrammar()));
        UNIT_ASSERT_HIGHLIGHTED("`/cluster/database`", "qqqqqqqqqqqqqqqqqqq");
        UNIT_ASSERT_HIGHLIGHTED("`test`select", "qqqqqqkkkkkk");
        UNIT_ASSERT_HIGHLIGHTED("`/cluster", "epiiiiiii");
        UNIT_ASSERT_HIGHLIGHTED("`\xF0\x9F\x98\x8A`", "qqqqqq");
    }

    Y_UNIT_TEST(String) {
        auto h = MakeHighlighter(MakeHighlighting(NSQLReflect::LoadLexerGrammar()));
        UNIT_ASSERT_HIGHLIGHTED("\"\"", "ss");
        UNIT_ASSERT_HIGHLIGHTED("\"test\"", "ssssss");
        UNIT_ASSERT_HIGHLIGHTED("\"", "e");
        UNIT_ASSERT_HIGHLIGHTED("\"\"\"", "sse");
        UNIT_ASSERT_HIGHLIGHTED("\"\\\"", "eee");
        UNIT_ASSERT_HIGHLIGHTED("\"test select from", "eiiii kkkkkk kkkk");
        UNIT_ASSERT_HIGHLIGHTED("\"\\\"\"", "ssss");
        UNIT_ASSERT_HIGHLIGHTED("\"select\"select", "sssssssssiiiii");
        UNIT_ASSERT_HIGHLIGHTED("\"select\"group", "sssssssskkkkk");
        UNIT_ASSERT_HIGHLIGHTED("SELECT \\\"\xF0\x9F\x98\x8A\\\" FROM test", "kkkkkk eeeeeeee kkkk iiii");
    }

    Y_UNIT_TEST(Number) {
        auto h = MakeHighlighter(MakeHighlighting(NSQLReflect::LoadLexerGrammar()));
        UNIT_ASSERT_HIGHLIGHTED("1234", "llll");
        UNIT_ASSERT_HIGHLIGHTED("-123", "plll");
        UNIT_ASSERT_HIGHLIGHTED(
            "SELECT "
            "123l AS `Int64`, "
            "0b01u AS `Uint32`, "
            "0xfful AS `Uint64`, "
            "0o7ut AS `Uint8`, "
            "456s AS `Int16`, "
            "1.2345f AS `Float`;",
            "kkkkkk "
            "llll kk qqqqqqqp "
            "lllll kk qqqqqqqqp "
            "llllll kk qqqqqqqqp "
            "lllll kk qqqqqqqp "
            "llll kk qqqqqqqp "
            "lllllll kk qqqqqqqp");
    }

    Y_UNIT_TEST(Comment) {
        auto h = MakeHighlighter(MakeHighlighting(NSQLReflect::LoadLexerGrammar()));
        UNIT_ASSERT_HIGHLIGHTED("- select", "p kkkkkk");
        UNIT_ASSERT_HIGHLIGHTED("select -- select", "kkkkkk ccccccccc");
        UNIT_ASSERT_HIGHLIGHTED("-- select\nselect", "cccccccccckkkkkk");
        UNIT_ASSERT_HIGHLIGHTED("/* select */", "cccccccccccc");
        UNIT_ASSERT_HIGHLIGHTED("select /* select */ select", "kkkkkk cccccccccccc kkkkkk");
        UNIT_ASSERT_HIGHLIGHTED("/**/ --", "cccc cc");
        UNIT_ASSERT_HIGHLIGHTED("/*/**/*/", "ccccccpp");
    }

    Y_UNIT_TEST(SQL) {
        auto h = MakeHighlighter(MakeHighlighting(NSQLReflect::LoadLexerGrammar()));
        UNIT_ASSERT_HIGHLIGHTED(
            "SELECT id, alias from users",
            "kkkkkk iip iiiii kkkk iiiii");
        UNIT_ASSERT_HIGHLIGHTED(
            R"(INSERT INTO users (id, alias) VALUES (12, "tester"))",
            R"(kkkkkk kkkk iiiii piip iiiiip kkkkkk pllp ssssssssp)");
        UNIT_ASSERT_HIGHLIGHTED(
            R"(SELECT 123467, "Hello, {name}!", (1 + (5 * 1 / 0)), MIN(identifier) FROM `local/test/space/table` JOIN test;)",
            R"(kkkkkk llllllp ssssssssssssssssp pl p pl p l p lppp fffpiiiiiiiiiip kkkk qqqqqqqqqqqqqqqqqqqqqqqq kkkk iiiip)");
        UNIT_ASSERT_HIGHLIGHTED(
            "SELECT Bool(phone) FROM customer",
            "kkkkkk ttttpiiiiip kkkk iiiiiiii");
    }
} // Y_UNIT_TEST_SUITE(SqlHighlighterTests)
