#include "regex.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NSQLTranslationV1;

namespace {
    auto grammar = NSQLReflect::LoadLexerGrammar();
    auto regexes = MakeRegexByTokenNameMap(grammar, /* ansi = */ false);
} // namespace

Y_UNIT_TEST_SUITE(SqlRegexTests) {
    Y_UNIT_TEST(StringValue) {
        UNIT_ASSERT_VALUES_EQUAL(
            regexes.at("STRING_VALUE"),
            "(((('([^'\\\\]|(\\\\(.|\\n)))*'))|((\"([^\"\\\\]|(\\\\(.|\\n)))*\"))|(((@@)(.|\\n)*?(@@))+@?))([sS]|[uU]|[yY]|[jJ]|[pP]([tT]|[bB]|[vV])?)?)");
    }

    Y_UNIT_TEST(IdPlain) {
        UNIT_ASSERT_VALUES_EQUAL(
            regexes.at("ID_PLAIN"),
            "([a-z]|[A-Z]|_)([a-z]|[A-Z]|_|[0-9])*");
    }

    Y_UNIT_TEST(IdQuoted) {
        UNIT_ASSERT_VALUES_EQUAL(
            regexes.at("ID_QUOTED"),
            "`(\\\\(.|\\n)|``|[^`\\\\])*`");
    }

    Y_UNIT_TEST(Digits) {
        UNIT_ASSERT_VALUES_EQUAL(
            regexes.at("DIGITS"),
            "([0-9]+)|(0[xX]([0-9]|[a-f]|[A-F])+)|(0[oO][0-8]+)|(0[bB](0|1)+)");
    }

    Y_UNIT_TEST(Real) {
        UNIT_ASSERT_VALUES_EQUAL(
            regexes.at("REAL"),
            "(([0-9]+)(\\.)[0-9]*([eE]((\\+)|-)?([0-9]+))?|([0-9]+)([eE]((\\+)|-)?([0-9]+)))([fF]|[pP]([fF](4|8)|[nN])?)?");
    }

    Y_UNIT_TEST(Ws) {
        UNIT_ASSERT_VALUES_EQUAL(
            regexes.at("WS"),
            "( |\\r|\\t|\\n)");
    }

    Y_UNIT_TEST(Comment) {
        UNIT_ASSERT_VALUES_EQUAL(
            regexes.at("COMMENT"),
            "((\\/\\*(.|\\n)*?\\*\\/)|(--[^\\n\\r]*(\\r\\n?|\\n|$)))");
    }

} // Y_UNIT_TEST_SUITE(SqlRegexTests)
