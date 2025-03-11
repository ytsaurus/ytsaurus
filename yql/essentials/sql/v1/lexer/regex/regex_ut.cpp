#include "regex.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NSQLTranslationV1;

Y_UNIT_TEST_SUITE(SqlRegexTests) {
    Y_UNIT_TEST(Sandbox) {
        auto meta = NSQLReflect::GetGrammarMeta();
        auto regexes = GetRegexByComplexTokenMap(meta, /* ansi = */ false);

        UNIT_ASSERT_VALUES_EQUAL(
            regexes.at("STRING_VALUE"),
            "(((('([^'\\\\]|(\\\\.))*'))|((\"([^\"\\\\]|(\\\\.))*\"))|(((@@).*?(@@))+@?))([sS]|[uU]|[yY]|[jJ]|[pP]([tT]|[bB]|[vV])?)?)");

        UNIT_ASSERT_VALUES_EQUAL(
            regexes.at("ID_PLAIN"),
            "([a-z]|[A-Z]|_)([a-z]|[A-Z]|_|[0-9])*");

        UNIT_ASSERT_VALUES_EQUAL(
            regexes.at("ID_QUOTED"),
            "`(\\\\.|``|[^`\\\\])*`");

        UNIT_ASSERT_VALUES_EQUAL(
            regexes.at("DIGITS"),
            "([0-9]+)|(0[xX]([0-9]|[a-f]|[A-F])+)|(0[oO][0-8]+)|(0[bB](0|1)+)");

        UNIT_ASSERT_VALUES_EQUAL(
            regexes.at("REAL"),
            "(([0-9]+)(\\.)[0-9]*([eE]((\\+)|-)?([0-9]+))?|([0-9]+)([eE]((\\+)|-)?([0-9]+)))([fF]|[pP]([fF](4|8)|[nN])?)?");

        UNIT_ASSERT_VALUES_EQUAL(
            regexes.at("WS"),
            "( |\\r|\\t|\\n)");

        UNIT_ASSERT_VALUES_EQUAL(
            regexes.at("COMMENT"),
            "((\\/\\*(.|\\n)*?\\*\\/)|(--[^\\n\\r]*(\\r\\n?|\\n|$)))");
    }
} // Y_UNIT_TEST_SUITE(SqlRegexTests)
