#include "sql_reflect.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NSQLReflect;

namespace {
    auto grammar = LoadLexerGrammar();
} // namespace

Y_UNIT_TEST_SUITE(SqlReflectTests) {
    Y_UNIT_TEST(Keywords) {
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetKeywordTokenNames().contains("SELECT"), true);
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetKeywordTokenNames().contains("INSERT"), true);
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetKeywordTokenNames().contains("WHERE"), true);
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetKeywordTokenNames().contains("COMMIT"), true);
    }

    Y_UNIT_TEST(Punctuation) {
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetPunctuationTokenNames().contains("LPAREN"), true);
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetRuleBlockByTokenName("LPAREN"), "(");

        UNIT_ASSERT_VALUES_EQUAL(grammar->GetPunctuationTokenNames().contains("MINUS"), true);
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetRuleBlockByTokenName("MINUS"), "-");

        UNIT_ASSERT_VALUES_EQUAL(grammar->GetPunctuationTokenNames().contains("NAMESPACE"), true);
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetRuleBlockByTokenName("NAMESPACE"), "::");
    }

    Y_UNIT_TEST(Other) {
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetOtherTokenNames().contains("REAL"), true);
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetOtherTokenNames().contains("STRING_VALUE"), true);
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetOtherTokenNames().contains("STRING_MULTILINE"), false);

        UNIT_ASSERT_VALUES_EQUAL(
            grammar->GetRuleBlockByTokenName("FLOAT_EXP"),
            "E (PLUS | MINUS)? DECDIGITS");
        UNIT_ASSERT_VALUES_EQUAL(
            grammar->GetRuleBlockByTokenName("STRING_MULTILINE"),
            "(DOUBLE_COMMAT .*? DOUBLE_COMMAT)+ COMMAT?");
        UNIT_ASSERT_VALUES_EQUAL(
            grammar->GetRuleBlockByTokenName("REAL"),
            "(DECDIGITS DOT DIGIT* FLOAT_EXP? | DECDIGITS FLOAT_EXP) (F | P (F ('4' | '8') | N)?)?");
    }

} // Y_UNIT_TEST_SUITE(SqlReflectTests)
