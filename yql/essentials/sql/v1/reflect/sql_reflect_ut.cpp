#include "sql_reflect.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NSQLReflect;

namespace {
    auto grammar = LoadLexerGrammar();
} // namespace

Y_UNIT_TEST_SUITE(SqlReflectTests) {
    Y_UNIT_TEST(Keywords) {
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetKeywordNames().contains("SELECT"), true);
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetKeywordNames().contains("INSERT"), true);
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetKeywordNames().contains("WHERE"), true);
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetKeywordNames().contains("COMMIT"), true);
    }

    Y_UNIT_TEST(Punctuation) {
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetPunctuationNames().contains("LPAREN"), true);
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetBlockByName("LPAREN"), "(");

        UNIT_ASSERT_VALUES_EQUAL(grammar->GetPunctuationNames().contains("MINUS"), true);
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetBlockByName("MINUS"), "-");

        UNIT_ASSERT_VALUES_EQUAL(grammar->GetPunctuationNames().contains("NAMESPACE"), true);
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetBlockByName("NAMESPACE"), "::");
    }

    Y_UNIT_TEST(Other) {
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetOtherNames().contains("REAL"), true);
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetOtherNames().contains("STRING_VALUE"), true);
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetOtherNames().contains("STRING_MULTILINE"), false);

        UNIT_ASSERT_VALUES_EQUAL(
            grammar->GetBlockByName("FLOAT_EXP"),
            "E (PLUS | MINUS)? DECDIGITS");
        UNIT_ASSERT_VALUES_EQUAL(
            grammar->GetBlockByName("STRING_MULTILINE"),
            "(DOUBLE_COMMAT .*? DOUBLE_COMMAT)+ COMMAT?");
        UNIT_ASSERT_VALUES_EQUAL(
            grammar->GetBlockByName("REAL"),
            "(DECDIGITS DOT DIGIT* FLOAT_EXP? | DECDIGITS FLOAT_EXP) (F | P (F ('4' | '8') | N)?)?");
    }

} // Y_UNIT_TEST_SUITE(SqlReflectTests)
