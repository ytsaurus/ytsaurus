#include "sql_reflect.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NSQLReflect;

namespace {
    auto grammar = LoadLexerGrammar();
} // namespace

Y_UNIT_TEST_SUITE(SqlReflectTests) {
    Y_UNIT_TEST(Keywords) {
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetKeywords().contains("SELECT"), true);
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetKeywords().contains("INSERT"), true);
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetKeywords().contains("WHERE"), true);
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetKeywords().contains("COMMIT"), true);
    }

    Y_UNIT_TEST(Punctuation) {
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetPunctuation().contains("LPAREN"), true);
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetContentByName("LPAREN"), "(");

        UNIT_ASSERT_VALUES_EQUAL(grammar->GetPunctuation().contains("MINUS"), true);
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetContentByName("MINUS"), "-");

        UNIT_ASSERT_VALUES_EQUAL(grammar->GetPunctuation().contains("NAMESPACE"), true);
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetContentByName("NAMESPACE"), "::");
    }

    Y_UNIT_TEST(Other) {
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetOther().contains("REAL"), true);
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetOther().contains("STRING_VALUE"), true);
        UNIT_ASSERT_VALUES_EQUAL(grammar->GetOther().contains("STRING_MULTILINE"), false);

        UNIT_ASSERT_VALUES_EQUAL(
            grammar->GetContentByName("FLOAT_EXP"),
            "E (PLUS | MINUS)? DECDIGITS");
        UNIT_ASSERT_VALUES_EQUAL(
            grammar->GetContentByName("STRING_MULTILINE"),
            "(DOUBLE_COMMAT .*? DOUBLE_COMMAT)+ COMMAT?");
        UNIT_ASSERT_VALUES_EQUAL(
            grammar->GetContentByName("REAL"),
            "(DECDIGITS DOT DIGIT* FLOAT_EXP? | DECDIGITS FLOAT_EXP) (F | P (F ('4' | '8') | N)?)?");
    }

} // Y_UNIT_TEST_SUITE(SqlReflectTests)
