#include "sql_reflect.h"

#include <library/cpp/testing/unittest/registar.h>

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
