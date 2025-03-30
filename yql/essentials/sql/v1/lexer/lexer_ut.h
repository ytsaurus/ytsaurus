#pragma once

#include "lexer.h"

#define Y_UNIT_TEST_ON_EACH_LEXER_NAME(ANSI, ANTLR4, FLAVOR) \
    TestLexerName_ANSI_##ANSI##_ANTLR4_##ANTLR4##_FLAVOR_##FLAVOR

constexpr const char* Y_UNIT_TEST_ON_EACH_LEXER_NAME(false, false, Default) = "antlr3";
constexpr const char* Y_UNIT_TEST_ON_EACH_LEXER_NAME(false, true, Default) = "antlr4";
constexpr const char* Y_UNIT_TEST_ON_EACH_LEXER_NAME(true, false, Default) = "antlr3_ansi";
constexpr const char* Y_UNIT_TEST_ON_EACH_LEXER_NAME(true, true, Default) = "antlr4_ansi";
constexpr const char* Y_UNIT_TEST_ON_EACH_LEXER_NAME(false, true, Pure) = "antlr4_pure";
constexpr const char* Y_UNIT_TEST_ON_EACH_LEXER_NAME(true, true, Pure) = "antlr4_pure_ansi";
constexpr const char* Y_UNIT_TEST_ON_EACH_LEXER_NAME(false, false, Regex) = "regex";
constexpr const char* Y_UNIT_TEST_ON_EACH_LEXER_NAME(true, false, Regex) = "regex_ansi";

#define Y_UNIT_TEST_ON_EACH_LEXER_ADD_TEST(N, ANSI, ANTLR4, FLAVOR)                              \
    TCurrentTest::AddTest(                                                                       \
        Y_UNIT_TEST_ON_EACH_LEXER_NAME(ANSI, ANTLR4, FLAVOR),                                    \
        static_cast<void (*)(NUnitTest::TTestContext&)>(&N<ANSI, ANTLR4, ELexerFlavor::FLAVOR>), \
        /* forceFork = */ false)

#define Y_UNIT_TEST_ON_EACH_LEXER(N)                                      \
    template <bool ANSI, bool ANTLR4, ELexerFlavor FLAVOR>                \
    void N(NUnitTest::TTestContext&);                                     \
    struct TTestRegistration##N {                                         \
        TTestRegistration##N() {                                          \
            Y_UNIT_TEST_ON_EACH_LEXER_ADD_TEST(N, false, false, Default); \
            Y_UNIT_TEST_ON_EACH_LEXER_ADD_TEST(N, false, true, Default);  \
            Y_UNIT_TEST_ON_EACH_LEXER_ADD_TEST(N, true, false, Default);  \
            Y_UNIT_TEST_ON_EACH_LEXER_ADD_TEST(N, true, true, Default);   \
            Y_UNIT_TEST_ON_EACH_LEXER_ADD_TEST(N, false, true, Pure);     \
            Y_UNIT_TEST_ON_EACH_LEXER_ADD_TEST(N, true, true, Pure);      \
            Y_UNIT_TEST_ON_EACH_LEXER_ADD_TEST(N, false, false, Regex);   \
            Y_UNIT_TEST_ON_EACH_LEXER_ADD_TEST(N, true, false, Regex);    \
        }                                                                 \
    };                                                                    \
    static TTestRegistration##N testRegistration##N;                      \
    template <bool ANSI, bool ANTLR4, ELexerFlavor FLAVOR>                \
    void N(NUnitTest::TTestContext&)
