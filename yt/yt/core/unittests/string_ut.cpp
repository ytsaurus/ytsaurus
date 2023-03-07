#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/string.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestCase
{
    const char* UnderCase;
    const char* CamelCase;
};

static std::vector<TTestCase> TestCases {
    { "kenny", "Kenny" },
    { "south_park", "SouthPark" },
    { "a", "A" },
    { "a_b_c", "ABC" },
    { "reed_solomon_6_3", "ReedSolomon_6_3" },
    { "lrc_12_2_2", "Lrc_12_2_2" },
    { "0", "0" },
    { "0_1_2", "0_1_2" },
    { "int64", "Int64" }
};

////////////////////////////////////////////////////////////////////////////////

TEST(TStringTest, UnderscoreCaseToCamelCase)
{
    for (const auto& testCase : TestCases) {
        auto result = UnderscoreCaseToCamelCase(testCase.UnderCase);
        EXPECT_STREQ(testCase.CamelCase, result.c_str())
            << "Original: \"" << testCase.UnderCase << '"';
    }
}

TEST(TStringTest, CamelCaseToUnderscoreCase)
{
    for (const auto& testCase : TestCases) {
        auto result = CamelCaseToUnderscoreCase(testCase.CamelCase);
        EXPECT_STREQ(testCase.UnderCase, result.c_str())
            << "Original: \"" << testCase.CamelCase << '"';
    }
}

DEFINE_ENUM(EColor,
    (Red)
    (BlackAndWhite)
);

DEFINE_BIT_ENUM(ELangs,
    ((None)       (0x00))
    ((Cpp)        (0x01))
    ((Go)         (0x02))
    ((Rust)       (0x04))
    ((Python)     (0x08))
    ((JavaScript) (0x10))
)

TEST(TStringTest, Enum)
{
    EXPECT_EQ("red", FormatEnum(EColor::Red));
    EXPECT_EQ(ParseEnum<EColor>("red"), EColor::Red);

    EXPECT_EQ("black_and_white", FormatEnum(EColor::BlackAndWhite));
    EXPECT_EQ(ParseEnum<EColor>("black_and_white"), EColor::BlackAndWhite);

    EXPECT_EQ("EColor(100)", FormatEnum(EColor(100)));

    EXPECT_EQ("java_script", FormatEnum(ELangs::JavaScript));
    EXPECT_EQ(ParseEnum<ELangs>("java_script"), ELangs::JavaScript);

    EXPECT_EQ("none", FormatEnum(ELangs::None));
    EXPECT_EQ(ParseEnum<ELangs>("none"), ELangs::None);

    EXPECT_EQ("cpp | go", FormatEnum(ELangs::Cpp | ELangs::Go));
    EXPECT_EQ(ParseEnum<ELangs>("cpp | go"), ELangs::Cpp | ELangs::Go);

    auto four = ELangs::Cpp | ELangs::Go | ELangs::Python | ELangs::JavaScript;
    EXPECT_EQ("cpp | go | python | java_script", FormatEnum(four));
    EXPECT_EQ(ParseEnum<ELangs>("cpp | go | python | java_script"), four);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

