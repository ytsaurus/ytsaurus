#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/yt/string/enum.h>
#include <library/cpp/yt/string/format.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

DEFINE_BIT_ENUM(ELangs,
    ((None)       (0x00))
    ((Cpp)        (0x01))
    ((Go)         (0x02))
    ((Rust)       (0x04))
    ((Python)     (0x08))
    ((JavaScript) (0x10))
    ((CppGo)      (0x03))
    ((All)        (0x1f))
);

TEST(TParseEnumTest, ParseEnumWithSuggestions)
{
    try {
        ParseEnum<ELangs>("Pyhon");
        FAIL() << "Expected TSimpleException to be thrown";
    } catch (const TSimpleException& e) {
        std::string errorMessage = e.what();
        std::string expectedSubstring = "closest possible values are \"Python\"";
        EXPECT_TRUE(errorMessage.find(expectedSubstring) != std::string::npos)
            << Format("Error message should contain %Qv: %v", expectedSubstring, errorMessage);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
