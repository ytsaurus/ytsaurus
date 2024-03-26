#include <gtest/gtest.h>

#include <yt/yt/library/s3/crypto_helpers.h>

namespace NYT::NS3::NCrypto {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TS3CryptoHelpersTest, Lowercase)
{
    EXPECT_EQ("", Lowercase(""));
    EXPECT_EQ("abab", Lowercase("abAB"));
    EXPECT_EQ("abab++123`~abc\0\n\t", Lowercase("abAB++123`~abc\0\n\t"));
}

TEST(TS3CryptoHelpersTest, Hex)
{
    EXPECT_EQ("", Hex(""));
    EXPECT_EQ("31617e2b41", Hex("1a~+A"));
}

TEST(TS3CryptoHelpersTest, Sha256HashHex)
{
    EXPECT_EQ("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", Sha256HashHex(""));
    EXPECT_EQ("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad", Sha256HashHex("abc"));
}

TEST(TS3CryptoHelpersTest, HmacSha256)
{
    EXPECT_EQ("a8ab5f0aaac340cb04e761a34f4dd9b104084a4d666fb7e7e8d5104701579548", Hex(HmacSha256("123", "abcdef")));
}

TEST(TS3CryptoHelpersTest, Trim)
{
    EXPECT_EQ("", Trim(""));
    EXPECT_EQ("", Trim("    "));
    EXPECT_EQ("a b", Trim(" a b  "));
    EXPECT_EQ("a b", Trim(" a b"));
    EXPECT_EQ("a b", Trim("a b  "));
    EXPECT_EQ("a b", Trim("a b"));
}

TEST(TS3CryptoHelpersTest, UriEncode)
{
    EXPECT_EQ("abAb19-._~\%20\%2B\%2F\%0A", UriEncode("abAb19-._~ +/\n\0\t@!%", /*isObjectPath*/ false));
    EXPECT_EQ("abAb19-._~\%20\%2B/\%0A", UriEncode("abAb19-._~ +/\n\0\t@!%", /*isObjectPath*/ true));
}

TEST(TS3CryptoHelpersTest, FormatTimeIso8601)
{
    EXPECT_EQ("20130524T000000Z", NCrypto::FormatTimeIso8601(TInstant::ParseIso8601("2013-05-24T00:00:00Z")));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NS3::NCrypto
