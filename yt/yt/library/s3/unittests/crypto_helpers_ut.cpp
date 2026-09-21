#include <gtest/gtest.h>

#include <yt/yt/library/s3/crypto_helpers.h>

#include <string>

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
    const char bytes[] = "abAb19-._~ +/\n\0\t@!%";
    const std::string value(bytes, sizeof(bytes) - 1);
    EXPECT_EQ("abAb19-._~%20%2B%2F%0A%00%09%40%21%25", UriEncode(value, /*isObjectPath*/ false));
    EXPECT_EQ("abAb19-._~%20%2B/%0A%00%09%40%21%25", UriEncode(value, /*isObjectPath*/ true));
}

TEST(TS3CryptoHelpersTest, UriEncodeUtf8Bytes)
{
    EXPECT_EQ("%D0%90%D1%8F", UriEncode("Ая", /*isObjectPath*/ false));
    EXPECT_EQ("caf%C3%A9", UriEncode("café", /*isObjectPath*/ false));
    EXPECT_EQ("%F0%9F%98%80", UriEncode("😀", /*isObjectPath*/ false));
}

TEST(TS3CryptoHelpersTest, UriEncodeObjectPathSlashes)
{
    EXPECT_EQ("/%D0%90/caf%C3%A9/%F0%9F%98%80", UriEncode("/А/café/😀", /*isObjectPath*/ true));
}

TEST(TS3CryptoHelpersTest, UriEncodeQuerySlashes)
{
    EXPECT_EQ("%2F%D0%90%2Fcaf%C3%A9%2F%F0%9F%98%80", UriEncode("/А/café/😀", /*isObjectPath*/ false));
    EXPECT_EQ("next%2Bpage%2Fa%3D%26%25", UriEncode("next+page/a=&%", /*isObjectPath*/ false));
}

TEST(TS3CryptoHelpersTest, UriEncodeLiteralPercent)
{
    EXPECT_EQ("photo%25201.jpeg", UriEncode("photo%201.jpeg", /*isObjectPath*/ true));
    EXPECT_EQ("100%25", UriEncode("100%", /*isObjectPath*/ false));
}

TEST(TS3CryptoHelpersTest, FormatTimeIso8601)
{
    EXPECT_EQ("20130524T000000Z", NCrypto::FormatTimeIso8601(TInstant::ParseIso8601("2013-05-24T00:00:00Z")));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NS3::NCrypto
