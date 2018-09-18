#include <yt/core/test_framework/framework.h>

#include <yt/core/net/address.h>
#include <yt/core/net/socket.h>

#ifdef _unix_
    #include <sys/types.h>
    #include <sys/socket.h>
#endif

namespace NYT {
namespace NNet {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TNetworkAddressTest, ParseGoodIpv4)
{
    TNetworkAddress address = TNetworkAddress::Parse("[192.0.2.33]");
    EXPECT_EQ("tcp://192.0.2.33", ToString(address, false));

    address = TNetworkAddress::Parse("192.0.2.33");
    EXPECT_EQ("tcp://192.0.2.33", ToString(address, false));
}

TEST(TNetworkAddressTest, ParseGoodIpv4WithPort)
{
    TNetworkAddress address = TNetworkAddress::Parse("[192.0.2.33]:1000");
    EXPECT_EQ("tcp://192.0.2.33:1000", ToString(address, true));

    address = TNetworkAddress::Parse("192.0.2.33:1000");
    EXPECT_EQ("tcp://192.0.2.33:1000", ToString(address, true));
}

TEST(TNetworkAddressTest, ParseBadIpv4Address)
{
    EXPECT_ANY_THROW(TNetworkAddress::Parse("[192.0.XXX.33]")); // extra symbols
    EXPECT_ANY_THROW(TNetworkAddress::Parse("[192.0.2.33]:")); // no port after colon
}

TEST(TNetworkAddressTest, ParseGoodIpv6)
{
    TNetworkAddress address = TNetworkAddress::Parse("[2001:db8:8714:3a90::12]");
    EXPECT_EQ("tcp://[2001:db8:8714:3a90::12]", ToString(address, false));
}

TEST(TNetworkAddressTest, IP6Conversion)
{
    TNetworkAddress address = TNetworkAddress::Parse("[2001:db8:8714:3a90::12]");
    auto ip6Address = address.ToIP6Address();

    EXPECT_EQ("2001:db8:8714:3a90::12", ToString(ip6Address));
}

TEST(TNetworkAddressTest, ParseGoodIpv6WithPort)
{
    TNetworkAddress address = TNetworkAddress::Parse("[2001:db8:8714:3a90::12]:1000");
    EXPECT_EQ("tcp://[2001:db8:8714:3a90::12]:1000", ToString(address, true));
}

TEST(TNetworkAddressTest, ParseBadIpv6Address)
{
    EXPECT_ANY_THROW(TNetworkAddress::Parse("[2001:db8:SOME_STRING:3a90::12]")); // extra symbols
    EXPECT_ANY_THROW(TNetworkAddress::Parse("[2001:db8:8714:3a90::12]:")); // no port after colon
}

#ifdef _unix_

TEST(TNetworkAddressTest, UnixSocketName)
{
    int fds[2];
    YCHECK(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

    EXPECT_EQ("unix://[*unnamed*]", ToString(GetSocketName(fds[0])));

    close(fds[0]);
    close(fds[1]);

    auto address = TNetworkAddress::CreateUnixDomainAddress("abc");
    EXPECT_EQ("unix://[abc]", ToString(address));

    auto binaryString = TString("a\0c", 3);
    auto binaryAddress = TNetworkAddress::CreateUnixDomainAddress(binaryString);

    EXPECT_EQ(
        Format("%Qv", TString("unix://[a\\x00c]")),
        Format("%Qv", ToString(binaryAddress)));
}

#endif

////////////////////////////////////////////////////////////////////////////////

TEST(TIP6AddressTest, ToString)
{
    {
        TIP6Address address;
        ASSERT_EQ("::", ToString(address));
    }

    {
        TIP6Address address;
        address.GetRawWords()[0] = 3;
        ASSERT_EQ("::3", ToString(address));
    }

    {
        TIP6Address address;
        address.GetRawWords()[7] = 0xfff1;
        ASSERT_EQ("fff1::", ToString(address));
    }
}

TEST(TIP6AddressTest, InvalidAddress)
{
    for (const auto& addr : std::vector<TString>{
        ":::",
        "1::1::1",
        "0:1:2:3:4:5:6:7:8",
        "0:1:2:3:4:5:67777",
        "0:1:2:3:4:5:6:7:",
        ":0:1:2:3:4:5:6:7",
        ":1:2:3:4:5:6:7",
        "1:2:3:4:5:6:7:"
    }) {
        EXPECT_THROW(TIP6Address::FromString(addr), TErrorException)
            << addr;
    }
}

std::array<ui16, 8> AddressToWords(const TIP6Address& addr) {
    std::array<ui16, 8> buf;
    std::copy(addr.GetRawWords(), addr.GetRawWords() + 8, buf.begin());
    return buf;
}

TEST(TIP6AddressTest, FromString)
{
    typedef std::pair<TString, std::array<ui16, 8>> TTestCase;
    
    for (const auto& testCase : {
        TTestCase{"0:0:0:0:0:0:0:0", {0, 0, 0, 0, 0, 0, 0, 0}},
        TTestCase{"0:0:0:0:0:0:0:3", {3, 0, 0, 0, 0, 0, 0, 0}},
        TTestCase{"0:0:0:0::0:3", {3, 0, 0, 0, 0, 0, 0, 0}},
        TTestCase{"fff1:1:3:4:5:6:7:8", {8, 7, 6, 5, 4, 3, 1_KB / 1_KB, 0xfff1}},
        TTestCase{"::", {0, 0, 0, 0, 0, 0, 0, 0}},
        TTestCase{"::1", {1, 0, 0, 0, 0, 0, 0, 0}},
        TTestCase{"::1:2", {2, 1, 0, 0, 0, 0, 0, 0}},
        TTestCase{"1::", {0, 0, 0, 0, 0, 0, 0, 1}},
        TTestCase{"0:1::", {0, 0, 0, 0, 0, 0, 1, 0}},
        TTestCase{"0:1::1:0:0", {0, 0, 1, 0, 0, 0, 1, 0}},
        TTestCase{"ffab:3:0::1234:6", {0x6, 0x1234, 0, 0, 0, 0, 0x3, 0xffab}}
    }) {
        auto address = TIP6Address::FromString(testCase.first);
        EXPECT_EQ(AddressToWords(address), testCase.second);
    }
}

TEST(TIP6AddressTest, CanonicalText)
{
    for (const auto& str : std::vector<TString>{
        "::",
        "::1",
        "1::",
        "2001:db8::1:0:0:1",
        "::2001:db8:1:0:0:1",
        "::2001:db8:1:1:0:0",
        "2001:db8::1:1:0:0",
        "1:2:3:4:5:6:7:8"
    }) {
        auto address = TIP6Address::FromString(str);
        EXPECT_EQ(str, ToString(address));
    }
}

TEST(TIP6AddressTest, NetworkMask)
{
    using TTestCase = std::tuple<const char*, std::array<ui16, 8>, int>;
    for (const auto& testCase : {
        TTestCase{"::/1", {0, 0, 0, 0, 0, 0, 0, 0x8000}, 1},
        TTestCase{"::/0", {0, 0, 0, 0, 0, 0, 0, 0}, 0},
        TTestCase{"::/24", {0, 0, 0, 0, 0, 0, 0xff00, 0xffff}, 24},
        TTestCase{"::/32", {0, 0, 0, 0, 0, 0, 0xffff, 0xffff}, 32},
        TTestCase{"::/64", {0, 0, 0, 0, 0xffff, 0xffff, 0xffff, 0xffff}, 64},
        TTestCase{"::/128", {0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff}, 128},
    }) {
        auto network = TIP6Network::FromString(std::get<0>(testCase));
        EXPECT_EQ(AddressToWords(network.GetMask()), std::get<1>(testCase));
        EXPECT_EQ(network.GetMaskSize(), std::get<2>(testCase));
        EXPECT_EQ(ToString(network), std::get<0>(testCase));
    }

    EXPECT_THROW(TIP6Network::FromString("::/129"), TErrorException);
    EXPECT_THROW(TIP6Network::FromString("::/1291"), TErrorException);
}

TEST(TIP6AddressTest, InvalidInput)
{
    for (const auto& testCase : std::vector<TString>{
        "",
        ":",
        "::/",
        "::/1",
        ":::",
        "::1::",
        "1",
        "1:1",
        "11111::",
        "g::",
        "1:::1",
        "fff1:1:3:4:5:6:7:8:9",
        "fff1:1:3:4:5:6:7:8::",
        "::fff1:1:3:4:5:6:7:8"
    }) {
        EXPECT_THROW(TIP6Address::FromString(testCase), TErrorException)
            << Format("input = %Qv", testCase);

        auto network = testCase + "/32";
        EXPECT_THROW(TIP6Network::FromString(network), TErrorException)
            << Format("input = %Qv", network);        
    }
}

TEST(TIP6AddressTest, ToStringFromStringRandom)
{
    for (int i = 0; i < 100; ++i) {
        ui8 bytes[TIP6Address::ByteSize];
        for (int j = 0; j < TIP6Address::ByteSize; ++j) {
            bytes[j] = RandomNumber<ui8>();
        }

        auto address = TIP6Address::FromRawBytes(bytes);
        ASSERT_EQ(address, TIP6Address::FromString(ToString(address)));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NNet
} // namespace NYT
