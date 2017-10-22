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
}

TEST(TNetworkAddressTest, ParseGoodIpv4WithPort)
{
    TNetworkAddress address = TNetworkAddress::Parse("[192.0.2.33]:1000");
    EXPECT_EQ("tcp://192.0.2.33:1000", ToString(address, true));
}

TEST(TNetworkAddressTest, ParseBadIpv4Address)
{
    EXPECT_ANY_THROW(TNetworkAddress::Parse("[192.0.XXX.33]")); // extra symbols
    EXPECT_ANY_THROW(TNetworkAddress::Parse("192.0.2.33")); // no brackets
    EXPECT_ANY_THROW(TNetworkAddress::Parse("[192.0.2.33]:")); // no port after colon
}

TEST(TNetworkAddressTest, ParseGoodIpv6)
{
    TNetworkAddress address = TNetworkAddress::Parse("[2001:db8:8714:3a90::12]");
    EXPECT_EQ("tcp://[2001:db8:8714:3a90::12]", ToString(address, false));
}

TEST(TNetworkAddressTest, ParseGoodIpv6WithPort)
{
    TNetworkAddress address = TNetworkAddress::Parse("[2001:db8:8714:3a90::12]:1000");
    EXPECT_EQ("tcp://[2001:db8:8714:3a90::12]:1000", ToString(address, true));
}

TEST(TNetworkAddressTest, ParseBadIpv6Address)
{
    EXPECT_ANY_THROW(TNetworkAddress::Parse("[2001:db8:SOME_STRING:3a90::12]")); // extra symbols
    EXPECT_ANY_THROW(TNetworkAddress::Parse("2001:db8:8714:3a90::12")); // no brackets
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
        ASSERT_EQ("0:0:0:0:0:0:0:0", ToString(address));
    }

    {
        TIP6Address address;
        address.GetRawWords()[0] = 3;
        ASSERT_EQ("0:0:0:0:0:0:0:3", ToString(address));
    }

    {
        TIP6Address address;
        address.GetRawWords()[7] = 0xfff1;
        ASSERT_EQ("fff1:0:0:0:0:0:0:0", ToString(address));
    }
}

TEST(TIP6AddressTest, FromString)
{
    {
        auto address = TIP6Address::FromString("0:0:0:0:0:0:0:0");
        ui16 parts[16] = {0, 0, 0, 0, 0, 0, 0, 0};
        EXPECT_EQ(0, ::memcmp(address.GetRawWords(), parts, 8));
    }

    {
        auto address = TIP6Address::FromString("0:0:0:0:0:0:0:3");
        ui16 parts[16] = {3, 0, 0, 0, 0, 0, 0, 0};
        EXPECT_EQ(0, ::memcmp(address.GetRawWords(), parts, 8));
    }

    {
        auto address = TIP6Address::FromString("fff1:1:3:4:5:6:7:8");
        ui16 parts[16] = {8, 7, 6, 5, 4, 3, 1, 0xfff1};
        EXPECT_EQ(0, ::memcmp(address.GetRawWords(), parts, 8));
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
