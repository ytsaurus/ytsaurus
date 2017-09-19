#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/address.h>
#include <yt/core/misc/socket.h>

#ifdef _unix_
    #include <sys/types.h>
    #include <sys/socket.h>
#endif

namespace NYT {
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

} // namespace
} // namespace NYT
