#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/address.h>

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


////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
