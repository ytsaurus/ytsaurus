#include <yt/core/test_framework/framework.h>

#include <yp/server/net/net_manager.h>

namespace NYP::NServer::NNet::NTests {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TestNetManager, TestIp6ToPtrDnsRecord)
{
    TString address = "2001:0db8:85a3:0000:0000:8a2e:0370:7334";
    EXPECT_EQ("4.3.3.7.0.7.3.0.e.2.a.8.0.0.0.0.0.0.0.0.3.a.5.8.8.b.d.0.1.0.0.2.ip6.arpa.", BuildIp6PtrDnsAddress(address));
}

TEST(TestNetManager, TestShortIp6ToPtrDnsRecord)
{
    TString address = "2001:db8::1";
    EXPECT_EQ("1.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.8.b.d.0.1.0.0.2.ip6.arpa.", BuildIp6PtrDnsAddress(address));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYP::NServer::NNet::NTests
