#include <yt/core/test_framework/framework.h>

#include <yt/client/api/rpc_proxy/api_service_proxy.h>

namespace NYT::NRpcProxy {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TRpcProxyTest, CrashExample)
{
    unsigned char badString[] = {
        0x4f, 0x6d, 0x61, 0x78, 0x00, 0x00, 0x01, 0x00,
        0x52, 0xd5, 0xe9, 0xe6, 0x43, 0x55, 0xee, 0xa7,
        0x41, 0xeb, 0xa5, 0x88, 0x2e, 0x02, 0x00, 0x00,
        0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
    };

    NApi::NRpcProxy::NProto::TReqPingTransaction ping;
    EXPECT_FALSE(TryDeserializeProtoWithEnvelope(&ping, TRef{badString, sizeof(badString)}));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NRpcProxy
