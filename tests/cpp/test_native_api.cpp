#include <yt/core/test_framework/framework.h>

#include <yt/ytlib/api/native_client.h>
#include <yt/ytlib/api/native_connection.h>
#include <yt/ytlib/api/config.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/yson/string.h>

#include <cstdlib>

namespace NYT {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TApiTestBase
    : public ::testing::Test
{
protected:
    static INativeConnectionPtr Connection_;
    static INativeClientPtr Client_;

    static void SetUpTestCase()
    {
        const auto* driverConfigPath = std::getenv("YT_DRIVER_CONFIG_PATH");
        TIFStream configStream(driverConfigPath);
        auto config = ConvertTo<TNativeConnectionConfigPtr>(&configStream);
        Connection_ = CreateNativeConnection(config);

        TClientOptions clientOptions;
        clientOptions.User = "root";
        Client_ = Connection_->CreateNativeClient(clientOptions);
    }

    static void TearDownTestCase()
    {
        Client_.Reset();
        Connection_.Reset();
    }
};

INativeConnectionPtr TApiTestBase::Connection_;
INativeClientPtr TApiTestBase::Client_;

////////////////////////////////////////////////////////////////////////////////

TEST_F(TApiTestBase, TestClusterConnection)
{
    auto resOrError = Client_->GetNode(TYPath("/"));
    EXPECT_TRUE(resOrError.Get().IsOK());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
