#include <yt/yt/tests/cpp/test_base/api_test_base.h>
#include <yt/yt/tests/cpp/test_base/private.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/yt/server/lib/cross_cluster_replicated_state/cross_cluster_client_detail.h>
#include <yt/yt/server/lib/cross_cluster_replicated_state/config.h>
#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/config.h>
#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/transaction_client/config.h>

#include <yt/yt/ytlib/ban_client/ban_service_proxy.h>

#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/client/api/internal_client.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/yson/protobuf_helpers.h>

#include <util/datetime/base.h>

#include <util/random/random.h>

#include <thread>


////////////////////////////////////////////////////////////////////////////////

namespace NYT::NCppTests {
namespace {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NRpc;
using namespace NSecurityClient;
using namespace NSignature;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

using namespace std::literals;

////////////////////////////////////////////////////////////////////////////////

// const auto Logger = CppTestsLogger;

////////////////////////////////////////////////////////////////////////////////

class TBanServiceTest
    : public TApiTestBase
{
public:
    void SetUp() override
    {
        TApiTestBase::SetUp();

        using namespace NCrossClusterReplicatedState;

        TCreateObjectOptions userOptions;
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("name", "user");
        userOptions.Attributes = std::move(attributes);
        userOptions.IgnoreExisting = true;
        WaitFor(Client_->CreateObject(EObjectType::User, userOptions))
            .ThrowOnError();

        auto config = New<TCrossClusterReplicatedStateConfig>();
        config->Replicas = {New<TCrossClusterStateReplicaConfig>(), New<TCrossClusterStateReplicaConfig>(), New<TCrossClusterStateReplicaConfig>()};
        config->Replicas[0]->ClusterName = "remote_0";
        config->Replicas[1]->ClusterName = "remote_1";
        config->Replicas[2]->ClusterName = "remote_2";

        auto primaryConnection = DynamicPointerCast<NNative::IConnection>(Connection_);
        WaitFor(primaryConnection->GetClusterDirectorySynchronizer()->GetFirstSuccessfulSyncFuture())
            .ThrowOnError();
        auto connections = CreateClusterConnections(primaryConnection, config);
        auto clients = CreateClusterClients(connections, NNative::TClientOptions::Root());

        TRemoveNodeOptions rOptions;
        rOptions.Recursive = true;
        rOptions.Force = true;
        TCreateNodeOptions cOptions;
        cOptions.IgnoreExisting = true;
        for (const auto& client : clients) {
            WaitFor(client->RemoveNode("//tmp/banned_users", rOptions))
                .ThrowOnError();
            WaitFor(client->CreateNode("//tmp/banned_users", EObjectType::MapNode, cOptions))
                .ThrowOnError();
        }
        WaitFor(Client_->SetNode("//sys/cypress_proxies/@config/ban_service/enable", TYsonString("%true"sv)))
            .ThrowOnError();
        WaitFor(Client_->SetNode("//sys/cypress_proxies/@config/ban_service/use_in_object_service", TYsonString("%true"sv)))
            .ThrowOnError();

        Sleep(TDuration::Seconds(2));
    }
};


TEST_F(TBanServiceTest, BanService)
{
    auto nativeClient = DynamicPointerCast<NApi::NNative::IClient>(Client_);
    auto isBanned = WaitFor(nativeClient->GetUserBanned("user"))
        .ValueOrThrow();
    EXPECT_FALSE(isBanned);
    auto bannedList = WaitFor(nativeClient->ListBannedUsers())
        .ValueOrThrow();
    EXPECT_TRUE(bannedList.empty());

    WaitFor(nativeClient->SetUserBanned("user", true))
        .ThrowOnError();
    Sleep(TDuration::Seconds(2));
    isBanned = WaitFor(nativeClient->GetUserBanned("user"))
        .ValueOrThrow();
    EXPECT_TRUE(isBanned);

    auto userClient = Connection_->CreateClient(NNative::TClientOptions::FromUser("user"));
    try {
        WaitFor(userClient->CreateNode("//tmp/table", NObjectClient::EObjectType::Table))
            .ThrowOnError();
        ADD_FAILURE();
    } catch (const TErrorException& error) {
        EXPECT_TRUE(std::string_view(error.what()).contains(R"(User "user" is banned via ban service)"));
    }

    bannedList = WaitFor(nativeClient->ListBannedUsers())
        .ValueOrThrow();
    EXPECT_EQ(bannedList, std::vector{"user"s});

    WaitFor(nativeClient->SetUserBanned("user", false))
        .ThrowOnError();
    Sleep(TDuration::Seconds(2));
    isBanned = WaitFor(nativeClient->GetUserBanned("user"))
        .ValueOrThrow();
    EXPECT_FALSE(isBanned);

    WaitFor(userClient->CreateNode("//tmp/table", NObjectClient::EObjectType::Table))
        .ThrowOnError();

    bannedList = WaitFor(nativeClient->ListBannedUsers())
        .ValueOrThrow();
    EXPECT_TRUE(bannedList.empty());
}

TEST_F(TBanServiceTest, BanServicePermissionDenied)
{
    TCreateObjectOptions userOptions;
    auto attributes = CreateEphemeralAttributes();
    attributes->Set("name", "smol_user");
    userOptions.Attributes = std::move(attributes);
    WaitFor(Client_->CreateObject(EObjectType::User, userOptions))
        .ThrowOnError();

    auto client = Connection_->CreateClient(NNative::TClientOptions::FromUser("smol_user"));

    auto nativeClient = DynamicPointerCast<NApi::NNative::IClient>(client);
    auto isBanned = WaitFor(nativeClient->GetUserBanned("user"))
        .ValueOrThrow();
    EXPECT_FALSE(isBanned);
    auto bannedList = WaitFor(nativeClient->ListBannedUsers())
        .ValueOrThrow();
    EXPECT_TRUE(bannedList.empty());
    try {
        WaitFor(nativeClient->SetUserBanned("user", true))
            .ThrowOnError();
        ADD_FAILURE();
    } catch (const TErrorException& error) {
        EXPECT_TRUE(std::string_view(error.what()).contains("Superuser permissions required"));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCppTests
