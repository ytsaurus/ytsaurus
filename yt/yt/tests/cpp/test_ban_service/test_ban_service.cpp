#include <yt/yt/tests/cpp/test_base/api_test_base.h>
#include <yt/yt/tests/cpp/test_base/private.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/server/lib/cross_cluster_replicated_state/cross_cluster_client_detail.h>
#include <yt/yt/server/lib/cross_cluster_replicated_state/config.h>

#include <yt/yt/ytlib/ban_client/ban_service_proxy.h>

#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>


////////////////////////////////////////////////////////////////////////////////

namespace NYT::NCppTests {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NYTree;
using namespace NYson;

using namespace std::literals;

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

TEST_F(TBanServiceTest, TestBanService)
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

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCppTests
