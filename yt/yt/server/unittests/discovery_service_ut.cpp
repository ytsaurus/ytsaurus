#include <yt/core/test_framework/framework.h>

#include <yt/core/concurrency/action_queue.h>

#include <util/string/builder.h>

#include <yt/server/lib/discovery_server/config.h>
#include <yt/server/lib/discovery_server/discovery_server.h>

#include <yt/ytlib/discovery_client/public.h>
#include <yt/ytlib/discovery_client/helpers.h>
#include <yt/ytlib/discovery_client/discovery_client.h>
#include <yt/ytlib/discovery_client/member_client.h>

#include <yt/core/rpc/local_channel.h>
#include <yt/core/rpc/local_server.h>
#include <yt/core/rpc/server.h>
#include <yt/core/rpc/static_channel_factory.h>

#include <yt/core/yson/string.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NDiscoveryServer {
namespace {

using namespace NRpc;
using namespace NDiscoveryClient;
using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryServiceTestSuite
    : public ::testing::Test
{
public:
    virtual void SetUp() override
    {
        ChannelFactory_ = New<TStaticChannelFactory>();
        for (const auto& address : Addresses_) {
            RpcServers_.push_back(CreateLocalServer());
            ChannelFactory_->Add(address, CreateLocalChannel(RpcServers_.back()));
            RpcServers_.back()->Start();
        }

        auto serverConfig = New<TDiscoveryServerConfig>();
        serverConfig->ServerAddresses = Addresses_;
        serverConfig->AttributesUpdatePeriod = TDuration::Seconds(2);

        for (int i = 0; i < Addresses_.size(); ++i) {
            DiscoveryServers_.push_back(CreateDiscoveryServer(serverConfig, i));
            DiscoveryServers_.back()->Initialize();
        }
    }

    void KillDiscoveryServer(int index)
    {
        DiscoveryServers_[index]->Finalize();
    }

    void RecreateDiscoveryServer(int index)
    {
        auto serverConfig = New<TDiscoveryServerConfig>();
        serverConfig->ServerAddresses = Addresses_;

        DiscoveryServers_[index] = CreateDiscoveryServer(serverConfig, index);
        DiscoveryServers_[index]->Initialize();
    }

    IDiscoveryServerPtr CreateDiscoveryServer(const TDiscoveryServerConfigPtr& serverConfig, int index)
    {
        serverConfig->GossipPeriod = TDuration::MilliSeconds(500);
        serverConfig->AttributesUpdatePeriod = TDuration::Seconds(1);

        auto serverActionQueue = New<TActionQueue>("DiscoveryServer" + ToString(index));
        auto gossipActionQueue = New<TActionQueue>("Gossip" + ToString(index));

        auto server = NDiscoveryServer::CreateDiscoveryServer(
            RpcServers_[index],
            Addresses_[index],
            serverConfig,
            ChannelFactory_,
            serverActionQueue->GetInvoker(),
            gossipActionQueue->GetInvoker());

        ActionQueues_.push_back(serverActionQueue);
        ActionQueues_.push_back(gossipActionQueue);

        return server;
    }

    virtual void TearDown() override
    {
        for (int i = 0; i < Addresses_.size(); ++i) {
            KillDiscoveryServer(i);
            RpcServers_[i]->Stop();
        }
    }

    IDiscoveryClientPtr CreateDiscoveryClient(
        const TDiscoveryClientConfigPtr& discoveryClientConfig = New<TDiscoveryClientConfig>())
    {
        if (discoveryClientConfig->ServerAddresses.empty()) {
            discoveryClientConfig->ServerAddresses = Addresses_;
        }
        return NDiscoveryClient::CreateDiscoveryClient(discoveryClientConfig, ChannelFactory_);
    }

    IMemberClientPtr CreateMemberClient(
        const TString& groupId,
        const TString& memberId,
        const TMemberClientConfigPtr& memberClientConfig = New<TMemberClientConfig>())
    {
        if (memberClientConfig->ServerAddresses.empty()) {
            memberClientConfig->ServerAddresses = Addresses_;
        }
        memberClientConfig->HeartbeatPeriod = TDuration::MilliSeconds(500);
        memberClientConfig->LeaseTimeout = TDuration::Seconds(1);
        memberClientConfig->AttributeUpdatePeriod = TDuration::Seconds(1);

        const auto& actionQueue = New<TActionQueue>("MemberClient");
        ActionQueues_.push_back(actionQueue);
        return NDiscoveryClient::CreateMemberClient(
            memberClientConfig,
            ChannelFactory_,
            actionQueue->GetInvoker(),
            memberId,
            groupId);
    }

    const std::vector<TString>& GetDiscoveryServersAddresses()
    {
        return Addresses_;
    }

    IDiscoveryServerPtr GetDiscoveryServer()
    {
        return DiscoveryServers_[0];
    }

private:
    std::vector<TString> Addresses_ = {"peer1", "peer2", "peer3", "peer4", "peer5"};
    std::vector<IDiscoveryServerPtr> DiscoveryServers_;
    std::vector<IServerPtr> RpcServers_;

    std::vector<TActionQueuePtr> ActionQueues_;
    TStaticChannelFactoryPtr ChannelFactory_;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TDiscoveryServiceTestSuite, TestSimple)
{
    const TString groupId = "/sample_group";
    const TString memberId1 = "sample_member1";
    const TString memberId2 = "sample_member2";
    auto memberClient1 = CreateMemberClient(groupId, memberId1);
    WaitFor(memberClient1->Start())
        .ThrowOnError();

    auto memberClient2 = CreateMemberClient(groupId, memberId2);
    WaitFor(memberClient2->Start())
        .ThrowOnError();

    Sleep(TDuration::Seconds(5));

    auto discoveryClient = CreateDiscoveryClient();

    auto metaFuture = discoveryClient->GetGroupMeta(groupId);
    const auto& meta = metaFuture.Get().ValueOrThrow();
    EXPECT_EQ(2, meta.MemberCount);

    {
        auto membersFuture = discoveryClient->ListMembers(groupId, {});
        const auto& members = membersFuture.Get().ValueOrThrow();
        EXPECT_EQ(2, members.size());
        EXPECT_EQ(memberId1, members[0].Id);
        EXPECT_EQ(memberId2, members[1].Id);
    }

    memberClient1->Stop();
    Sleep(TDuration::Seconds(5));

    {
        auto membersFuture = discoveryClient->ListMembers(groupId, {});
        const auto& members = membersFuture.Get().ValueOrThrow();
        EXPECT_EQ(1, members.size());
        EXPECT_EQ(memberId2, members[0].Id);
    }
}

TEST_F(TDiscoveryServiceTestSuite, TestGossip)
{
    const TString groupId = "/sample_group";
    const TString memberId = "sample_member";
    const auto& addresses = GetDiscoveryServersAddresses();

    auto memberClientConfig = New<TMemberClientConfig>();
    memberClientConfig->ServerAddresses = {addresses[0], addresses[1], addresses[2]};
    auto memberClient = CreateMemberClient(groupId, memberId, memberClientConfig);
    WaitFor(memberClient->Start())
        .ThrowOnError();

    Sleep(TDuration::Seconds(5));

    auto discoveryClientConfig = New<TDiscoveryClientConfig>();
    discoveryClientConfig->ServerAddresses = {addresses[3], addresses[4]};
    auto discoveryClient = CreateDiscoveryClient(discoveryClientConfig);

    {
        auto membersFuture = discoveryClient->ListMembers(groupId, {});
        const auto& members = membersFuture.Get().ValueOrThrow();
        EXPECT_EQ(1, members.size());
        EXPECT_EQ(memberId, members[0].Id);
    }
}

TEST_F(TDiscoveryServiceTestSuite, TestAttributes)
{
    const TString groupId = "/sample_group";
    const TString memberId = "sample_member";

    const TString key = "key";
    const TString value = "value";

    const auto& addresses = GetDiscoveryServersAddresses();

    auto memberClientConfig = New<TMemberClientConfig>();
    memberClientConfig->ServerAddresses = {addresses[0], addresses[1], addresses[2]};
    auto memberClient = CreateMemberClient(groupId, memberId, memberClientConfig);
    WaitFor(memberClient->Start())
        .ThrowOnError();

    Sleep(TDuration::Seconds(5));

    auto discoveryClientConfig = New<TDiscoveryClientConfig>();
    discoveryClientConfig->ServerAddresses = {addresses[3], addresses[4]};

    auto discoveryClient = CreateDiscoveryClient(discoveryClientConfig);

    TListMembersOptions options;
    options.AttributeKeys.push_back(key);
    {
        auto membersFuture = discoveryClient->ListMembers(groupId, options);
        const auto& members = membersFuture.Get().ValueOrThrow();
        EXPECT_EQ(1, members.size());
        EXPECT_EQ(memberId, members[0].Id);
        EXPECT_TRUE(members[0].Attributes->ListKeys().empty());
    }

    auto* attributes = memberClient->GetAttributes();
    attributes->Set(key, value);

    Sleep(TDuration::Seconds(5));

    {
        auto membersFuture = discoveryClient->ListMembers(groupId, options);
        const auto& members = membersFuture.Get().ValueOrThrow();
        EXPECT_EQ(1, members.size());
        EXPECT_EQ(value, members[0].Attributes->Find<TString>(key));
    }
}

TEST_F(TDiscoveryServiceTestSuite, TestPriority)
{
    const TString groupId = "/sample_group";
    const TString memberId = "sample_member";

    std::vector<IMemberClientPtr> memberClients;
    std::vector<TFuture<void>> memberStartFutures;
    int membersNum = 10;
    for (int i = 0; i < membersNum; ++i) {
        memberClients.push_back(CreateMemberClient(groupId, memberId + ToString(i)));
        memberClients.back()->SetPriority(i);

        memberStartFutures.push_back(memberClients.back()->Start());
    }

    WaitFor(AllSucceeded(memberStartFutures))
        .ThrowOnError();
    Sleep(TDuration::Seconds(2));

    auto discoveryClient = CreateDiscoveryClient();

    {
        auto membersFuture = discoveryClient->ListMembers(groupId, {});
        const auto& members = membersFuture.Get().ValueOrThrow();
        EXPECT_EQ(membersNum, members.size());
        for (int i = 0; i < membersNum; ++i) {
            EXPECT_EQ(i, members[i].Priority);
        }
    }

    TListMembersOptions options;
    options.Limit = 3;

    {
        auto membersFuture = discoveryClient->ListMembers(groupId, options);
        const auto& members = membersFuture.Get().ValueOrThrow();
        EXPECT_EQ(options.Limit, members.size());
        for (int i = 0; i < options.Limit; ++i) {
            EXPECT_EQ(i, members[i].Priority);
        }
    }
}

TEST_F(TDiscoveryServiceTestSuite, TestServerBan)
{
    const TString groupId = "/sample_group";
    const TString memberId = "sample_member";
    const auto& addresses = GetDiscoveryServersAddresses();

    auto memberClientConfig = New<TMemberClientConfig>();
    memberClientConfig->ServerBanTimeout = TDuration::Seconds(3);
    memberClientConfig->ServerAddresses = {addresses[0], addresses[1], addresses[2]};
    memberClientConfig->HeartbeatPeriod = TDuration::Seconds(1);
    auto memberClient = CreateMemberClient(groupId, memberId, memberClientConfig);
    WaitFor(memberClient->Start())
        .ThrowOnError();

    auto discoveryClientConfig = New<TDiscoveryClientConfig>();
    discoveryClientConfig->ServerAddresses = {addresses[3], addresses[4]};
    auto discoveryClient = CreateDiscoveryClient(discoveryClientConfig);

    KillDiscoveryServer(0);
    Sleep(TDuration::Seconds(2));

    KillDiscoveryServer(1);
    RecreateDiscoveryServer(0);

    Sleep(TDuration::Seconds(5));

    {
        auto membersFuture = discoveryClient->ListMembers(groupId, {});
        const auto& members = membersFuture.Get().ValueOrThrow();
        EXPECT_EQ(1, members.size());
        EXPECT_EQ(memberId, members[0].Id);
    }
}

TEST_F(TDiscoveryServiceTestSuite, TestWrongParameters)
{
    auto memberClient = CreateMemberClient("incorrect_group_id", "sample_member");
    EXPECT_THROW(WaitFor(memberClient->Start()).ThrowOnError(), std::exception);

    memberClient = CreateMemberClient("/incorrect_group/", "sample_member");
    EXPECT_THROW(WaitFor(memberClient->Start()).ThrowOnError(), std::exception);

    memberClient = CreateMemberClient("/incorrect@group", "sample_member");
    EXPECT_THROW(WaitFor(memberClient->Start()).ThrowOnError(), std::exception);

    memberClient = CreateMemberClient("/", "sample_member");
    EXPECT_THROW(WaitFor(memberClient->Start()).ThrowOnError(), std::exception);

    memberClient = CreateMemberClient("/sample_group", "");
    EXPECT_THROW(WaitFor(memberClient->Start()).ThrowOnError(), std::exception);
}

TEST_F(TDiscoveryServiceTestSuite, TestNestedGroups)
{
    const std::vector<std::pair<TString, TString>> testMembers = {
        {"/sample_group", "sample_member_1"},
        {"/sample_group/subgroup", "sample_member_2"},
        {"/sample_group/subgroup/subgroup", "sample_member_3"},
    };

    std::vector<IMemberClientPtr> memberClients;
    std::vector<TFuture<void>> memberStartFutures;

    for (const auto& [groupId, memberId] : testMembers) {
        auto memberClient = CreateMemberClient(groupId, memberId);
        memberClients.push_back(memberClient);
        memberStartFutures.push_back(memberClient->Start());
    }

    WaitFor(AllSucceeded(memberStartFutures))
        .ThrowOnError();
    Sleep(TDuration::Seconds(1.5));

    auto discoveryClient = CreateDiscoveryClient();
    
    for (const auto& [groupId, memberId] : testMembers) {
        auto membersFuture = discoveryClient->ListMembers(groupId, {});
        const auto& members = membersFuture.Get().ValueOrThrow();
        EXPECT_EQ(1, members.size());
        EXPECT_EQ(memberId, members[0].Id);
    }

    // Deleting the middle group.
    WaitFor(memberClients[1]->Stop())
        .ThrowOnError();
    Sleep(TDuration::Seconds(1.5));

    for (int index = 0; index < testMembers.size(); ++index) {
        const auto& [groupId, memberId] = testMembers[index];
        auto groupMetaFuture = discoveryClient->GetGroupMeta(groupId);
        auto membersFuture = discoveryClient->ListMembers(groupId, {});
        if (index == 1) {
            EXPECT_THROW_WITH_SUBSTRING(groupMetaFuture.Get().ThrowOnError(), "does not exist");    
            EXPECT_THROW_WITH_SUBSTRING(membersFuture.Get().ThrowOnError(), "does not exist");    
        } else {
            auto groupMeta = groupMetaFuture.Get().ValueOrThrow();
            EXPECT_EQ(1, groupMeta.MemberCount);

            auto members = membersFuture.Get().ValueOrThrow();
            EXPECT_EQ(1, members.size());
            EXPECT_EQ(memberId, members[0].Id);
        }
    }
}

TEST_F(TDiscoveryServiceTestSuite, TestYPath)
{
    const TString groupId1 = "/sample_group1";
    const TString groupId2 = "/test/sample_group2";

    const TString memberId1 = "sample_member1";
    const TString memberId2 = "sample_member2";

    auto memberClient1 = CreateMemberClient(groupId1, memberId1);
    memberClient1->SetPriority(3);
    WaitFor(memberClient1->Start())
        .ThrowOnError();

    auto memberClient2 = CreateMemberClient(groupId2, memberId2);
    WaitFor(memberClient2->Start())
        .ThrowOnError();

    Sleep(TDuration::Seconds(5));

    auto ypathService = GetDiscoveryServer()->GetYPathService();

    EXPECT_EQ(true, SyncYPathExists(ypathService, "/sample_group1"));
    EXPECT_EQ(true, SyncYPathExists(ypathService, "/sample_group1/@"));
    EXPECT_EQ(true, SyncYPathExists(ypathService, "/test/sample_group2"));
    EXPECT_EQ(true, SyncYPathExists(ypathService, "/sample_group1/@members"));
    EXPECT_EQ(true, SyncYPathExists(ypathService, "/sample_group1/@members/sample_member1"));
    EXPECT_EQ(true, SyncYPathExists(ypathService, "/test/sample_group2/@members/sample_member2"));

    EXPECT_EQ(false, SyncYPathExists(ypathService, "/sample_group2"));
    EXPECT_EQ(false, SyncYPathExists(ypathService, "/test/sample_group1"));

    EXPECT_EQ(true, SyncYPathExists(ypathService, "/sample_group1/@members/sample_member1/@"));
    EXPECT_EQ(true, SyncYPathExists(ypathService, "/sample_group1/@members/sample_member1/@priority"));
    EXPECT_EQ(false, SyncYPathExists(ypathService, "/sample_group1/@members/sample_member1/@priority/aa"));

    {
        auto result = SyncYPathList(ypathService, "/");
        std::sort(result.begin(), result.end());
        EXPECT_EQ((std::vector<TString>{"sample_group1", "test"}), result);
    }

    {
        auto result = SyncYPathList(ypathService, "/@");
        std::sort(result.begin(), result.end());
        EXPECT_EQ((std::vector<TString>{"child_count", "type"}), result);
    }

    {
        auto result = SyncYPathList(ypathService, "/sample_group1/@");
        std::sort(result.begin(), result.end());
        EXPECT_EQ((std::vector<TString>{"child_count", "member_count", "members", "type"}), result);
    }

    EXPECT_EQ((std::vector<TString>{"sample_member1"}), SyncYPathList(ypathService, "/sample_group1/@members"));
    {
        auto result = SyncYPathList(ypathService, "/sample_group1/@members/sample_member1/@");
        std::vector<TString> expected{"priority", "revision", "last_heartbeat_time", "last_attributes_update_time"};
        EXPECT_EQ(expected, result);
    }

    EXPECT_THROW(SyncYPathList(ypathService, "/sample_group1/ttt"), std::exception);
    EXPECT_THROW(SyncYPathList(ypathService, "/sample_group1/@members/ttt"), std::exception);
    EXPECT_THROW(SyncYPathList(ypathService, "/sample_group1/@members/sample_member1/@priority"), std::exception);

    EXPECT_THROW(SyncYPathGet(ypathService, "/sample_group1/@members/sample_member1/@priority/qq"), std::exception);

    EXPECT_EQ(true, SyncYPathExists(ypathService, "/sample_group1/@members/sample_member1/@priority"));
    EXPECT_EQ(true, SyncYPathExists(ypathService, "/sample_group1/@members/sample_member1/@revision"));
    EXPECT_EQ(true, SyncYPathExists(ypathService, "/sample_group1/@members/sample_member1/@last_heartbeat_time"));
    EXPECT_EQ(true, SyncYPathExists(ypathService, "/sample_group1/@members/sample_member1/@last_attributes_update_time"));

    EXPECT_EQ(ConvertToYsonString(3, EYsonFormat::Binary),
        SyncYPathGet(ypathService, "/sample_group1/@members/sample_member1/@priority"));

    EXPECT_EQ(false, SyncYPathExists(ypathService, "/sample_group1/@members/sample_member1/@test"));

    auto* attributes = memberClient1->GetAttributes();
    attributes->Set("test", 123);

    Sleep(TDuration::Seconds(5));

    EXPECT_EQ(true, SyncYPathExists(ypathService, "/sample_group1/@members/sample_member1/@test"));
    EXPECT_EQ(ConvertToYsonString(123, EYsonFormat::Binary),
        SyncYPathGet(ypathService, "/sample_group1/@members/sample_member1/@test"));

    EXPECT_EQ(false, SyncYPathExists(ypathService, "/sample_group1/@members/sample_member1/@test/abc"));
    EXPECT_EQ(false, SyncYPathExists(ypathService, "/sample_group1/@members/sample_member1/@qq/abc"));
    EXPECT_THROW(SyncYPathGet(ypathService, "/sample_group1/@members/sample_member1/@test/abc"), std::exception);
    EXPECT_THROW(SyncYPathGet(ypathService, "/sample_group1/@members/sample_member1/@qq/abc"), std::exception);

    attributes->Set("q1", TYsonString("{q=w}"));
    attributes->Set("q2", TYsonString("{q={w=e}}"));

    Sleep(TDuration::Seconds(5));

    EXPECT_EQ(true, SyncYPathExists(ypathService, "/sample_group1/@members/sample_member1/@q1/q"));
    EXPECT_EQ(true, SyncYPathExists(ypathService, "/sample_group1/@members/sample_member1/@q2/q"));
    EXPECT_EQ(true, SyncYPathExists(ypathService, "/sample_group1/@members/sample_member1/@q2/q/w"));

    EXPECT_EQ(ConvertToYsonString("e", EYsonFormat::Binary),
        SyncYPathGet(ypathService, "/sample_group1/@members/sample_member1/@q2/q/w"));
    
    EXPECT_EQ(std::vector<TString>{"q"},
        SyncYPathList(ypathService, "/sample_group1/@members/sample_member1/@q2"));
    EXPECT_EQ(std::vector<TString>{"w"},
        SyncYPathList(ypathService, "/sample_group1/@members/sample_member1/@q2/q"));

    EXPECT_EQ(true, SyncYPathExists(ypathService, "/sample_group1/@child_count"));
    EXPECT_EQ(true, SyncYPathExists(ypathService, "/sample_group1/@members"));
    EXPECT_EQ(true, SyncYPathExists(ypathService, "/sample_group1/@member_count"));
    EXPECT_EQ(true, SyncYPathExists(ypathService, "/sample_group1/@type"));
    EXPECT_EQ(ConvertToYsonString("group", EYsonFormat::Binary), SyncYPathGet(ypathService, "/sample_group1/@type"));

    EXPECT_TRUE(
        AreNodesEqual(
            ConvertToNode(
                SyncYPathGet(
                    ypathService,
                    "",
                    std::vector<TString> {
                        "child_count",
                        "type",
                        "member_count",
                        "wrong_attribute",
                    })),
            BuildYsonNodeFluently()
                .BeginAttributes()
                    .Item("child_count").Value(2)
                    .Item("type").Value("node")
                .EndAttributes()
                .BeginMap()
                    .Item("sample_group1")
                        .BeginAttributes()
                            .Item("child_count").Value(0)
                            .Item("type").Value("group")
                            .Item("member_count").Value(1)
                        .EndAttributes()
                        .BeginMap()
                        .EndMap()
                    .Item("test")
                        .BeginAttributes()
                            .Item("child_count").Value(1)
                            .Item("type").Value("node")
                        .EndAttributes()
                        .BeginMap()
                            .Item("sample_group2")
                                .BeginAttributes()
                                .Item("child_count").Value(0)
                                .Item("type").Value("group")
                                .Item("member_count").Value(1)
                            .EndAttributes()
                            .BeginMap()
                            .EndMap()
                        .EndMap()
                .EndMap()));

    EXPECT_TRUE(
        AreNodesEqual(
            ConvertToNode(
                SyncYPathGet(
                    ypathService,
                    "/sample_group1",
                    std::vector<TString> {
                        "child_count",
                        "type",
                        "member_count",
                        "members",
                        "wrong_attribute",
                        "priority",
                    })),
            BuildYsonNodeFluently()
                .BeginAttributes()
                    .Item("child_count").Value(0)
                    .Item("type").Value("group")
                    .Item("member_count").Value(1)
                    .Item("members")
                        .BeginMap()
                            .Item("sample_member1")
                                .BeginAttributes()
                                    .Item("priority").Value(3)
                                .EndAttributes()
                                .Entity()
                        .EndMap()
                .EndAttributes()
                .BeginMap()
                .EndMap()));

    EXPECT_TRUE(
        AreNodesEqual(
            ConvertToNode(
                SyncYPathGet(
                    ypathService,
                    "/sample_group1/@members",
                    std::vector<TString> {
                        "priority",
                        "test",
                        "q1",
                        "wrong_attribute",
                    })),
            BuildYsonNodeFluently()
                .BeginMap()
                    .Item("sample_member1")
                        .BeginAttributes()
                            .Item("priority").Value(3)
                            .Item("test").Value(123)
                            .Item("q1")
                                .BeginMap()
                                    .Item("q").Value("w")
                                .EndMap()
                        .EndAttributes()
                        .Entity()
                .EndMap()));

    {
        auto sampleMemberNode = ConvertToNode(SyncYPathGet(
            ypathService,
            "/sample_group1/@members/sample_member1",
            std::nullopt));

        EXPECT_EQ(sampleMemberNode->GetType(), ENodeType::Entity);

        auto attributeKeys = sampleMemberNode->Attributes().ListKeys();
        std::sort(attributeKeys.begin(), attributeKeys.end());

        std::vector<TString> correctAttributeKeys = {
            "priority",
            "revision",
            "last_heartbeat_time",
            "last_attributes_update_time",
            "test",
            "q1",
            "q2",
        };
        std::sort(correctAttributeKeys.begin(), correctAttributeKeys.end());

        EXPECT_EQ(attributeKeys, correctAttributeKeys);
    }
}

TEST_F(TDiscoveryServiceTestSuite, TestGroupRemoval)
{
    const TString groupId1 = "/sample_group1";
    const TString memberId1 = "sample_member1";

    const TString groupId2 = "/sample_group2";
    const TString memberId2 = "sample_member2";

    auto memberClient1 = CreateMemberClient(groupId1, memberId1);
    WaitFor(memberClient1->Start())
        .ThrowOnError();

    auto memberClient2 = CreateMemberClient(groupId2, memberId2);
    WaitFor(memberClient2->Start())
        .ThrowOnError();

    Sleep(TDuration::Seconds(5));

    auto ypathService = GetDiscoveryServer()->GetYPathService();

    {
        auto result = SyncYPathList(ypathService, "/");
        std::sort(result.begin(), result.end());
        EXPECT_EQ((std::vector<TString>{"sample_group1", "sample_group2"}), result);
    }

    memberClient1->Stop();
    Sleep(TDuration::Seconds(5));

    {
        auto result = SyncYPathList(ypathService, "/");
        std::sort(result.begin(), result.end());
        EXPECT_EQ((std::vector<TString>{"sample_group2"}), result);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NDiscoveryServer
