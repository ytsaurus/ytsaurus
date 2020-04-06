#include <yt/core/test_framework/framework.h>

#include <yt/core/concurrency/action_queue.h>

#include <util/string/builder.h>

#include <yt/server/lib/discovery_server/config.h>
#include <yt/server/lib/discovery_server/discovery_service.h>

#include <yt/ytlib/discovery_client/public.h>
#include <yt/ytlib/discovery_client/helpers.h>
#include <yt/ytlib/discovery_client/discovery_client.h>
#include <yt/ytlib/discovery_client/member_client.h>

#include <yt/core/rpc/local_channel.h>
#include <yt/core/rpc/local_server.h>
#include <yt/core/rpc/server.h>
#include <yt/core/rpc/static_channel_factory.h>

namespace NYT::NDiscoveryServer {
namespace {

using namespace NRpc;
using namespace NDiscoveryClient;
using namespace NConcurrency;

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

    TDiscoveryServerPtr CreateDiscoveryServer(const TDiscoveryServerConfigPtr& serverConfig, int index)
    {
        serverConfig->GossipPeriod = TDuration::MilliSeconds(500);
        serverConfig->AttributesUpdatePeriod = TDuration::Seconds(1);

        auto serverActionQueue = New<TActionQueue>("DiscoveryServer" + ToString(index));
        auto gossipActionQueue = New<TActionQueue>("Gossip" + ToString(index));

        auto server = New<TDiscoveryServer>(
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

    TDiscoveryClientPtr CreateDiscoveryClient(
        const TDiscoveryClientConfigPtr& discoveryClientConfig = New<TDiscoveryClientConfig>())
    {
        if (discoveryClientConfig->ServerAddresses.empty()) {
            discoveryClientConfig->ServerAddresses = Addresses_;
        }
        return New<TDiscoveryClient>(discoveryClientConfig, ChannelFactory_);
    }

    TMemberClientPtr CreateMemberClient(
        const TString& groupId,
        const TString& memberId,
        const TMemberClientConfigPtr& memberClientConfig = New<TMemberClientConfig>())
    {
        if (memberClientConfig->ServerAddresses.empty()) {
            memberClientConfig->ServerAddresses = Addresses_;
        }
        memberClientConfig->HeartbeatPeriod = TDuration::MilliSeconds(500);
        memberClientConfig->AttributeUpdatePeriod = TDuration::Seconds(1);
        const auto& actionQueue = New<TActionQueue>("MemberClient");
        ActionQueues_.push_back(actionQueue);
        return New<TMemberClient>(
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

private:
    std::vector<TString> Addresses_ = {"peer1", "peer2", "peer3", "peer4", "peer5"};
    std::vector<TDiscoveryServerPtr> DiscoveryServers_;
    std::vector<IServerPtr> RpcServers_;

    std::vector<TActionQueuePtr> ActionQueues_;
    TStaticChannelFactoryPtr ChannelFactory_;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TDiscoveryServiceTestSuite, TestSimple)
{
    TString groupId = "sample_group";
    TString memberId1 = "sample_member1";
    TString memberId2 = "sample_member2";
    auto memberClient1 = CreateMemberClient(groupId, memberId1);
    memberClient1->Start();

    auto memberClient2 = CreateMemberClient(groupId, memberId2);
    memberClient2->Start();

    Sleep(TDuration::Seconds(5));

    auto discoveryClient = CreateDiscoveryClient();

    {
        auto groupsFuture = discoveryClient->ListGroups();
        const auto& groups = groupsFuture.Get().ValueOrThrow();
        EXPECT_EQ(1, groups.size());
        EXPECT_EQ(groupId, groups[0]);
    }

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

    memberClient2->Stop();
    Sleep(TDuration::Seconds(5));

    {
        auto groupsFuture = discoveryClient->ListGroups();
        const auto& groups = groupsFuture.Get().ValueOrThrow();
        EXPECT_EQ(0, groups.size());
    }
}

TEST_F(TDiscoveryServiceTestSuite, TestGossip)
{
    TString groupId = "sample_group";
    TString memberId = "sample_member";
    const auto& addresses = GetDiscoveryServersAddresses();

    auto memberClientConfig = New<TMemberClientConfig>();
    memberClientConfig->ServerAddresses = {addresses[0], addresses[1], addresses[2]};
    auto memberClient = CreateMemberClient(groupId, memberId, memberClientConfig);
    memberClient->Start();

    Sleep(TDuration::Seconds(5));

    auto discoveryClientConfig = New<TDiscoveryClientConfig>();
    discoveryClientConfig->ServerAddresses = {addresses[3], addresses[4]};
    auto discoveryClient = CreateDiscoveryClient(discoveryClientConfig);

    {
        auto groupsFuture = discoveryClient->ListGroups();
        const auto& groups = groupsFuture.Get().ValueOrThrow();
        EXPECT_EQ(1, groups.size());
        EXPECT_EQ(groupId, groups[0]);
    }

    {
        auto membersFuture = discoveryClient->ListMembers(groupId, {});
        const auto& members = membersFuture.Get().ValueOrThrow();
        EXPECT_EQ(1, members.size());
        EXPECT_EQ(memberId, members[0].Id);
    }
}

TEST_F(TDiscoveryServiceTestSuite, TestAttributes)
{
    TString groupId = "sample_group";
    TString memberId = "sample_member";

    TString key = "key";
    TString value = "value";

    const auto& addresses = GetDiscoveryServersAddresses();

    auto memberClientConfig = New<TMemberClientConfig>();
    memberClientConfig->ServerAddresses = {addresses[0], addresses[1], addresses[2]};
    auto memberClient = CreateMemberClient(groupId, memberId, memberClientConfig);
    memberClient->Start();

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
    TString groupId = "sample_group";
    TString memberId = "sample_member";

    std::vector<TMemberClientPtr> memberClients;
    int membersNum = 10;
    for (int i = 0; i < membersNum; ++i) {
        memberClients.push_back(CreateMemberClient(groupId, memberId + ToString(i)));
        memberClients.back()->Start();

        memberClients.back()->SetPriority(i);
    }

    Sleep(TDuration::Seconds(10));

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
    TString groupId = "sample_group";
    TString memberId = "sample_member";
    const auto& addresses = GetDiscoveryServersAddresses();

    auto memberClientConfig = New<TMemberClientConfig>();
    memberClientConfig->ServerBanTimeout = TDuration::Seconds(3);
    memberClientConfig->ServerAddresses = {addresses[0], addresses[1], addresses[2]};
    memberClientConfig->HeartbeatPeriod = TDuration::Seconds(1);
    auto memberClient = CreateMemberClient(groupId, memberId, memberClientConfig);
    memberClient->Start();

    auto discoveryClientConfig = New<TDiscoveryClientConfig>();
    discoveryClientConfig->ServerAddresses = {addresses[3], addresses[4]};
    auto discoveryClient = CreateDiscoveryClient(discoveryClientConfig);

    KillDiscoveryServer(0);
    Sleep(TDuration::Seconds(2));

    KillDiscoveryServer(1);
    RecreateDiscoveryServer(0);

    Sleep(TDuration::Seconds(5));
    {
        auto groupsFuture = discoveryClient->ListGroups();
        const auto& groups = groupsFuture.Get().ValueOrThrow();
        EXPECT_EQ(1, groups.size());
        EXPECT_EQ(groupId, groups[0]);
    }

    {
        auto membersFuture = discoveryClient->ListMembers(groupId, {});
        const auto& members = membersFuture.Get().ValueOrThrow();
        EXPECT_EQ(1, members.size());
        EXPECT_EQ(memberId, members[0].Id);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NDiscoveryServer
