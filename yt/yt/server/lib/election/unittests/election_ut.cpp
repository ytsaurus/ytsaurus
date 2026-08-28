#include <yt/yt/core/test_framework/framework.h>

#include "election_manager_mock.h"
#include "election_service_mock.h"

#include <yt/yt/server/lib/election/config.h>
#include <yt/yt/server/lib/election/election_manager.h>
#include <yt/yt/server/lib/election/distributed_election_manager.h>
#include <yt/yt/server/lib/election/public.h>

#include <yt/yt/ytlib/election/proto/election_service.pb.h>

#include <yt/yt/ytlib/election/cell_manager.h>
#include <yt/yt/ytlib/election/config.h>
#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/library/profiling/solomon/registry.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/rpc/channel.h>
#include <yt/yt/core/rpc/local_channel.h>
#include <yt/yt/core/rpc/local_server.h>
#include <yt/yt/core/rpc/server.h>
#include <yt/yt/core/rpc/static_channel_factory.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NElection {
namespace {

using namespace NConcurrency;
using namespace NRpc;

using NYT::ToProto;

using testing::Return;
using testing::InSequence;
using testing::Invoke;
using testing::_;

////////////////////////////////////////////////////////////////////////////////

class TElectionTest
    : public testing::Test
{
public:
    void Configure(
        int peerCount,
        int selfId,
        std::optional<int> quorumPeerCount = std::nullopt,
        const std::vector<int>& weights = {})
    {
        // NB: During func gauge registration callback holding strong
        // reference to RPC server is put into the queue that is not
        // drained in tests since metrics are not fetched.
        // To prevent memory leak we simple disable profiling.
        NProfiling::TSolomonRegistry::Get()->Disable();

        auto selfServer = CreateLocalServer();

        PeerMocks.resize(peerCount);
        for (int id = 0; id < peerCount; ++id) {
            if (id != selfId) {
                auto server = CreateLocalServer();
                auto channel = CreateLocalChannel(server);
                ChannelFactory->Add(GetPeerAddress(id), channel);
                auto mock = New<TElectionServiceMock>(ActionQueue->GetInvoker());
                PeerMocks[id] = mock;
                server->RegisterService(mock);
            } else {
                auto channel = CreateLocalChannel(selfServer);
                ChannelFactory->Add(GetPeerAddress(id), channel);
            }
        }

        auto cellConfig = New<TCellConfig>();
        for (int id = 0; id < peerCount; ++id) {
            auto peerConfig = New<TCellPeerConfig>();
            peerConfig->Address = GetPeerAddress(id);
            if (id < std::ssize(weights)) {
                peerConfig->Weight = weights[id];
            }
            cellConfig->Peers.push_back(peerConfig);
        }
        cellConfig->QuorumPeerCount = quorumPeerCount;

        auto cellManager = New<TCellManager>(cellConfig, ChannelFactory, nullptr, selfId);

        auto electionConfig = New<TDistributedElectionManagerConfig>();
        electionConfig->ControlRpcTimeout = RpcTimeout;
        electionConfig->VotingRoundPeriod = TDuration::MilliSeconds(100);
        electionConfig->FollowerPingRpcTimeout = TDuration::MilliSeconds(600);
        electionConfig->FollowerGraceTimeout = TDuration::MilliSeconds(300);
        electionConfig->FollowerPingPeriod = TDuration::MilliSeconds(500);

        ElectionManager = CreateDistributedElectionManager(
            electionConfig,
            cellManager,
            ActionQueue->GetInvoker(),
            CallbacksMock,
            selfServer,
            /*authenticator*/ nullptr);

        WaitFor(BIND(&IElectionManager::Initialize, ElectionManager)
            .AsyncVia(ActionQueue->GetInvoker())
            .Run())
            .ThrowOnError();

        EXPECT_CALL(*CallbacksMock, FormatPriority(_))
            .WillRepeatedly([] (TPeerPriority priority) {
                return Format("{First: %v, Second: %v}",
                    priority.first,
                    priority.second);
            });
    }

    void Sleep()
    {
        TDelayedExecutor::WaitForDuration(TDuration::Seconds(2));
    }

    void RunElections()
    {
        WaitFor(BIND([this] {
            ElectionManager->Participate();
            Sleep();
            YT_UNUSED_FUTURE(ElectionManager->Abandon(TError("oops")));
            Sleep();
        })
            .AsyncVia(ActionQueue->GetInvoker())
            .Run())
            .ThrowOnError();
    }

protected:
    const TActionQueuePtr ActionQueue = New<TActionQueue>("Control");
    const TIntrusivePtr<TElectionCallbacksMock> CallbacksMock = New<TElectionCallbacksMock>();
    const TStaticChannelFactoryPtr ChannelFactory = New<TStaticChannelFactory>();
    const TDuration RpcTimeout = TDuration::MilliSeconds(400);

    IElectionManagerPtr ElectionManager;
    std::vector<TIntrusivePtr<TElectionServiceMock>> PeerMocks;

    static std::string GetPeerAddress(int id)
    {
        return "peer" + ToString(id);
    }

private:
    void TearDown() override
    {
        Sleep();

        testing::Mock::VerifyAndClearExpectations(CallbacksMock.Get());

        for (auto mock : PeerMocks) {
            if (mock) {
                testing::Mock::VerifyAndClearExpectations(mock.Get());
            }
        }

        WaitFor(BIND(&IElectionManager::Finalize, ElectionManager)
            .AsyncVia(ActionQueue->GetInvoker())
            .Run())
            .ThrowOnError();
        ElectionManager.Reset();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TElectionTest, SinglePeer)
{
    Configure(1, 0);

    EXPECT_CALL(*CallbacksMock, GetPriority())
        .WillRepeatedly(Return(std::pair(0LL, 0LL)));

    {
        InSequence dummy;
        EXPECT_CALL(*CallbacksMock, OnStartLeading(_));
        EXPECT_CALL(*CallbacksMock, OnStopLeading(_));
    }

    RunElections();
}

TEST_F(TElectionTest, JoinActiveQuorumNoResponseThenResponse)
{
    Configure(3, 0);

    EXPECT_CALL(*CallbacksMock, GetPriority())
        .WillRepeatedly(Return(std::pair(0LL, 0LL)));

    for (int id = 1; id < 3; ++id) {
        EXPECT_RPC_CALL(*PeerMocks[id], GetStatus)
            .WillOnce(HANDLE_RPC_CALL(TElectionServiceMock, GetStatus, [=], { }))
            .WillRepeatedly(HANDLE_RPC_CALL(TElectionServiceMock, GetStatus, [=], {
                response->set_state(ToProto(id == 2 ? EPeerState::Leading : EPeerState::Following));
                response->set_vote_id(2);
                ToProto(response->mutable_vote_epoch_id(), TEpochId());
                ToProto(response->mutable_priority(), std::pair<i64, i64>(0LL, id));
                response->set_self_id(id);
                context->Reply();
            }));
    }

    {
        InSequence dummy;
        EXPECT_CALL(*CallbacksMock, OnStartFollowing(_));
        EXPECT_CALL(*CallbacksMock, OnStopFollowing(_));
    }

    RunElections();
}

TEST_F(TElectionTest, BecomeLeaderOneHealthyFollower)
{
    Configure(3, 0);

    EXPECT_CALL(*CallbacksMock, GetPriority())
        .WillRepeatedly(Return(std::pair(0LL, 0LL)));

    for (int id = 1; id < 3; ++id) {
        EXPECT_RPC_CALL(*PeerMocks[id], GetStatus)
            .WillRepeatedly(HANDLE_RPC_CALL(TElectionServiceMock, GetStatus, ([=, this]), {
                auto channel = ChannelFactory->CreateChannel(GetPeerAddress(0));
                TElectionServiceProxy proxy(channel);
                proxy.SetDefaultTimeout(RpcTimeout);

                auto rspOrError = WaitFor(proxy.GetStatus()->Invoke());
                EXPECT_TRUE(rspOrError.IsOK()) << ToString(rspOrError);
                const auto& rsp = rspOrError.Value();

                response->set_state(ToProto(EPeerState::Following));
                response->set_vote_id(0);
                ToProto(response->mutable_vote_epoch_id(), rsp->vote_epoch_id());
                ToProto(response->mutable_priority(), std::pair<i64, i64>(0, id));
                response->set_self_id(id);
                context->Reply();
            }));
        if (id == 1) {
            EXPECT_RPC_CALL(*PeerMocks[id], PingFollower)
                .WillRepeatedly(HANDLE_RPC_CALL(TElectionServiceMock, PingFollower, [=], {
                    context->Reply();
                }));
        } else {
            EXPECT_RPC_CALL(*PeerMocks[id], PingFollower)
                .WillRepeatedly(HANDLE_RPC_CALL(TElectionServiceMock, PingFollower, [], {
                    // Do not reply.
                }));
        }
    }

    {
        InSequence dummy;
        EXPECT_CALL(*CallbacksMock, OnStartLeading(_));
        EXPECT_CALL(*CallbacksMock, OnStopLeading(_));
    }

    RunElections();
}

TEST_F(TElectionTest, BecomeLeaderTwoHealthyFollowers)
{
    Configure(3, 0);

    EXPECT_CALL(*CallbacksMock, GetPriority())
        .WillRepeatedly(Return(std::pair(0LL, 0LL)));

    for (int id = 1; id < 3; ++id) {
        EXPECT_RPC_CALL(*PeerMocks[id], GetStatus)
            .WillRepeatedly(HANDLE_RPC_CALL(TElectionServiceMock, GetStatus, ([=, this]), {
                auto channel = ChannelFactory->CreateChannel(GetPeerAddress(0));
                TElectionServiceProxy proxy(channel);
                proxy.SetDefaultTimeout(RpcTimeout);

                auto rspOrError = WaitFor(proxy.GetStatus()->Invoke());
                EXPECT_TRUE(rspOrError.IsOK()) << ToString(rspOrError);
                const auto& rsp = rspOrError.Value();

                response->set_state(ToProto(EPeerState::Following));
                response->set_vote_id(0);
                ToProto(response->mutable_vote_epoch_id(), rsp->vote_epoch_id());
                ToProto(response->mutable_priority(), std::pair<i64, i64>(0, id));
                response->set_self_id(id);
                context->Reply();
            }));
        EXPECT_RPC_CALL(*PeerMocks[id], PingFollower)
            .WillRepeatedly(HANDLE_RPC_CALL(TElectionServiceMock, PingFollower, [=], {
                context->Reply();
            }));
    }

    {
        InSequence dummy;
        EXPECT_CALL(*CallbacksMock, OnStartLeading(_));
        EXPECT_CALL(*CallbacksMock, OnStopLeading(_));
    }

    RunElections();
}

TEST_F(TElectionTest, BecomeLeaderQuorumLostOnce)
{
    Configure(3, 0);

    EXPECT_CALL(*CallbacksMock, GetPriority())
        .WillRepeatedly(Return(std::pair(0LL, 0LL)));

    int startLeadingCounter = 0;
    for (int id = 1; id < 3; ++id) {
        EXPECT_RPC_CALL(*PeerMocks[id], GetStatus)
            .WillRepeatedly(HANDLE_RPC_CALL(TElectionServiceMock, GetStatus, ([=, this]), {
                auto channel = ChannelFactory->CreateChannel(GetPeerAddress(0));
                TElectionServiceProxy proxy(channel);
                proxy.SetDefaultTimeout(RpcTimeout);

                auto rspOrError = WaitFor(proxy.GetStatus()->Invoke());
                EXPECT_TRUE(rspOrError.IsOK()) << ToString(rspOrError);
                const auto& rsp = rspOrError.Value();

                response->set_state(ToProto(EPeerState::Following));
                response->set_vote_id(0);
                ToProto(response->mutable_vote_epoch_id(), rsp->vote_epoch_id());
                ToProto(response->mutable_priority(), std::pair<i64, i64>(0, id));
                response->set_self_id(id);
                context->Reply();
            }));
        EXPECT_RPC_CALL(*PeerMocks[id], PingFollower)
            .WillRepeatedly(HANDLE_RPC_CALL(TElectionServiceMock, PingFollower, [=], {
                if (startLeadingCounter > 1) {
                    context->Reply();
                }
            }));
    }

    {
        InSequence dummy;
        EXPECT_CALL(*CallbacksMock, OnStartLeading(_))
            .WillOnce([&startLeadingCounter] (TEpochContextPtr /*epochContext*/) {
                ++startLeadingCounter;
            });
        EXPECT_CALL(*CallbacksMock, OnStopLeading(_))
            .WillOnce([this] (const TError&) {
                ElectionManager->Participate();
            });
        EXPECT_CALL(*CallbacksMock, OnStartLeading(_))
            .WillOnce([&startLeadingCounter] (TEpochContextPtr /*epochContext*/) {
                ++startLeadingCounter;
            });
        EXPECT_CALL(*CallbacksMock, OnStopLeading(_));
    }

    RunElections();
}

TEST_F(TElectionTest, BecomeLeaderGracePeriod)
{
    Configure(3, 0);

    EXPECT_CALL(*CallbacksMock, GetPriority())
        .WillRepeatedly(Return(std::pair<i64, i64>(0, 0)));

    for (int id = 1; id < 3; ++id) {
        EXPECT_RPC_CALL(*PeerMocks[id], GetStatus)
            .WillRepeatedly(HANDLE_RPC_CALL(TElectionServiceMock, GetStatus, ([=, this]), {
                auto channel = ChannelFactory->CreateChannel(GetPeerAddress(0));
                TElectionServiceProxy proxy(channel);
                proxy.SetDefaultTimeout(RpcTimeout);

                auto rspOrError = WaitFor(proxy.GetStatus()->Invoke());
                EXPECT_TRUE(rspOrError.IsOK()) << ToString(rspOrError);
                const auto& rsp = rspOrError.Value();

                response->set_state(ToProto(EPeerState::Following));
                response->set_vote_id(0);
                ToProto(response->mutable_vote_epoch_id(), rsp->vote_epoch_id());
                ToProto(response->mutable_priority(), std::pair<i64, i64>(0, id));
                response->set_self_id(id);
                context->Reply();
            }));
        EXPECT_RPC_CALL(*PeerMocks[id], PingFollower)
            .WillRepeatedly(HANDLE_RPC_CALL(TElectionServiceMock, PingFollower, [], {
                THROW_ERROR_EXCEPTION(NElection::EErrorCode::InvalidLeader, "Dummy error");
            }));
    }

    {
        InSequence dummy;
        EXPECT_CALL(*CallbacksMock, OnStartLeading(_));
        EXPECT_CALL(*CallbacksMock, OnStopLeading(_))
            .WillOnce([this] (const TError& /*error*/) {
                ElectionManager->Participate();
            });
        EXPECT_CALL(*CallbacksMock, OnStartLeading(_));
        EXPECT_CALL(*CallbacksMock, OnStopLeading(_));
    }

    RunElections();
}

TEST_F(TElectionTest, ConfigurableQuorum)
{
    Configure(6, 0, /*quorumPeerCount*/ 5);

    EXPECT_CALL(*CallbacksMock, GetPriority())
        .WillRepeatedly(Return(std::pair(0LL, 0LL)));

    for (int id = 1; id < 4; ++id) {
        EXPECT_RPC_CALL(*PeerMocks[id], GetStatus)
            .WillRepeatedly(HANDLE_RPC_CALL(TElectionServiceMock, GetStatus, ([=, this]), {
                auto channel = ChannelFactory->CreateChannel(GetPeerAddress(0));
                TElectionServiceProxy proxy(channel);
                proxy.SetDefaultTimeout(RpcTimeout);

                auto rspOrError = WaitFor(proxy.GetStatus()->Invoke());
                EXPECT_TRUE(rspOrError.IsOK()) << ToString(rspOrError);
                const auto& rsp = rspOrError.Value();

                response->set_state(ToProto(EPeerState::Following));
                response->set_vote_id(0);
                ToProto(response->mutable_vote_epoch_id(), rsp->vote_epoch_id());
                ToProto(response->mutable_priority(), std::pair<i64, i64>(0, id));
                response->set_self_id(id);
                context->Reply();
            }));
    }

    // Peers 4-5 never respond, simulating dead peers.
    for (int id = 4; id < 6; ++id) {
        EXPECT_RPC_CALL(*PeerMocks[id], GetStatus)
            .WillRepeatedly(HANDLE_RPC_CALL(TElectionServiceMock, GetStatus, [], {
                // Do not reply.
            }));
    }

    EXPECT_CALL(*CallbacksMock, OnStartLeading(_))
        .Times(0);
    EXPECT_CALL(*CallbacksMock, OnStopLeading(_))
        .Times(0);
    EXPECT_CALL(*CallbacksMock, OnStartFollowing(_))
        .Times(0);
    EXPECT_CALL(*CallbacksMock, OnStopFollowing(_))
        .Times(0);

    RunElections();
}

TEST_F(TElectionTest, WeightedQuorumReachedWithFewAlivePeers)
{
    // Weights: self = 1, peers 1-3 = 1, peer 4 = 10; total weight 14, default quorum peer count is
    // 3 (out of 5 voting peers), so quorum weight = ceil(14 * 3 / 5) = 9.
    Configure(5, 0, /*quorumPeerCount*/ std::nullopt, /*weights*/ {1, 1, 1, 1, 10});

    EXPECT_CALL(*CallbacksMock, GetPriority())
        .WillRepeatedly(Return(std::pair(0LL, 0LL)));

    // Only the heavyweight peer 4 is alive; combined with self (1 + 10 = 11 >= 9), quorum is
    // reached even though only 2 out of 5 peers are alive.
    EXPECT_RPC_CALL(*PeerMocks[4], GetStatus)
        .WillRepeatedly(HANDLE_RPC_CALL(TElectionServiceMock, GetStatus, ([=, this]), {
            auto channel = ChannelFactory->CreateChannel(GetPeerAddress(0));
            TElectionServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(RpcTimeout);

            auto rspOrError = WaitFor(proxy.GetStatus()->Invoke());
            EXPECT_TRUE(rspOrError.IsOK()) << ToString(rspOrError);
            const auto& rsp = rspOrError.Value();

            response->set_state(ToProto(EPeerState::Following));
            response->set_vote_id(0);
            ToProto(response->mutable_vote_epoch_id(), rsp->vote_epoch_id());
            ToProto(response->mutable_priority(), std::pair<i64, i64>(0, 4));
            response->set_self_id(4);
            context->Reply();
        }));

    // Peers 1-3 never respond, simulating dead peers.
    for (int id = 1; id < 4; ++id) {
        EXPECT_RPC_CALL(*PeerMocks[id], GetStatus)
            .WillRepeatedly(HANDLE_RPC_CALL(TElectionServiceMock, GetStatus, [], {
                // Do not reply.
            }));
    }

    {
        InSequence dummy;
        EXPECT_CALL(*CallbacksMock, OnStartLeading(_));
        EXPECT_CALL(*CallbacksMock, OnStopLeading(_));
    }

    RunElections();
}

TEST_F(TElectionTest, WeightedQuorumNotReachedDespiteMajorityOfPeersAlive)
{
    // Same weights as above; quorum weight is 9.
    Configure(5, 0, /*quorumPeerCount*/ std::nullopt, /*weights*/ {1, 1, 1, 1, 10});

    EXPECT_CALL(*CallbacksMock, GetPriority())
        .WillRepeatedly(Return(std::pair(0LL, 0LL)));

    // Peers 1-3 (weight 1 each) are alive, but self + peers 1-3 only sum to 4 < 9, so quorum is
    // not reached even though 4 out of 5 peers are alive.
    for (int id = 1; id < 4; ++id) {
        EXPECT_RPC_CALL(*PeerMocks[id], GetStatus)
            .WillRepeatedly(HANDLE_RPC_CALL(TElectionServiceMock, GetStatus, ([=, this]), {
                auto channel = ChannelFactory->CreateChannel(GetPeerAddress(0));
                TElectionServiceProxy proxy(channel);
                proxy.SetDefaultTimeout(RpcTimeout);

                auto rspOrError = WaitFor(proxy.GetStatus()->Invoke());
                EXPECT_TRUE(rspOrError.IsOK()) << ToString(rspOrError);
                const auto& rsp = rspOrError.Value();

                response->set_state(ToProto(EPeerState::Following));
                response->set_vote_id(0);
                ToProto(response->mutable_vote_epoch_id(), rsp->vote_epoch_id());
                ToProto(response->mutable_priority(), std::pair<i64, i64>(0, id));
                response->set_self_id(id);
                context->Reply();
            }));
    }

    // Peer 4 (weight 10) never responds, simulating a dead peer.
    EXPECT_RPC_CALL(*PeerMocks[4], GetStatus)
        .WillRepeatedly(HANDLE_RPC_CALL(TElectionServiceMock, GetStatus, [], {
            // Do not reply.
        }));

    EXPECT_CALL(*CallbacksMock, OnStartLeading(_))
        .Times(0);
    EXPECT_CALL(*CallbacksMock, OnStopLeading(_))
        .Times(0);
    EXPECT_CALL(*CallbacksMock, OnStartFollowing(_))
        .Times(0);
    EXPECT_CALL(*CallbacksMock, OnStopFollowing(_))
        .Times(0);

    RunElections();
}

TEST_F(TElectionTest, WeightedQuorumScalesWithConfiguredQuorumPeerCount)
{
    // Same weights as above (total weight 14), but with an explicit quorum peer count override of
    // 4 out of 5 (stricter than the default majority of 3). Quorum weight is proportional to that
    // override: ceil(14 * 4 / 5) = 12, not just a plain majority of weight (which would be 8).
    Configure(5, 0, /*quorumPeerCount*/ 4, /*weights*/ {1, 1, 1, 1, 10});

    EXPECT_CALL(*CallbacksMock, GetPriority())
        .WillRepeatedly(Return(std::pair(0LL, 0LL)));

    // Only the heavyweight peer 4 is alive; combined with self (1 + 10 = 11 < 12), quorum is not
    // reached, even though the same weights reached quorum in WeightedQuorumReachedWithFewAlivePeers
    // above under the default (unconfigured) quorum peer count.
    EXPECT_RPC_CALL(*PeerMocks[4], GetStatus)
        .WillRepeatedly(HANDLE_RPC_CALL(TElectionServiceMock, GetStatus, ([=, this]), {
            auto channel = ChannelFactory->CreateChannel(GetPeerAddress(0));
            TElectionServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(RpcTimeout);

            auto rspOrError = WaitFor(proxy.GetStatus()->Invoke());
            EXPECT_TRUE(rspOrError.IsOK()) << ToString(rspOrError);
            const auto& rsp = rspOrError.Value();

            response->set_state(ToProto(EPeerState::Following));
            response->set_vote_id(0);
            ToProto(response->mutable_vote_epoch_id(), rsp->vote_epoch_id());
            ToProto(response->mutable_priority(), std::pair<i64, i64>(0, 4));
            response->set_self_id(4);
            context->Reply();
        }));

    // Peers 1-3 never respond, simulating dead peers.
    for (int id = 1; id < 4; ++id) {
        EXPECT_RPC_CALL(*PeerMocks[id], GetStatus)
            .WillRepeatedly(HANDLE_RPC_CALL(TElectionServiceMock, GetStatus, [], {
                // Do not reply.
            }));
    }

    EXPECT_CALL(*CallbacksMock, OnStartLeading(_))
        .Times(0);
    EXPECT_CALL(*CallbacksMock, OnStopLeading(_))
        .Times(0);
    EXPECT_CALL(*CallbacksMock, OnStartFollowing(_))
        .Times(0);
    EXPECT_CALL(*CallbacksMock, OnStopFollowing(_))
        .Times(0);

    RunElections();
}

////////////////////////////////////////////////////////////////////////////////

struct TStatus
{
    EPeerState State;
    int VoteId;
    TEpochId VoteEpochId;
    TPeerPriority Priority;

    TStatus(EPeerState state, int voteId, TEpochId voteEpochId, TPeerPriority priority)
        : State(state)
        , VoteId(voteId)
        , VoteEpochId(voteEpochId)
        , Priority(priority)
    { }
};

struct TElectionTestData
{
    std::optional<TStatus> Statuses[2];
    int ExpectedLeader;

    TElectionTestData(int expectedLeader, TStatus status)
        : ExpectedLeader(expectedLeader)
    {
        Statuses[0] = status;
    }

    TElectionTestData(int expectedLeader, TStatus status, TStatus otherStatus)
        : ExpectedLeader(expectedLeader)
    {
        Statuses[0] = status;
        Statuses[1] = otherStatus;
    }
};

class TElectionGenericTest
    : public TElectionTest
    , public ::testing::WithParamInterface<TElectionTestData>
{ };

TEST_P(TElectionGenericTest, Basic)
{
    Configure(3, 0);

    TElectionTestData data = GetParam();

    EXPECT_CALL(*CallbacksMock, GetPriority())
        .WillRepeatedly(Return(std::pair(0LL, 0LL)));

    for (int id = 1; id < 3; ++id) {
        EXPECT_RPC_CALL(*PeerMocks[id], GetStatus)
            .WillRepeatedly(HANDLE_RPC_CALL(TElectionServiceMock, GetStatus, [=], {
                const auto& status = data.Statuses[id - 1];
                if (status) {
                    response->set_state(ToProto(status->State));
                    response->set_vote_id(status->VoteId);
                    ToProto(response->mutable_vote_epoch_id(), status->VoteEpochId);
                    ToProto(response->mutable_priority(), status->Priority);
                    response->set_self_id(id);
                    context->Reply();
                }
            }));
    }

    if (data.ExpectedLeader >= 0) {
        InSequence dummy;
        EXPECT_CALL(*CallbacksMock, OnStartFollowing(_));
        EXPECT_CALL(*CallbacksMock, OnStopFollowing(_));
    } else {
        EXPECT_CALL(*CallbacksMock, OnStartFollowing(_))
            .Times(0);
        EXPECT_CALL(*CallbacksMock, OnStopFollowing(_))
            .Times(0);
    }
    EXPECT_CALL(*CallbacksMock, OnStartLeading(_))
        .Times(0);
    EXPECT_CALL(*CallbacksMock, OnStopLeading(_))
        .Times(0);

    RunElections();
}

static TEpochId OtherEpoch = TEpochId::Create();

INSTANTIATE_TEST_SUITE_P(
    ValueParametrized,
    TElectionGenericTest,
    ::testing::Values(
        TElectionTestData(
            -1,
            TStatus(EPeerState::Following, 0, TEpochId(), {0, 1}),
            TStatus(EPeerState::Following, 0, TEpochId(), {0, 2})),
        TElectionTestData(
            1,
            TStatus(EPeerState::Leading, 1, OtherEpoch, {0, 1})),
        TElectionTestData(
            -1,
            TStatus(EPeerState::Leading, 1, OtherEpoch, {-1, -1})),
        // all followers
        TElectionTestData(
            -1,
            TStatus(EPeerState::Following, 1, OtherEpoch, {0, 1}),
            TStatus(EPeerState::Following, 2, OtherEpoch, {0, 2})),
        // all leaders
        TElectionTestData(
            2,
            TStatus(EPeerState::Leading, 1, OtherEpoch, {0, 1}),
            TStatus(EPeerState::Leading, 2, OtherEpoch, {0, 2})),
        // potential leader should recognize itself as a leader
        TElectionTestData(
            -1,
            TStatus(EPeerState::Following, 2, OtherEpoch, {0, 1}),
            TStatus(EPeerState::Following, 2, OtherEpoch, {0, 2}))
));

////////////////////////////////////////////////////////////////////////////////

class TElectionDelayedTest
    : public TElectionTest
    , public ::testing::WithParamInterface<TDuration>
{ };

TEST_P(TElectionDelayedTest, JoinActiveQuorum)
{
    Configure(3, 0);

    auto delay = GetParam();

    EXPECT_CALL(*CallbacksMock, GetPriority())
        .WillRepeatedly(Return(std::pair(0LL, 0LL)));

    for (int id = 1; id < 3; ++id) {
        EXPECT_RPC_CALL(*PeerMocks[id], GetStatus)
            .WillRepeatedly(HANDLE_RPC_CALL(TElectionServiceMock, GetStatus, [=], {
                TDelayedExecutor::Submit(BIND([=] {
                    response->set_state(ToProto(id == 2 ? EPeerState::Leading : EPeerState::Following));
                    response->set_vote_id(2);
                    ToProto(response->mutable_vote_epoch_id(), TEpochId());
                    ToProto(response->mutable_priority(), std::pair<i64, i64>(0, id));
                    response->set_self_id(id);
                    context->Reply();
                }), delay);
            }));
    }

    if (delay < RpcTimeout) {
        InSequence dummy;
        EXPECT_CALL(*CallbacksMock, OnStartFollowing(_));
        EXPECT_CALL(*CallbacksMock, OnStopFollowing(_));
    } else {
        EXPECT_CALL(*CallbacksMock, OnStartFollowing(_))
            .Times(0);
        EXPECT_CALL(*CallbacksMock, OnStopFollowing(_))
            .Times(0);
    }
    EXPECT_CALL(*CallbacksMock, OnStartLeading(_))
        .Times(0);
    EXPECT_CALL(*CallbacksMock, OnStopLeading(_))
        .Times(0);

    RunElections();
}

INSTANTIATE_TEST_SUITE_P(
    ValueParametrized,
    TElectionDelayedTest,
    ::testing::Values(
        TDuration::MilliSeconds(100),
        TDuration::MilliSeconds(600)));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NElection
