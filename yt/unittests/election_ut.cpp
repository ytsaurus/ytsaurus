#include "stdafx.h"
#include "framework.h"

#include <core/concurrency/action_queue.h>

#include <core/rpc/server.h>
#include <core/rpc/local_server.h>
#include <core/rpc/channel.h>
#include <core/rpc/local_channel.h>
#include <core/rpc/static_channel_factory.h>

#include <core/concurrency/scheduler.h>

#include <ytlib/election/cell_manager.h>
#include <ytlib/election/config.h>
#include <ytlib/election/election_service_mock.h>

#include <server/election/election_manager.h>
#include <server/election/config.h>
#include <server/election/election_callbacks_mock.h>

namespace NYT {
namespace NElection {
namespace {

using namespace NConcurrency;
using namespace NRpc;

using testing::Return;
using testing::InSequence;
using testing::Invoke;
using testing::_;

////////////////////////////////////////////////////////////////////////////////

class TElectionTest
    : public testing::Test
{
public:
    TElectionTest()
        : ActionQueue(New<TActionQueue>("Main"))
        , CallbacksMock(New<TElectionCallbacksMock>())
        , RpcServer(CreateLocalServer())
        , ChannelFactory(New<TStaticChannelFactory>())
        , RpcTimeout(TDuration::MilliSeconds(40))
    { }

    void Configure(int peerCount, TPeerId selfId)
    {
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
                auto channel = CreateLocalChannel(RpcServer);
                ChannelFactory->Add(GetPeerAddress(id), channel);
            }
        }

        auto cellConfig = New<TCellConfig>();
        for (int id = 0; id < peerCount; ++id) {
            cellConfig->Addresses.push_back(GetPeerAddress(id));
        }
        auto cellManager = New<TCellManager>(cellConfig, ChannelFactory, selfId);

        auto electionConfig = New<TElectionManagerConfig>();
        electionConfig->ControlRpcTimeout = RpcTimeout;
        electionConfig->VotingRoundPeriod = TDuration::MilliSeconds(10);
        electionConfig->FollowerPingRpcTimeout = TDuration::MilliSeconds(60);
        electionConfig->FollowerGraceTimeout = TDuration::MilliSeconds(30);
        electionConfig->FollowerPingPeriod = TDuration::MilliSeconds(50);
        ElectionManager = New<TElectionManager>(
            electionConfig,
            cellManager,
            ActionQueue->GetInvoker(),
            CallbacksMock);
        RpcServer->RegisterService(ElectionManager->GetRpcService());

        EXPECT_CALL(*CallbacksMock, FormatPriority(_))
            .WillRepeatedly(Invoke([] (TPeerPriority priority) {
                return ToString(priority);
            }));
    }

    void Sleep(int ticks = 1)
    {
        ::Sleep(TDuration::MilliSeconds(100) * ticks);
    }

    void RunElections()
    {
        ElectionManager->Start();
        Sleep(1);
        ElectionManager->Stop();
        ElectionManager.Reset();
        Sleep(1);
    }

protected:
    TActionQueuePtr ActionQueue;
    TIntrusivePtr<TElectionCallbacksMock> CallbacksMock;
    IServerPtr RpcServer;
    TStaticChannelFactoryPtr ChannelFactory;
    TElectionManagerPtr ElectionManager;
    std::vector<TIntrusivePtr<TElectionServiceMock>> PeerMocks;

    const TDuration RpcTimeout;

    static Stroka GetPeerAddress(TPeerId id)
    {
        return "peer" + ToString(id);
    }

private:
    virtual void TearDown() override
    {
        Sleep();

        testing::Mock::VerifyAndClearExpectations(CallbacksMock.Get());

        for (auto mock : PeerMocks) {
            if (mock) {
                testing::Mock::VerifyAndClearExpectations(mock.Get());
            }
        }

        ElectionManager.Reset();
    }

};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TElectionTest, SinglePeer)
{
    Configure(1, 0);

    EXPECT_CALL(*CallbacksMock, GetPriority())
        .WillRepeatedly(Return(0));

    {
        InSequence dummy;
        EXPECT_CALL(*CallbacksMock, OnStartLeading());
        EXPECT_CALL(*CallbacksMock, OnStopLeading());
    }

    RunElections();
}

TEST_F(TElectionTest, JoinActiveQuorumNoResponseThenResponse)
{
    Configure(3, 0);

    EXPECT_CALL(*CallbacksMock, GetPriority())
        .WillRepeatedly(Return(0));

    for (int id = 1; id < 3; id++) {
        EXPECT_RPC_CALL(*PeerMocks[id], GetStatus)
            .WillOnce(HANLDE_RPC_CALL(TElectionServiceMock, GetStatus, [=], { }))
            .WillRepeatedly(HANLDE_RPC_CALL(TElectionServiceMock, GetStatus, [=], {
                response->set_state(static_cast<int>(id == 2 ? EPeerState::Leading : EPeerState::Following));
                response->set_vote_id(2);
                ToProto(response->mutable_vote_epoch_id(), TEpochId());
                response->set_priority(id);
                response->set_self_id(id);
                context->Reply();
            }));
    }

    {
        InSequence dummy;
        EXPECT_CALL(*CallbacksMock, OnStartFollowing());
        EXPECT_CALL(*CallbacksMock, OnStopFollowing());
    }

    RunElections();
}

TEST_F(TElectionTest, BecomeLeaderOneHealthyFollower)
{
    Configure(3, 0);

    EXPECT_CALL(*CallbacksMock, GetPriority())
        .WillRepeatedly(Return(0));

    for (int id = 1; id < 3; id++) {
        EXPECT_RPC_CALL(*PeerMocks[id], GetStatus)
            .WillRepeatedly(HANLDE_RPC_CALL(TElectionServiceMock, GetStatus, [=], {
                auto channel = ChannelFactory->CreateChannel(GetPeerAddress(0));
                TElectionServiceProxy proxy(channel);
                proxy.SetDefaultTimeout(RpcTimeout);

                auto rspOrError = WaitFor(proxy.GetStatus()->Invoke());
                EXPECT_TRUE(rspOrError.IsOK()) << ToString(rspOrError);
                const auto& rsp = rspOrError.Value();

                response->set_state(static_cast<int>(EPeerState::Following));
                response->set_vote_id(0);
                ToProto(response->mutable_vote_epoch_id(), rsp->vote_epoch_id());
                response->set_priority(id);
                response->set_self_id(id);
                context->Reply();
            }));
        if (id == 1) {
            EXPECT_RPC_CALL(*PeerMocks[id], PingFollower)
                .WillRepeatedly(HANLDE_RPC_CALL(TElectionServiceMock, PingFollower, [=], {
                    context->Reply();
                }));
        } else {
            EXPECT_RPC_CALL(*PeerMocks[id], PingFollower)
                .WillRepeatedly(HANLDE_RPC_CALL(TElectionServiceMock, PingFollower, [=], {
                    // Do not reply.
                }));
        }
    }

    {
        InSequence dummy;
        EXPECT_CALL(*CallbacksMock, OnStartLeading());
        EXPECT_CALL(*CallbacksMock, OnStopLeading());
    }

    RunElections();
}

TEST_F(TElectionTest, BecomeLeaderTwoHealthyFollowers)
{
    Configure(3, 0);

    EXPECT_CALL(*CallbacksMock, GetPriority())
        .WillRepeatedly(Return(0));

    for (int id = 1; id < 3; id++) {
        EXPECT_RPC_CALL(*PeerMocks[id], GetStatus)
            .WillRepeatedly(HANLDE_RPC_CALL(TElectionServiceMock, GetStatus, [=], {
                auto channel = ChannelFactory->CreateChannel(GetPeerAddress(0));
                TElectionServiceProxy proxy(channel);
                proxy.SetDefaultTimeout(RpcTimeout);

                auto rspOrError = WaitFor(proxy.GetStatus()->Invoke());
                EXPECT_TRUE(rspOrError.IsOK()) << ToString(rspOrError);
                const auto& rsp = rspOrError.Value();

                response->set_state(static_cast<int>(EPeerState::Following));
                response->set_vote_id(0);
                ToProto(response->mutable_vote_epoch_id(), rsp->vote_epoch_id());
                response->set_priority(id);
                response->set_self_id(id);
                context->Reply();
            }));
        EXPECT_RPC_CALL(*PeerMocks[id], PingFollower)
            .WillRepeatedly(HANLDE_RPC_CALL(TElectionServiceMock, PingFollower, [=], {
                context->Reply();
            }));
    }

    {
        InSequence dummy;
        EXPECT_CALL(*CallbacksMock, OnStartLeading());
        EXPECT_CALL(*CallbacksMock, OnStopLeading());
    }

    RunElections();
}

TEST_F(TElectionTest, BecomeLeaderQuorumLostOnce)
{
    Configure(3, 0);

    EXPECT_CALL(*CallbacksMock, GetPriority())
        .WillRepeatedly(Return(0));

    int startLeadingCounter = 0;
    for (int id = 1; id < 3; id++) {
        EXPECT_RPC_CALL(*PeerMocks[id], GetStatus)
            .WillRepeatedly(HANLDE_RPC_CALL(TElectionServiceMock, GetStatus, [=], {
                auto channel = ChannelFactory->CreateChannel(GetPeerAddress(0));
                TElectionServiceProxy proxy(channel);
                proxy.SetDefaultTimeout(RpcTimeout);

                auto rspOrError = WaitFor(proxy.GetStatus()->Invoke());
                EXPECT_TRUE(rspOrError.IsOK()) << ToString(rspOrError);
                const auto& rsp = rspOrError.Value();

                response->set_state(static_cast<int>(EPeerState::Following));
                response->set_vote_id(0);
                ToProto(response->mutable_vote_epoch_id(), rsp->vote_epoch_id());
                response->set_priority(id);
                response->set_self_id(id);
                context->Reply();
            }));
        EXPECT_RPC_CALL(*PeerMocks[id], PingFollower)
            .WillRepeatedly(HANLDE_RPC_CALL(TElectionServiceMock, PingFollower, [&], {
                if (startLeadingCounter > 1) {
                    context->Reply();
                }
            }));
    }

    {
        InSequence dummy;
        EXPECT_CALL(*CallbacksMock, OnStartLeading())
            .WillOnce(::testing::Invoke([&] {
                ++startLeadingCounter;
            }));
        EXPECT_CALL(*CallbacksMock, OnStopLeading())
            .WillOnce(::testing::Invoke([&] {
                ElectionManager->Start();
            }));
        EXPECT_CALL(*CallbacksMock, OnStartLeading())
            .WillOnce(::testing::Invoke([&] {
                ++startLeadingCounter;
            }));
        EXPECT_CALL(*CallbacksMock, OnStopLeading());
    }

    RunElections();
}

TEST_F(TElectionTest, BecomeLeaderGracePeriod)
{
    Configure(3, 0);

    EXPECT_CALL(*CallbacksMock, GetPriority())
        .WillRepeatedly(Return(0));

    for (int id = 1; id < 3; id++) {
        EXPECT_RPC_CALL(*PeerMocks[id], GetStatus)
            .WillRepeatedly(HANLDE_RPC_CALL(TElectionServiceMock, GetStatus, [=], {
                auto channel = ChannelFactory->CreateChannel(GetPeerAddress(0));
                TElectionServiceProxy proxy(channel);
                proxy.SetDefaultTimeout(RpcTimeout);

                auto rspOrError = WaitFor(proxy.GetStatus()->Invoke());
                EXPECT_TRUE(rspOrError.IsOK()) << ToString(rspOrError);
                const auto& rsp = rspOrError.Value();

                response->set_state(static_cast<int>(EPeerState::Following));
                response->set_vote_id(0);
                ToProto(response->mutable_vote_epoch_id(), rsp->vote_epoch_id());
                response->set_priority(id);
                response->set_self_id(id);
                context->Reply();
            }));
        EXPECT_RPC_CALL(*PeerMocks[id], PingFollower)
            .WillRepeatedly(HANLDE_RPC_CALL(TElectionServiceMock, PingFollower, [], {
                THROW_ERROR_EXCEPTION(NElection::EErrorCode::InvalidLeader, "");
            }));
    }

    {
        InSequence dummy;
        EXPECT_CALL(*CallbacksMock, OnStartLeading());
        EXPECT_CALL(*CallbacksMock, OnStopLeading())
            .WillOnce(::testing::Invoke([&] {
                ElectionManager->Start();
            }));
        EXPECT_CALL(*CallbacksMock, OnStartLeading());
        EXPECT_CALL(*CallbacksMock, OnStopLeading());
    }

    RunElections();
}

////////////////////////////////////////////////////////////////////////////////

struct TStatus
{
    EPeerState State;
    TPeerId VoteId;
    TEpochId VoteEpochId;
    TPeerPriority Priority;

    TStatus(EPeerState state, TPeerId voteId, TEpochId voteEpochId, TPeerPriority priority)
        : State(state)
        , VoteId(voteId)
        , VoteEpochId(voteEpochId)
        , Priority(priority)
    { }
};

struct TElectionTestData
{
    TNullable<TStatus> Statuses[2];
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
        .WillRepeatedly(Return(0));

    for (int id = 1; id < 3; id++) {
        EXPECT_RPC_CALL(*PeerMocks[id], GetStatus)
            .WillRepeatedly(HANLDE_RPC_CALL(TElectionServiceMock, GetStatus, [=], {
                const auto* status = data.Statuses[id - 1].GetPtr();
                if (status) {
                    response->set_state(static_cast<int>(status->State));
                    response->set_vote_id(status->VoteId);
                    ToProto(response->mutable_vote_epoch_id(), status->VoteEpochId);
                    response->set_priority(status->Priority);
                    response->set_self_id(id);
                    context->Reply();
                }
            }));
    }

    if (data.ExpectedLeader >= 0) {
        InSequence dummy;
        EXPECT_CALL(*CallbacksMock, OnStartFollowing());
        EXPECT_CALL(*CallbacksMock, OnStopFollowing());
    } else {
        EXPECT_CALL(*CallbacksMock, OnStartFollowing())
            .Times(0);
        EXPECT_CALL(*CallbacksMock, OnStopFollowing())
            .Times(0);
    }
    EXPECT_CALL(*CallbacksMock, OnStartLeading())
        .Times(0);
    EXPECT_CALL(*CallbacksMock, OnStopLeading())
        .Times(0);

    RunElections();
}

static TEpochId OtherEpoch = TEpochId::Create();

INSTANTIATE_TEST_CASE_P(
    ValueParametrized,
    TElectionGenericTest,
    ::testing::Values(
        TElectionTestData(
            -1,
            TStatus(EPeerState::Following, 0, TEpochId(), 1),
            TStatus(EPeerState::Following, 0, TEpochId(), 2)
        ),
        TElectionTestData(
            1,
            TStatus(EPeerState::Leading, 1, OtherEpoch, 1)
        ),
        TElectionTestData(
            -1,
            TStatus(EPeerState::Leading, 1, OtherEpoch, -1)
        ),
        // all followers
        TElectionTestData(
            -1,
            TStatus(EPeerState::Following, 1, OtherEpoch, 1),
            TStatus(EPeerState::Following, 2, OtherEpoch, 2)
        ),
        // all leaders
        TElectionTestData(
            2,
            TStatus(EPeerState::Leading, 1, OtherEpoch, 1),
            TStatus(EPeerState::Leading, 2, OtherEpoch, 2)
        ),
        // potential leader should recognize itself as a leader
        TElectionTestData(
            -1,
            TStatus(EPeerState::Following, 2, OtherEpoch, 1),
            TStatus(EPeerState::Following, 2, OtherEpoch, 2)
        )
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
        .WillRepeatedly(Return(0));

    for (int id = 1; id < 3; id++) {
        EXPECT_RPC_CALL(*PeerMocks[id], GetStatus)
            .WillRepeatedly(HANLDE_RPC_CALL(TElectionServiceMock, GetStatus, [=], {
                TDelayedExecutor::Submit(BIND([=] () {
                    response->set_state(static_cast<int>(id == 2 ? EPeerState::Leading : EPeerState::Following));
                    response->set_vote_id(2);
                    ToProto(response->mutable_vote_epoch_id(), TEpochId());
                    response->set_priority(id);
                    response->set_self_id(id);
                    context->Reply();
                }), delay);
            }));
    }

    if (delay < RpcTimeout) {
        InSequence dummy;
        EXPECT_CALL(*CallbacksMock, OnStartFollowing());
        EXPECT_CALL(*CallbacksMock, OnStopFollowing());
    } else {
        EXPECT_CALL(*CallbacksMock, OnStartFollowing())
            .Times(0);
        EXPECT_CALL(*CallbacksMock, OnStopFollowing())
            .Times(0);
    }
    EXPECT_CALL(*CallbacksMock, OnStartLeading())
        .Times(0);
    EXPECT_CALL(*CallbacksMock, OnStopLeading())
        .Times(0);

    RunElections();
}

INSTANTIATE_TEST_CASE_P(
    ValueParametrized,
    TElectionDelayedTest,
    ::testing::Values(
        TDuration::MilliSeconds(10),
        TDuration::MilliSeconds(60)));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NElection
} // namespace NYT
