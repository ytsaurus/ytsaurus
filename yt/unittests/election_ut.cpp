#include "stdafx.h"
#include "framework.h"

#include <core/concurrency/action_queue.h>

#include <core/rpc/server.h>
#include <core/rpc/local_server.h>
#include <core/rpc/channel.h>
#include <core/rpc/local_channel.h>
#include <core/rpc/static_channel_factory.h>

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

#ifndef _win_

class TElectionTest
    : public testing::Test
{
public:
    TElectionTest()
        : DelayedExecutor()
        , ActionQueue(New<TActionQueue>("Main"))
        , CallbacksMock(New<TElectionCallbacksMock>())
        , RpcServer(CreateLocalServer())
        , ChannelFactory(New<TStaticChannelFactory>())
    { }

    void Configure(int peerCount, TPeerId selfId)
    {
        PeerMocks.resize(peerCount);
        for (int id = 0; id < peerCount; ++id) {
            if (id != selfId) {
                auto mock = New<TElectionServiceMock>(ActionQueue->GetInvoker());
                auto server = CreateLocalServer();
                server->RegisterService(mock);
                auto channel = CreateLocalChannel(server);
                ChannelFactory->Add(GetPeerAddress(id), channel);
                PeerMocks[id] = mock;
            }
        }

        auto cellConfig = New<TCellConfig>();
        for (int id = 0; id < peerCount; ++id) {
            cellConfig->Addresses.push_back(GetPeerAddress(id));
        }
        auto cellManager = New<TCellManager>(cellConfig, ChannelFactory, selfId);

        auto electionConfig = New<TElectionManagerConfig>();
        electionConfig->RpcTimeout = TDuration::MilliSeconds(40);
        electionConfig->VotingRoundInterval = TDuration::MilliSeconds(10);
        ElectionManager = New<TElectionManager>(
            electionConfig,
            cellManager,
            ActionQueue->GetInvoker(),
            CallbacksMock,
            RpcServer);

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
    }

protected:
    TDelayedExecutor DelayedExecutor;
    TActionQueuePtr ActionQueue;
    TIntrusivePtr<TElectionCallbacksMock> CallbacksMock;
    IServerPtr RpcServer;
    TStaticChannelFactoryPtr ChannelFactory;
    TElectionManagerPtr ElectionManager;
    std::vector<TIntrusivePtr<TElectionServiceMock>> PeerMocks;

private:
    static Stroka GetPeerAddress(TPeerId id)
    {
        return "peer" + ToString(id);
    }

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
                response->set_state(id == 2 ? EPeerState::Leading : EPeerState::Following);
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

////////////////////////////////////////////////////////////////////////////////

struct TStatus
{
    EPeerState State;
    TPeerId VoteId;
    TEpochId VoteEpochId;
    TPeerPriority Priority;
};

struct TElectionTestData
{
    TNullable<TStatus> Statuses[2];
    int ExpectedLeader;
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
                const TStatus* status = data.Statuses[id - 1].GetPtr();
                if (status != nullptr) {
                    response->set_state(status->State);
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
        TElectionTestData{
            {
                TStatus{ EPeerState::Following, 0, TEpochId(), 1 },
                TStatus{ EPeerState::Following, 0, TEpochId(), 2 }
            },
            -1
        },
        TElectionTestData{
            {
                TStatus{ EPeerState::Leading, 1, OtherEpoch, 1 },
                TNullable<TStatus>()
            },
            1
        },
        TElectionTestData{
            {
                TStatus{ EPeerState::Leading, 1, OtherEpoch, -1 },
                TNullable<TStatus>()
            },
            -1
        },
        // all followers
        TElectionTestData{
            {
                TStatus{ EPeerState::Following, 1, OtherEpoch, 1 },
                TStatus{ EPeerState::Following, 2, OtherEpoch, 2 },
            },
            -1
        },
        // all leaders
        TElectionTestData{
            {
                TStatus{ EPeerState::Leading, 1, OtherEpoch, 1 },
                TStatus{ EPeerState::Leading, 2, OtherEpoch, 2 },
            },
            2
        },
        // potential leader should recognize itself as a leader
        TElectionTestData{
            {
                TStatus{ EPeerState::Following, 2, OtherEpoch, 1 },
                TStatus{ EPeerState::Following, 2, OtherEpoch, 2 },
            },
            -1
        }
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
                DelayedExecutor.Submit(BIND([=] () {
                    response->set_state(id == 2 ? EPeerState::Leading : EPeerState::Following);
                    response->set_vote_id(2);
                    ToProto(response->mutable_vote_epoch_id(), TEpochId());
                    response->set_priority(id);
                    response->set_self_id(id);
                    context->Reply();
                }), delay);
            }));
    }

    if (delay < TDuration::MilliSeconds(40)) {
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


#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NElection
} // namespace NYT
