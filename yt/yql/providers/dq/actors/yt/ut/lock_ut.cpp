#include <yt/yql/providers/dq/actors/yt/lock.h>
#include <yt/yql/providers/dq/actors/yt/yt_wrapper.h>

#include <contrib/ydb/library/actors/testlib/test_runtime.h>

#include <contrib/ydb/library/yql/providers/dq/actors/events/events.h>

#include <library/cpp/testing/unittest/registar.h>

#include <yt/yt/client/unittests/mock/transaction.h>

using namespace NActors;
using namespace NYql;

namespace {

bool AllowScheduledEvents(
    TTestActorRuntimeBase& runtime,
    TAutoPtr<IEventHandle>& event,
    TDuration delay,
    TInstant& deadline)
{
    Y_UNUSED(event);
    deadline = runtime.GetTimeProvider()->Now() + delay;
    return false;
}

struct TLockTestRuntime {
    TLockTestRuntime() {
        Runtime.SetScheduledEventFilter(AllowScheduledEvents);
        Runtime.Initialize();
        YtActor = Runtime.AllocateEdgeActor();
        ParentActor = Runtime.AllocateEdgeActor();
        LockActor = Runtime.Register(CreateYtLock(YtActor, "//lock", "gwm", "{}", false));
        Runtime.EnableScheduleForActor(LockActor);
    }

    TEvCreateNode::TPtr GrabCreatePrefixRequest() {
        return Runtime.GrabEdgeEvent<TEvCreateNode>(YtActor, TDuration::Seconds(5));
    }

    void SwitchToFollowerMode() {
        Runtime.Send(new IEventHandle(LockActor, ParentActor, new TEvBecomeFollower()));
    }

    TTestActorRuntimeBase Runtime;
    TActorId YtActor;
    TActorId ParentActor;
    TActorId LockActor;
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(TYtLockTest) {

    Y_UNIT_TEST(FirstLockAttemptIsImmediate) {
        TLockTestRuntime env;
        const auto start = env.Runtime.GetTimeProvider()->Now();

        auto createRequest = env.GrabCreatePrefixRequest();

        UNIT_ASSERT(createRequest);
        UNIT_ASSERT_VALUES_EQUAL(env.Runtime.GetTimeProvider()->Now() - start, TDuration::Zero());
    }

    Y_UNIT_TEST(FailedLockAttemptsAreRetriedAfterFiveSeconds) {
        TLockTestRuntime env;
        auto createRequest = env.GrabCreatePrefixRequest();
        UNIT_ASSERT(createRequest);

        for (size_t attempt = 0; attempt < 2; ++attempt) {
            const auto start = env.Runtime.GetTimeProvider()->Now();
            env.Runtime.Send(new IEventHandle(
                env.LockActor,
                env.YtActor,
                new TEvCreateNodeResponse(
                    createRequest->Get()->RequestId,
                    NYT::TErrorOr<NYT::NCypressClient::TNodeId>(NYT::TError("Test prefix creation failure")))));

            createRequest = env.GrabCreatePrefixRequest();

            UNIT_ASSERT(createRequest);
            UNIT_ASSERT_VALUES_EQUAL(env.Runtime.GetTimeProvider()->Now() - start, TDuration::Seconds(5));
        }
    }

    Y_UNIT_TEST(FollowerPollsLeaderInfoEveryFiveSeconds) {
        TLockTestRuntime env;
        auto createRequest = env.GrabCreatePrefixRequest();
        UNIT_ASSERT(createRequest);

        auto start = env.Runtime.GetTimeProvider()->Now();
        env.SwitchToFollowerMode();

        for (int poll = 0; poll < 2; ++poll) {
            auto getNodeRequest = env.Runtime.GrabEdgeEvent<TEvGetNode>(
                env.YtActor,
                TDuration::Seconds(5));

            UNIT_ASSERT(getNodeRequest);
            UNIT_ASSERT_VALUES_EQUAL(env.Runtime.GetTimeProvider()->Now() - start, TDuration::Seconds(5));

            start = env.Runtime.GetTimeProvider()->Now();
            env.Runtime.Send(new IEventHandle(
                env.LockActor,
                env.YtActor,
                new TEvGetNodeResponse(
                    getNodeRequest->Get()->RequestId,
                    NYT::TErrorOr<NYT::NYson::TYsonString>(NYT::NYson::TYsonString(TStringBuf("{}"))))));
        }
    }

    Y_UNIT_TEST(DelayedCreateNodeResponseDoesNotStartTransactionInFollowerMode) {
        TLockTestRuntime env;

        auto createRequest = env.GrabCreatePrefixRequest();
        UNIT_ASSERT(createRequest);
        env.SwitchToFollowerMode();

        env.Runtime.Send(new IEventHandle(
            env.LockActor,
            env.YtActor,
            new TEvCreateNodeResponse(
                createRequest->Get()->RequestId,
                NYT::TErrorOr<NYT::NCypressClient::TNodeId>(NYT::NCypressClient::TNodeId::Create()))));

        TAutoPtr<IEventHandle> handle;
        auto events = env.Runtime.GrabEdgeEvents<TEvStartTransaction, TEvGetNode>(
            handle,
            TDuration::Seconds(5));
        UNIT_ASSERT_C(!std::get<0>(events), "A stale CreateNode response started a transaction");
        UNIT_ASSERT_C(std::get<1>(events), "Follower mode did not start polling leader info");
    }

    Y_UNIT_TEST(DelayedStartTransactionResponseIsAbortedInFollowerMode) {
        TLockTestRuntime env;

        auto createRequest = env.GrabCreatePrefixRequest();
        UNIT_ASSERT(createRequest);
        env.Runtime.Send(new IEventHandle(
            env.LockActor,
            env.YtActor,
            new TEvCreateNodeResponse(
                createRequest->Get()->RequestId,
                NYT::TErrorOr<NYT::NCypressClient::TNodeId>(NYT::NCypressClient::TNodeId::Create()))));

        auto startRequest = env.Runtime.GrabEdgeEvent<TEvStartTransaction>(
            env.YtActor,
            TDuration::Seconds(5));
        UNIT_ASSERT(startRequest);
        env.SwitchToFollowerMode();

        auto transaction = NYT::New<NYT::NApi::TMockTransaction>();
        EXPECT_CALL(*transaction, GetId())
            .WillOnce(::testing::Return(NYT::NTransactionClient::TTransactionId::Create()));
        EXPECT_CALL(*transaction, Abort(::testing::_))
            .WillOnce(::testing::Return(NYT::OKFuture));

        env.Runtime.Send(new IEventHandle(
            env.LockActor,
            env.YtActor,
            new TEvStartTransactionResponse(
                startRequest->Get()->RequestId,
                NYT::TErrorOr<NYT::NApi::ITransactionPtr>(transaction))));

        auto getNodeRequest = env.Runtime.GrabEdgeEvent<TEvGetNode>(
            env.YtActor,
            TDuration::Seconds(5));
        UNIT_ASSERT_C(getNodeRequest, "Follower mode did not continue polling leader info");
    }
}
