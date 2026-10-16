#include "nodeid_cleaner.h"
#include "yt_wrapper.h"

#include <contrib/ydb/library/yql/providers/dq/actors/events/events.h>
#include <contrib/ydb/library/yql/providers/dq/common/attrs.h>

#include <contrib/ydb/library/actors/testlib/test_runtime.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <yql/essentials/utils/log/proto/logger_config.pb.h>
#include <yql/essentials/utils/log/log.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/yson/node/node_io.h>

#include <yt/cpp/mapreduce/interface/fluent.h>

using namespace NYql;
using namespace NActors;

namespace {

// By default TTestActorRuntimeBase drops all scheduled events (NopFilterFunc returns true).
// This filter allows scheduling for actors enabled via EnableScheduleForActor,
// placing them at the correct virtual time. Others are still dropped.
// By default TTestActorRuntimeBase drops all scheduled events (NopFilterFunc returns true).
// This filter allows scheduling for all actors, placing events at the correct virtual time.
bool ScheduledFilterFunc(TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event,
    TDuration delay, TInstant& deadline)
{
    Y_UNUSED(runtime);
    Y_UNUSED(event);
    deadline = runtime.GetTimeProvider()->Now() + delay;
    return false; // allow scheduling for all actors
}

void SetupLogging(TTestActorRuntimeBase& /*runtime*/) {
    NYql::NProto::TLoggingConfig loggerConfig;
    loggerConfig.set_allcomponentslevel(NYql::NProto::TLoggingConfig_ELevel_TRACE);
    NYql::NLog::InitLogger(loggerConfig, false);
}

// Build a YSON list-node response with a single stale node entry.
// The node has ACTOR_NODEID_ATTR and a modification_time far in the past
// so that it exceeds Options.Timeout and triggers removal.
TEvListNodeResponse* MakeStaleListResponse(ui64 requestId) {
    NYT::TNode list = NYT::TNode::CreateList();

    NYT::TNode node("stale_worker");
    node.Attributes()[NCommonAttrs::ACTOR_NODEID_ATTR] = NYT::TNode::CreateList();
    node.Attributes()["modification_time"] = "2020-01-01T00:00:00.000000Z";
    list.Add(node);

    NYT::TErrorOr<TString> ok(NYT::NodeToYsonString(list));
    return new TEvListNodeResponse(requestId, ok);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TNodeIdCleanerTest) {

    // The cleaner must NOT remove nodes on the first successful listing.
    // LastListNodeOk starts false, so the first OnListNodeResponse only primes
    // the flag and schedules a retry. Removal happens only on the second listing.
    // This regresses the bug where LastListNodeOk was initialized to true,
    // causing the first (possibly stale) listing to trigger removals immediately.
    Y_UNIT_TEST(FirstListingDoesNotRemoveNodes) {
        TTestActorRuntimeBase runtime;
        runtime.SetScheduledEventFilter(ScheduledFilterFunc);
        runtime.Initialize();
        SetupLogging(runtime);

        TActorId ytActor = runtime.AllocateEdgeActor();

        TNodeIdCleanerOptions options;
        options.Prefix = "//home/test/worker_node";
        options.CheckPeriod = TDuration::MilliSeconds(100);
        options.RetryPeriod = TDuration::MilliSeconds(100);
        options.Timeout = TDuration::MilliSeconds(1); // 1ms — any past timestamp is stale

        TActorId cleanerActor = runtime.Register(
            CreateNodeIdCleaner(ytActor, options));
        runtime.EnableScheduleForActor(cleanerActor);

        // Wait for the cleaner to send the first TEvListNode.
        auto listEv1 = runtime.GrabEdgeEvent<TEvListNode>(ytActor, TDuration::Seconds(5));
        UNIT_ASSERT_C(listEv1, "Cleaner did not send first TEvListNode");

        // Respond with a list containing a stale node.
        runtime.Send(new IEventHandle(cleanerActor, ytActor,
            MakeStaleListResponse(listEv1->Get()->RequestId)));

        // The cleaner must NOT send TEvRemoveNode after the first listing.
        // It should only schedule a retry (TEvListNode again after RetryPeriod).
        // Use GrabEdgeEvents to capture whichever event arrives first.
        TAutoPtr<IEventHandle> handle;
        auto events = runtime.GrabEdgeEvents<TEvListNode, TEvRemoveNode>(handle, TDuration::Seconds(5));
        auto* listEv2 = std::get<0>(events);
        auto* removeEv = std::get<1>(events);
        UNIT_ASSERT_C(listEv2, "Cleaner did not send second TEvListNode (retry)");
        UNIT_ASSERT_C(!removeEv, "Cleaner sent TEvRemoveNode on first listing — "
            "LastListNodeOk guard failed (should require 2 consecutive OK listings)");
    }

    // The cleaner must remove stale nodes on the second successful listing.
    // First listing primes LastListNodeOk=true; second listing passes the guard
    // and calls ScheduleRemove, which sends TEvRemoveNode for stale nodes.
    Y_UNIT_TEST(SecondListingRemovesStaleNodes) {
        TTestActorRuntimeBase runtime;
        runtime.SetScheduledEventFilter(ScheduledFilterFunc);
        runtime.Initialize();
        SetupLogging(runtime);

        TActorId ytActor = runtime.AllocateEdgeActor();

        TNodeIdCleanerOptions options;
        options.Prefix = "//home/test/worker_node";
        options.CheckPeriod = TDuration::MilliSeconds(100);
        options.RetryPeriod = TDuration::MilliSeconds(100);
        options.Timeout = TDuration::MilliSeconds(1); // 1ms — any past timestamp is stale

        TActorId cleanerActor = runtime.Register(
            CreateNodeIdCleaner(ytActor, options));
        runtime.EnableScheduleForActor(cleanerActor);

        // First listing: respond with stale node → primes LastListNodeOk, no removal.
        auto listEv1 = runtime.GrabEdgeEvent<TEvListNode>(ytActor, TDuration::Seconds(5));
        UNIT_ASSERT_C(listEv1, "Cleaner did not send first TEvListNode");
        runtime.Send(new IEventHandle(cleanerActor, ytActor,
            MakeStaleListResponse(listEv1->Get()->RequestId)));

        // Second listing: respond with same stale node → guard passes → removal.
        auto listEv2 = runtime.GrabEdgeEvent<TEvListNode>(ytActor, TDuration::Seconds(5));
        UNIT_ASSERT_C(listEv2, "Cleaner did not send second TEvListNode");
        runtime.Send(new IEventHandle(cleanerActor, ytActor,
            MakeStaleListResponse(listEv2->Get()->RequestId)));

        // Now the cleaner must send TEvRemoveNode for the stale node.
        auto removeEv = runtime.GrabEdgeEvent<TEvRemoveNode>(ytActor, TDuration::Seconds(5));
        UNIT_ASSERT_C(removeEv, "Cleaner did not send TEvRemoveNode after second listing");

        const TString& removePath = std::get<0>(*removeEv->Get());
        UNIT_ASSERT_C(removePath.Contains("stale_worker"),
            "TEvRemoveNode path should target stale_worker, got: " << removePath);
    }

    // After a failed listing, the next successful listing is treated as the "first"
    // (LastListNodeOk reset to false), so removal still requires two consecutive
    // successful listings.
    Y_UNIT_TEST(FailedListingResetsToFirstAttempt) {
        TTestActorRuntimeBase runtime;
        runtime.SetScheduledEventFilter(ScheduledFilterFunc);
        runtime.Initialize();
        SetupLogging(runtime);

        TActorId ytActor = runtime.AllocateEdgeActor();

        TNodeIdCleanerOptions options;
        options.Prefix = "//home/test/worker_node";
        options.CheckPeriod = TDuration::MilliSeconds(100);
        options.RetryPeriod = TDuration::MilliSeconds(100);
        options.Timeout = TDuration::MilliSeconds(1);

        TActorId cleanerActor = runtime.Register(
            CreateNodeIdCleaner(ytActor, options));
        runtime.EnableScheduleForActor(cleanerActor);

        // First listing: respond with an error (not OK).
        auto listEv1 = runtime.GrabEdgeEvent<TEvListNode>(ytActor, TDuration::Seconds(5));
        UNIT_ASSERT_C(listEv1, "Cleaner did not send first TEvListNode");
        {
            NYT::TErrorOr<TString> err(NYT::TError("List node failed"));
            runtime.Send(new IEventHandle(cleanerActor, ytActor,
                new TEvListNodeResponse(listEv1->Get()->RequestId, err)));
        }

        // Second listing: respond OK with stale node.
        // Since the first listing failed (LastListNodeOk=false), this OK listing
        // is the "first" successful one → should NOT trigger removal.
        auto listEv2 = runtime.GrabEdgeEvent<TEvListNode>(ytActor, TDuration::Seconds(5));
        UNIT_ASSERT_C(listEv2, "Cleaner did not send second TEvListNode");
        runtime.Send(new IEventHandle(cleanerActor, ytActor,
            MakeStaleListResponse(listEv2->Get()->RequestId)));

        // Third listing: respond OK with stale node again.
        // Now LastListNodeOk=true from the second listing → removal should fire.
        auto listEv3 = runtime.GrabEdgeEvent<TEvListNode>(ytActor, TDuration::Seconds(5));
        UNIT_ASSERT_C(listEv3, "Cleaner did not send third TEvListNode");
        runtime.Send(new IEventHandle(cleanerActor, ytActor,
            MakeStaleListResponse(listEv3->Get()->RequestId)));

        // Now TEvRemoveNode must be sent.
        auto removeEv = runtime.GrabEdgeEvent<TEvRemoveNode>(ytActor, TDuration::Seconds(5));
        UNIT_ASSERT_C(removeEv, "Cleaner did not send TEvRemoveNode after two consecutive OK listings");

        const TString& removePath = std::get<0>(*removeEv->Get());
        UNIT_ASSERT_C(removePath.Contains("stale_worker"),
            "TEvRemoveNode path should target stale_worker, got: " << removePath);
    }

}
