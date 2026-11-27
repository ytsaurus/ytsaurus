#include "data_protocol.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYql;
using namespace NProto;
using namespace NSubprocessTaskExecutor;

Y_UNIT_TEST_SUITE(TDataProtocolTest)
{
    Y_UNIT_TEST(ExplicitCoreHost) {
        struct TTestHandler: public IDataServerHandler {
            explicit TTestHandler(TString id)
                : Id(std::move(id))
            {
            }

            TGetTaskResponse OnGetTask(TGetTaskRequest&&) override {
                TGetTaskResponse response;
                response.MutableData()->SetId(Id);
                return response;
            }

            void OnTaskWatchEvent(TTaskWatchEventRequest&&, NBus::TOnMessageContext*) override {
                Y_ABORT("must not be called");
            }

            void OnMetricsRegistrySnapshot(TMetricsRegistrySnapshotRequest&&) override {
                Y_ABORT("must not be called");
            }

            const TString Id;
        };

        auto bindTo = NBus::BindOnLoopbackPort(0, false).second;
        TTestHandler ipv4Handler("ipv4");
        TTestHandler ipv6Handler("ipv6");
        TDataServer ipv4Server({bindTo[0]}, 2, &ipv4Handler);
        TDataServer ipv6Server({bindTo[1]}, 2, &ipv6Handler);

        TDataClient ipv4Client("127.0.0.1", ipv4Server.ActualPort());
        TDataClient ipv6Client("::1", ipv6Server.ActualPort());
        UNIT_ASSERT_VALUES_EQUAL(
            ipv4Client.GetTask(TGetTaskRequest{}).GetValue(TDuration::Seconds(5)).GetData().GetId(), "ipv4");
        UNIT_ASSERT_VALUES_EQUAL(
            ipv6Client.GetTask(TGetTaskRequest{}).GetValue(TDuration::Seconds(5)).GetData().GetId(), "ipv6");
    }

    Y_UNIT_TEST(GetTask) {
        struct TTestHandler: public IDataServerHandler {
            TGetTaskResponse OnGetTask(TGetTaskRequest&& req) override {
                UNIT_ASSERT_EQUAL(req.GetPid(), 12345);
                TGetTaskResponse resp;
                resp.SetAction(ETaskAction::COMPILE);
                TTaskData* data = resp.MutableData();
                data->SetId("test_id");
                return resp;
            }

            void OnTaskWatchEvent(TTaskWatchEventRequest&&, NBus::TOnMessageContext*) override {
                Y_ABORT("must not be called");
            }

            void OnMetricsRegistrySnapshot(
                TMetricsRegistrySnapshotRequest&&) override {
                Y_ABORT("must not be called");
            }
        } testServerHanlder;

        auto bindTo = NBus::BindOnPort(0, false).second;
        TDataServer server(bindTo, 2, &testServerHanlder);
        TDataClient client(/*host=*/"localhost", server.ActualPort());

        TGetTaskRequest req;
        req.SetPid(12345);

        auto future = client.GetTask(std::move(req));
        const TGetTaskResponse& resp = future.GetValue(TDuration::Seconds(1));
        UNIT_ASSERT_EQUAL(resp.GetAction(), ETaskAction::COMPILE);
        UNIT_ASSERT_EQUAL(resp.GetData().GetId(), "test_id");
    }

    Y_UNIT_TEST(UpdateTaskStatus) {
        struct TTestHandler: public IDataServerHandler {
            TGetTaskResponse OnGetTask(TGetTaskRequest&& req) override {
                Y_UNUSED(req);
                Y_ABORT("must not be called");
            }

            void OnTaskWatchEvent(TTaskWatchEventRequest&& req, NBus::TOnMessageContext* taskWatchCtx) override {
                UNIT_ASSERT_EQUAL(req.GetPid(), 12345);
                UNIT_ASSERT_STRINGS_EQUAL(req.GetTaskId(), "test_id");
                UNIT_ASSERT_EQUAL(req.GetEvent(), NProto::TTaskWatchEventRequest::STARTED);
                UNIT_ASSERT(req.HasChanges());
                UNIT_ASSERT_EQUAL(req.GetChanges().GetRevision(), 1);
                if (taskWatchCtx) {
                    taskWatchCtx->SendReplyMove(new TBusTaskWatchEventResponse);
                }
            }

            void OnMetricsRegistrySnapshot(
                TMetricsRegistrySnapshotRequest&&) override {
                Y_ABORT("must not be called");
            }
        } testServerHanlder;

        auto bindTo = NBus::BindOnPort(0, false).second;
        TDataServer server(bindTo, 2, &testServerHanlder);
        TDataClient client(/*host=*/"localhost", server.ActualPort());

        TTaskWatchEventRequest req;
        req.SetPid(12345);
        req.SetTaskId("test_id");
        req.SetEvent(NProto::TTaskWatchEventRequest::STARTED);
        NProto::TTaskResult* result = req.MutableChanges();
        result->SetRevision(1);

        auto future = client.SendTaskWatchEvent(std::move(req));
        future.GetValue(TDuration::Seconds(1));
    }
} // Y_UNIT_TEST_SUITE(TDataProtocolTest)
