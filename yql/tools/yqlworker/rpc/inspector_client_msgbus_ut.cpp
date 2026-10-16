#include "inspector_client.h"
#include "inspector_protocol.h"

#include <yql/cfg/proto/worker_config.pb.h>

#include <yql/essentials/utils/log/log.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <util/network/address.h>
#include <util/network/ip.h>
#include <library/cpp/messagebus/ybus.h>


using namespace NYql;

Y_UNIT_TEST_SUITE(TInspectorMsgBusClientTest) {

    struct TMsgBusClientServer: public NBus::IBusServerHandler {
        NLog::YqlLoggerScope Logger;
        TPortManager PortManager;
        TMsgBusInspectorProtocol Protocol;
        NBus::TBusServerSessionPtr ServerSession;
        NBus::TBusMessageQueuePtr MessageQueue;
        NProto::TInspectorConfig ClientConfig;
        IInspectorClientPtr Client;
        TAtomicCounter CallbackCounter;
        ui32 ExpectedCallbackCounter;
        i32 FailedResponses;

        TMsgBusClientServer(ui32 retries = 2, i32 failedResponses = 5)
            : Logger(&Cerr)
            , Protocol(PortManager.GetPort())
            , FailedResponses(failedResponses)
        {
            NLog::YqlLogger().SetComponentLevel(NYql::NLog::EComponent::Net, NYql::NLog::ELevel::TRACE);
            MessageQueue = NBus::CreateMessageQueue();
            NBus::TBusServerSessionConfig serverConfig;
            ServerSession = NBus::TBusServerSession::Create(&Protocol, this, serverConfig, MessageQueue);

            ClientConfig.AddHosts("localhost");
            ClientConfig.SetPort(Protocol.GetPort());
            ClientConfig.SetSendTimeoutSeconds(1);
            ClientConfig.SetTotalTimeoutSeconds(1);
            ClientConfig.SetMaxSendRetries(retries);
            ClientConfig.SetRetryIntervalMillis(10);
            ClientConfig.SetMaxInFlight(1);

            this->Client = CreateMsgBusInspectorClient(ClientConfig);
            UNIT_ASSERT_EQUAL(1, Client->GetConnectionsCount());
        }

        ~TMsgBusClientServer() override {
            Client.reset();
            UNIT_ASSERT_EQUAL(ExpectedCallbackCounter, CallbackCounter.Val());
            ServerSession->Shutdown();
            MessageQueue->Stop();
        }

        void OnMessage(NBus::TOnMessageContext& request) override {
            if (request.GetMessage()->GetHeader()->Type != MTYPE_UPDATE_STATUS_REQUEST) {
                Y_ABORT("unexpected request");
            }
            auto busReq = static_cast<TBusStatusUpdateRequest*>(request.GetMessage());
            if (busReq->Record.GetTaskId() == "task-ok") {
                TAutoPtr<TBusStatusUpdateResponse> response(new TBusStatusUpdateResponse);
                response->Record.SetStatus(NProto::TStatusUpdateResponse_EStatus_OK);
                request.SendReplyMove(response);
                return;
            } else if (busReq->Record.GetTaskId() == "task-reply-failed") {
                TAutoPtr<TBusStatusUpdateResponse> response(new TBusStatusUpdateResponse);
                response->Record.SetStatus(NProto::TStatusUpdateResponse_EStatus_FAILED);
                request.SendReplyMove(response);
                return;
            } else if (busReq->Record.GetTaskId() == "task-retried") {
                if (FailedResponses < 0) {
                    Y_ABORT("unexpected retry");
                }
                TAutoPtr<TBusStatusUpdateResponse> response(new TBusStatusUpdateResponse);
                response->Record.SetStatus((FailedResponses > 0)
                        ? NProto::TStatusUpdateResponse_EStatus_FAILED
                        : NProto::TStatusUpdateResponse_EStatus_OK);
                request.SendReplyMove(response);
                FailedResponses--;
                return;
            } else if (busReq->Record.GetTaskId() == "task-timeout") {
                if (FailedResponses < 0) {
                    Y_ABORT("unexpected retry");
                }
                if (FailedResponses > 0) {
                    request.ForgetRequest();
                } else {
                    TAutoPtr<TBusStatusUpdateResponse> response(new TBusStatusUpdateResponse);
                    response->Record.SetStatus(NProto::TStatusUpdateResponse_EStatus_OK);
                    request.SendReplyMove(response);
                }
                FailedResponses--;
                return;
            }
            request.ForgetRequest();
        }

        void Send(TString taskId) {
            NProto::TStatusUpdateRequest statusUpdate;
            statusUpdate.SetTaskId(taskId);

            NBus::TNetAddr addr("localhost", Protocol.GetPort());
            Client->SendStatusUpdate(addr, std::move(statusUpdate),
                [this](const NAddr::IRemoteAddr&, NProto::TStatusUpdateResponse&&) {
                    this->CallbackCounter.Inc();
                    YQL_CLOG(DEBUG, Net) << "Callback";
                });
        }
    };

    Y_UNIT_TEST(SendOk) {
        TMsgBusClientServer msgBus(0);
        msgBus.Send("task-ok");
        msgBus.ExpectedCallbackCounter = 1;
    }

    Y_UNIT_TEST(SendBusy) {
        TMsgBusClientServer msgBus(10);
        for (int i = 0; i < 10; ++i) {
            msgBus.Send("task-ok");
        }
        msgBus.ExpectedCallbackCounter = 10;
        Sleep(TDuration::Seconds(2));
    }

    Y_UNIT_TEST(SendTimeoutFailed) {
        TMsgBusClientServer msgBus(2, 3);
        msgBus.Send("task-timeout");
        msgBus.ExpectedCallbackCounter = 0;
        Sleep(TDuration::Seconds(7));
    }

    Y_UNIT_TEST(SendTimeoutOk) {
        TMsgBusClientServer msgBus(2, 2);
        msgBus.Send("task-timeout");
        msgBus.ExpectedCallbackCounter = 1;
        Sleep(TDuration::Seconds(7));
    }

    Y_UNIT_TEST(SendReplyFailed) {
        TMsgBusClientServer msgBus;
        msgBus.Send("task-reply-failed");
        msgBus.ExpectedCallbackCounter = 0;
        Sleep(TDuration::Seconds(2));
    }

    Y_UNIT_TEST(SendRetried) {
        TMsgBusClientServer msgBus(5, 5);
        msgBus.Send("task-retried");
        msgBus.ExpectedCallbackCounter = 1;
        Sleep(TDuration::Seconds(2));
    }
}
