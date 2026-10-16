#include "worker_server.h"
#include "worker_protocol.h"

#include <yql/essentials/utils/log/log.h>
#include <yql/tools/yqlworker/config/config.h>

#include <library/cpp/messagebus/handler.h>
#include <library/cpp/messagebus/protobuf/ybusbuf.h>

#include <util/stream/format.h>


namespace NYql {
namespace {

//////////////////////////////////////////////////////////////////////////////
// TMsgBusWorkerServer
//////////////////////////////////////////////////////////////////////////////
class TMsgBusWorkerServer final:
        public IWorkerServer,
        public NBus::IBusServerHandler
{
public:
    TMsgBusWorkerServer(
            const NProto::TWorkerServerConfig& config,
            const TVector<NBus::TBindResult>& bindTo,
            IWorkerServerHandlerPtr handler)
        : Protocol_(bindTo.at(0).Addr.GetPort())
        , Handler_(handler)
    {
        NBus::TBusQueueConfig queueCfg;
        queueCfg.Name = "yqlworker.server";
        queueCfg.NumWorkers = config.GetThreads();
        BusQueue_ = NBus::CreateMessageQueue(queueCfg, "server");

        NBus::TBusClientSessionConfig sessionCfg;
        sessionCfg.MaxInFlight = config.GetMaxInFlight();
        sessionCfg.SendTimeout = config.GetSendTimeoutSeconds() * 1000;
        sessionCfg.TotalTimeout = config.GetTotalTimeoutSeconds() * 1000;
        sessionCfg.TcpNoDelay = true;
        // TODO (jamel): allow to configure more MsgBus options

        Session_ = NBus::TBusServerSession::Create(
                    &Protocol_, this, sessionCfg, BusQueue_, bindTo);
        Y_ASSERT(Session_ && "probably somebody is listening on the same port");
        YQL_CLOG(INFO, Net) << "MsgBus worker server listening on port "
                            << Session_->GetActualListenPort();
    }

private:
    void Shutdown() override {
        Session_->Shutdown();
    }

    int ActualPort() const override {
        return Session_->GetActualListenPort();
    }

    void OnMessage(NBus::TOnMessageContext& ctx) override {
        try {
            NBus::TBusMessage* msg = ctx.GetMessage();
            auto type = static_cast<EWorkerMessageType>(msg->GetHeader()->Type);

            switch (type) {
            case MTYPE_PROCESS_TASK_REQUEST: {
                auto reqMsg = static_cast<TBusProcessTaskRequest*>(msg);
                auto peerAddr = reqMsg->GetReplyTo();
                auto resp = Handler_->OnProcessTask(peerAddr, std::move(reqMsg->Record));
                ctx.SendReplyMove(new TBusProcessTaskResponse(std::move(resp)));
                break;
            }
            case MTYPE_KILL_TASK_REQUEST: {
                auto reqMsg = static_cast<TBusKillTaskRequest*>(msg);
                auto peerAddr = reqMsg->GetReplyTo();
                auto resp = Handler_->OnKillTask(peerAddr, std::move(reqMsg->Record));
                ctx.SendReplyMove(new TBusKillTaskResponse(std::move(resp)));
                break;
            }
            case MTYPE_SUBSCRIBE_REQUEST: {
                auto reqMsg = static_cast<TBusSubscribeRequest*>(msg);
                auto peerAddr = reqMsg->GetReplyTo();
                auto resp = Handler_->OnSubscribe(peerAddr, std::move(reqMsg->Record));
                ctx.SendReplyMove(new TBusSubscribeResponse(std::move(resp)));
                break;
            }
            case MTYPE_UNSUBSCRIBE_REQUEST: {
                auto reqMsg = static_cast<TBusUnsubscribeRequest*>(msg);
                auto peerAddr = reqMsg->GetReplyTo();
                auto resp = Handler_->OnUnsubscribe(peerAddr, std::move(reqMsg->Record));
                ctx.SendReplyMove(new TBusUnsubscribeResponse(std::move(resp)));
                break;
            }
            default:
                YQL_CLOG(WARN, Net) << "Unknown message type: " << Hex(type);
            }
        } catch (...) {
            YQL_CLOG(ERROR, Net) << "unhandled exception: "
                                 << CurrentExceptionMessage();
        }
    }

    // called when message or reply can't be delivered
    void OnError(
            TAutoPtr<NBus::TBusMessage> msg,
            NBus::EMessageStatus status) override
    {
#define LOG_UNKNOWN_MESSAGE_CASE(name, ...) \
    case name: \
        YQL_CLOG(ERROR, Net) \
                << "Cannot deliver message " #name " " \
                << NBus::GetMessageStatus(status) << ": " \
                << NBus::MessageStatusDescription(status); \
        break;

        try {
            auto type = static_cast<EWorkerMessageType>(msg->GetHeader()->Type);
            switch (type) {
                WORKER_MESSAGE_TYPES(LOG_UNKNOWN_MESSAGE_CASE)
            default:
                YQL_CLOG(ERROR, Net)
                        << "Cannot deliver unknown message " << Hex(type)
                        << " " << NBus::GetMessageStatus(status)
                        << ": " << NBus::MessageStatusDescription(status);
            }
        } catch (...) {
            YQL_CLOG(ERROR, Net) << "unhandled exception: "
                                 << CurrentExceptionMessage();
        }

#undef LOG_UNKNOWN_MESSAGE_CASE
    }

private:
    TMsgBusWorkerProtocol Protocol_;
    NBus::TBusMessageQueuePtr BusQueue_;
    NBus::TBusServerSessionPtr Session_;
    IWorkerServerHandlerPtr Handler_;
};

} // namspace

IWorkerServerPtr CreateMsgBusWorkerServer(
        const NProto::TWorkerServerConfig& config,
        const TVector<NBus::TBindResult>& bindTo,
        IWorkerServerHandlerPtr handler)
{
    return new TMsgBusWorkerServer(config, bindTo, handler);
}

} // namespace NYql
