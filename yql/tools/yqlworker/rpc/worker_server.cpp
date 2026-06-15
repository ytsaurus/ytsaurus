#include "worker_server.h"

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/signals/utils.h>
#include <yql/essentials/providers/common/metrics/sensors_group.h>


namespace NYql {
namespace {

TSensorCounterPtr NewRpsCounter(const TString& name) {
    static TString rpsLabel("rps");
    static TSensorsGroupPtr g = GetSensorsGroupFor(NSensorComponent::kWorkerServer);
    return g->GetNamedCounter(rpsLabel, name, true);
}

TString FormatRequest(const NProto::TProcessTaskRequest& request) {
    if (!request.HasData()) {
        return PbMessageToStr(request);
    }

    NProto::TProcessTaskRequest copy(request);
    auto data = copy.MutableData();
    data->ClearFiles();
    data->ClearAuthData();
    data->ClearAttributes();
    data->ClearParameters();
    data->ClearProgram();
    data->ClearGatewaysConfig();
    data->ClearFunctionRegistryData();
    data->ClearGatewaysConfigPatch();
    data->ClearUserAuthData();
    return PbMessageToStr(copy);
}

// adds:
//    * function profiling logs
//    * tracing logs of input and output messages
//    * monitoring counters
class TProfilingWorkerServerHandler final: public IWorkerServerHandler {
public:
    TProfilingWorkerServerHandler(IWorkerServerHandlerPtr slave)
        : Slave_(std::move(slave))
    {
        ProcessTaskRps_ = NewRpsCounter("process_task");
        KillTaskRps_ = NewRpsCounter("kill_task");
        SubscribeRps_ = NewRpsCounter("subscribe");
        UnsubscribeRps_ = NewRpsCounter("unsubscribe");
    }

    NProto::TProcessTaskResponse OnProcessTask(
            const NAddr::IRemoteAddr& peerAddr,
            NProto::TProcessTaskRequest&& request) override
    {
        YQL_PROFILE_FUNC(TRACE);
        ProcessTaskRps_->Inc();
        YQL_CLOG(TRACE, Net) << "ProcessTask(" << peerAddr << ", {"
                             << FormatRequest(request) << "})";
        auto resp = Slave_->OnProcessTask(peerAddr, std::move(request));
        YQL_CLOG(TRACE, Net) << "ProcessTask() response: " << PbMessageToStr(resp);
        return resp;
    }

    NProto::TKillTaskResponse OnKillTask(
            const NAddr::IRemoteAddr& peerAddr,
            NProto::TKillTaskRequest&& request) override
    {
        YQL_PROFILE_FUNC(TRACE);
        KillTaskRps_->Inc();
        YQL_CLOG(TRACE, Net) << "KillTask(" << peerAddr << ", {"
                             << PbMessageToStr(request) << "})";
        auto resp = Slave_->OnKillTask(peerAddr, std::move(request));
        YQL_CLOG(TRACE, Net) << "KillTask() response: " << PbMessageToStr(resp);
        return resp;
    }

    NProto::TSubscribeResponse OnSubscribe(
            const NAddr::IRemoteAddr& peerAddr,
            NProto::TSubscribeRequest&& request) override
    {
        SubscribeRps_->Inc();
        YQL_CLOG(TRACE, Net) << "Subscribe(" << peerAddr << ", {"
                             << PbMessageToStr(request) << "})";
        auto resp = Slave_->OnSubscribe(peerAddr, std::move(request));
        YQL_CLOG(TRACE, Net) << "Subscribe() response: " << PbMessageToStr(resp);
        return resp;
    }

    NProto::TUnsubscribeResponse OnUnsubscribe(
            const NAddr::IRemoteAddr& peerAddr,
            NProto::TUnsubscribeRequest&& request) override
    {
        UnsubscribeRps_->Inc();
        YQL_CLOG(TRACE, Net) << "Unsubscribe(" << peerAddr << ", {"
                             << PbMessageToStr(request) << "})";
        auto resp = Slave_->OnUnsubscribe(peerAddr, std::move(request));
        YQL_CLOG(TRACE, Net) << "Unsubscribe() response: " << PbMessageToStr(resp);
        return resp;
    }

private:
    IWorkerServerHandlerPtr Slave_;
    // counters
    TSensorCounterPtr ProcessTaskRps_;
    TSensorCounterPtr KillTaskRps_;
    TSensorCounterPtr SubscribeRps_;
    TSensorCounterPtr UnsubscribeRps_;
};

} // namspace


IWorkerServerHandlerPtr WrapWithProfilingHandler(IWorkerServerHandlerPtr h) {
    return new TProfilingWorkerServerHandler(std::move(h));
}

} // namspace NYql
