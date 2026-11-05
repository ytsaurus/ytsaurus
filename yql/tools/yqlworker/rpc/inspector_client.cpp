#include "inspector_client.h"

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/signals/utils.h>
#include <yql/essentials/providers/common/metrics/sensors_group.h>


namespace NYql {
namespace {

TSensorCounterPtr NewRpsCounter(const TString& name) {
    static TString rpsLabel("rps");
    static TSensorsGroupPtr g = GetSensorsGroupFor(NSensorComponent::kInspectorClient);
    return g->GetNamedCounter(rpsLabel, name, true);
}

// adds:
//    * function profiling logs
//    * tracing logs of input and output messages
//    * monitoring counters
class TProfilingInspectorClient final: public IInspectorClient {
public:
    TProfilingInspectorClient(IInspectorClientPtr slave)
        : Slave_(std::move(slave))
    {
        SendHeartbeatRps_ = NewRpsCounter("send_heartbeat");
        SendStatusUpdateRps_ = NewRpsCounter("send_status_update");
        NotifyTaskSubscriptionRps_ = NewRpsCounter("notify_task_subscription");
    }

    void SendHeartbeatToAll(
            NProto::THeartbeatRequest&& request,
            THeartbeatCallback callback) override
    {
        SendHeartbeatRps_->Inc();
        auto profileScope = YQL_PROFILE_FUNC_VAL(TRACE);
        YQL_CLOG(TRACE, Net) << "SendHeartbeatToAll({"
                             << PbMessageToStr(request) << "})";
        Slave_->SendHeartbeatToAll(std::move(request),
                [profileScope, callback](
                        const NAddr::IRemoteAddr& addr,
                        NProto::THeartbeatResponse&& resp)
                {
                    callback(addr, std::move(resp));
                });
    }

    void SendStatusUpdate(
            const NAddr::IRemoteAddr& prefferedAddr,
            NProto::TStatusUpdateRequest&& request,
            TStatusUpdateCallback callback) override
    {
        SendStatusUpdateRps_->Inc();
        auto profileScope = YQL_PROFILE_FUNC_VAL(TRACE);
        YQL_CLOG(DEBUG, Net)
            << "SendStatusUpdate of " << request.GetTaskId() << " revision "
            << (request.HasResult() ? request.GetResult().GetRevision() : 0u)
            << " to " << prefferedAddr;
        Slave_->SendStatusUpdate(prefferedAddr, std::move(request),
                [profileScope, callback](
                        const NAddr::IRemoteAddr& addr,
                        NProto::TStatusUpdateResponse&& resp)
                {
                    callback(addr, std::move(resp));
                });
    }

    void NotifyTaskSubscription(
            const NAddr::IRemoteAddr& peerAddress,
            NProto::TTaskSubscriptionNotifyRequest&& request) override
    {
        NotifyTaskSubscriptionRps_->Inc();
        YQL_CLOG(TRACE, Net) << "NotifyTaskSubscription(" << peerAddress
                             << ", {" << PbMessageToStr(request) << "})";
        Slave_->NotifyTaskSubscription(peerAddress, std::move(request));
    }

    void CloseConnectionWith(const NAddr::IRemoteAddr& peerAddr) override {
        YQL_PROFILE_FUNC(TRACE);
        Slave_->CloseConnectionWith(peerAddr);
    }

    size_t GetConnectionsCount() override {
        YQL_PROFILE_FUNC(TRACE);
        return Slave_->GetConnectionsCount();
    }

    void WaitAllMessagesDelivered() const override {
        YQL_PROFILE_FUNC(TRACE);
        Slave_->WaitAllMessagesDelivered();
    }

private:
    IInspectorClientPtr Slave_;
    // counters
    TSensorCounterPtr SendHeartbeatRps_;
    TSensorCounterPtr SendStatusUpdateRps_;
    TSensorCounterPtr NotifyTaskSubscriptionRps_;
};

} // namespace

IInspectorClientPtr WrapWithProfilingClient(IInspectorClientPtr c) {
    return std::make_shared<TProfilingInspectorClient>(std::move(c));
}

} // namespace NYql
