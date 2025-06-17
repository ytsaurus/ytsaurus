#include "chaos_lease.h"

#include <yt/yt/client/api/chaos_lease_base.h>
#include <yt/yt/ytlib/chaos_client/chaos_node_service_proxy.h>

namespace NYT::NApi::NNative {

using namespace NChaosClient;

////////////////////////////////////////////////////////////////////////////////

class TChaosLease
    : public NApi::TChaosLeaseBase
{
public:
    TChaosLease(
        IClientPtr client,
        NRpc::IChannelPtr channel,
        TChaosLeaseId id,
        TDuration timeout,
        bool pingAncestors,
        const NLogging::TLogger& logger)
        : NApi::TChaosLeaseBase(
            std::move(client),
            std::move(channel),
            id,
            timeout,
            pingAncestors,
            std::nullopt,
            logger)
        , Proxy_(Channel_)
    { }

    TFuture<void> DoPing(const TPrerequisitePingOptions& /*options*/ = {}) override
    {
        auto req = Proxy_.PingChaosLease();
        // TODO(gryzlov-ad): Put correct timeout here.
        req->SetTimeout(NRpc::DefaultRpcRequestTimeout);

        ToProto(req->mutable_chaos_lease_id(), GetId());
        req->set_ping_ancestors(PingAncestors_);

        return req->Invoke().AsVoid();
    }

private:
    TChaosNodeServiceProxy Proxy_;
};

DEFINE_REFCOUNTED_TYPE(TChaosLease)

////////////////////////////////////////////////////////////////////////////////

NApi::IPrerequisitePtr CreateChaosLease(
    IClientPtr client,
    NRpc::IChannelPtr channel,
    TChaosLeaseId id,
    TDuration timeout,
    bool pingAncestors,
    const NLogging::TLogger& logger)
{
    auto chaosLease = New<TChaosLease>(
        std::move(client),
        std::move(channel),
        id,
        timeout,
        pingAncestors,
        logger);

    return chaosLease;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
