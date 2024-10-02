#include <yt/yt/core/rpc/channel.h>

#include <yt/yt/orm/client/native/response.h>
#include <yt/yt/orm/client/native/discovery_client.h>

namespace NYT::NOrm::NClient::NNative {

////////////////////////////////////////////////////////////////////////////////

namespace NTests {

////////////////////////////////////////////////////////////////////////////////

struct TFakeDiscoveryServiceProxy
{ };

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ENodeShutdownSimulationMode,
    (IgnoreDiscoverRequests)
    (FailUserRequests)
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ISimulator)

struct ISimulator
    : public TRefCounted
{
    virtual void Shutdown(std::string_view peer) = 0;
    virtual void Restart(std::string_view peer) = 0;
    virtual bool IsAvailable(std::string_view peer) = 0;
    virtual void SetSimulationMode(ENodeShutdownSimulationMode mode) = 0;
    virtual std::optional<std::vector<TMasterInfo>> GetMasters(std::string_view address) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISimulator)

////////////////////////////////////////////////////////////////////////////////

ISimulatorPtr InitializeSimulator(std::vector<TMasterInfo> peers);

NRpc::IChannelFactoryPtr MakeFakeChannelFactory();

std::string GetAddress(const NRpc::IChannelPtr& channel);

TFuture<std::string> PingGetAddress(const NRpc::IChannelPtr& channel);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTests

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <>
struct TDiscoveryServiceTraits<NTests::TFakeDiscoveryServiceProxy>
{
    static TFuture<TGetMastersResult> GetMasters(
        const NRpc::IChannelPtr& channel,
        const NLogging::TLogger& logger,
        TDuration timeout);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative
