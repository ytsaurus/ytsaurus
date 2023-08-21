#include "public.h"

#include "config.h"

#include <yt/yt/library/tvm/service/tvm_service.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

struct ITvmBridge
    : public IDynamicTvmService
{
    virtual TFuture<void> EnsureDestinationServiceIds(const std::vector<TTvmId>& serviceIds) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITvmBridge)

////////////////////////////////////////////////////////////////////////////////

//! Creates a TVM bridge.
//!
//! TVM bridges are used to relay TVM tickets to components which cannot maintain
//! secrets to use regular TVM API. A notable example is job proxy, which runs inside a
//! container, and it's unsafe to expose the secrets.
//!
//! To fetch tickets, TVM uses a TVM bridge service. The latter can pass tickets from
//! the real TVM API.
ITvmBridgePtr CreateTvmBridge(
    IInvokerPtr invoker,
    NRpc::IChannelPtr channel,
    TTvmBridgeConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
