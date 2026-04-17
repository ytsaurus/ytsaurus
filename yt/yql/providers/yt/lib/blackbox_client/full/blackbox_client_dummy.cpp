#include "blackbox_client.h"

#include <yt/yql/providers/yt/lib/blackbox_client/dummy/dummy_blackbox_client.h>

namespace NYql {

Y_WEAK IBlackboxClient::TPtr CreateBlackboxClient(const ITvmClient::TPtr& /*tvmClient*/, const TYtGatewayConfig& /*ytGatewayConfig*/) {
    return CreateDummyBlackboxClient();
}

}; // namespace NYql
