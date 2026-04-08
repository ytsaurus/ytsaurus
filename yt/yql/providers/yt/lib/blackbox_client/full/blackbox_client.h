#pragma once

#include <yt/yql/providers/yt/lib/blackbox_client/blackbox_client.h>

namespace NYql {

IBlackboxClient::TPtr CreateBlackboxClient(const ITvmClient::TPtr& tvmClient, const TYtGatewayConfig& ytGatewayConfig);

}; // namespace NYql
