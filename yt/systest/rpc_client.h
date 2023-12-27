#pragma once

#include <yt/yt/client/api/config.h>
#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

#include <yt/yt/library/program/config.h>
#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/helpers.h>

#include <yt/systest/config.h>

namespace NYT::NTest {

NApi::IClientPtr CreateRpcClient(const TNetworkConfig& config);

}  // namespace NYT::NTest
