#pragma once

#include <yt/yql/providers/yt/lib/blackbox_client/blackbox_client.h>
#include <yt/yql/providers/yt/lib/blackbox_client/proto/blackbox.pb.h>

namespace NYql {

IBlackboxClient::TPtr CreateBlackboxClient(const ITvmClient::TPtr& tvmClient, const TYtBBConfig& config);

}; // namespace NYql
