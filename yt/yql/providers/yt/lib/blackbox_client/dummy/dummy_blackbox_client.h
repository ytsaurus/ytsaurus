#pragma once

#include <yt/yql/providers/yt/lib/blackbox_client/blackbox_client.h>

namespace NYql {

IBlackboxClient::TPtr CreateDummyBlackboxClient();

}; // namespace NYql
