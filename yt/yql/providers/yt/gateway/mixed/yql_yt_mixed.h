#pragma once

#include <yt/yql/providers/yt/provider/yql_yt_gateway.h>
#include <yt/yql/providers/yt/gateway/native/yql_yt_native.h>

namespace NYql {

IYtGateway::TPtr CreateYtMixedGateway(const TYtNativeServices& services);

} // namspace NYql
