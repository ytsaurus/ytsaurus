#include "coordination_helper.h"

namespace NYql {

TString ICoordinationHelper::GetGrpcHost() const
{
    const auto ip = GetIp();
    if (ip.empty()) {
        return GetHostname();
    }
    return ip.Contains(':') ? "[" + ip + "]" : ip;
}

} // namespace NYql
