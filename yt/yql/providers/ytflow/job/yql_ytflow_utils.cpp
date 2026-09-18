#include "yql_ytflow_utils.h"

#include <util/string/cast.h>
#include <util/system/compiler.h>


namespace NYql::NYtflow {

std::optional<double> TryGetCpuToVCpuFactor()
{
    try {
        static const auto ytEnv = "YT_CPU_TO_VCPU_FACTOR";
        if (const char* cpuToVCpuFactorFromYt = std::getenv(ytEnv)) {
            double cpuToVCpuFactor = FromString(cpuToVCpuFactorFromYt);
            return cpuToVCpuFactor;
        }
    } catch (const std::exception& ex) {
        Y_UNUSED(ex);
    }

    return std::nullopt;
}

} // namespace NYql::NYtflow
