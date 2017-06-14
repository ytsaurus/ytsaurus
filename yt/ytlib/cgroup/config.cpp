#include "config.h"
#include "private.h"

namespace NYT {
namespace NCGroup {

////////////////////////////////////////////////////////////////////////////////

bool TCGroupConfig::IsCGroupSupported(const TString& cgroupType) const
{
    auto it = std::find_if(
        SupportedCGroups.begin(),
        SupportedCGroups.end(),
        [&] (const TString& type) {
            return type == cgroupType;
        });
    return it != SupportedCGroups.end();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCGroup
} // namespace NYT
