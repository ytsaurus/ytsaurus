#include "config.h"
#include "private.h"

namespace NYT {
namespace NCGroup {

////////////////////////////////////////////////////////////////////////////////

bool TCGroupConfig::IsCGroupSupported(const Stroka& cgroupType) const
{
    auto it = std::find_if(
        SupportedCGroups.begin(),
        SupportedCGroups.end(),
        [&] (const Stroka& type) {
            return type == cgroupType;
        });
    return it != SupportedCGroups.end();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCGroup
} // namespace NYT
