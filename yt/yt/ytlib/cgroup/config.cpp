#include "config.h"
#include "private.h"

namespace NYT::NCGroup {

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

TCGroupConfig::TCGroupConfig()
{
    RegisterParameter("supported_cgroups", SupportedCGroups)
        .Default();

    RegisterPostprocessor([&] () {
        for (const auto& type : SupportedCGroups) {
            if (!IsValidCGroupType(type)) {
                THROW_ERROR_EXCEPTION("Invalid cgroup type %Qv", type);
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCGroup
