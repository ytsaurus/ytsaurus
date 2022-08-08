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

void TCGroupConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("supported_cgroups", &TThis::SupportedCGroups)
        .Default();

    registrar.Postprocessor([] (TThis* config) {
        for (const auto& type : config->SupportedCGroups) {
            if (!IsValidCGroupType(type)) {
                THROW_ERROR_EXCEPTION("Invalid cgroup type %Qv", type);
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCGroup
