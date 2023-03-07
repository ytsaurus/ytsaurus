#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NCGroup {

////////////////////////////////////////////////////////////////////////////////

class TCGroupConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    std::vector<TString> SupportedCGroups;

    TCGroupConfig()
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

    bool IsCGroupSupported(const TString& cgroupType) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCGroup
