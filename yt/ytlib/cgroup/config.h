#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NCGroup {

////////////////////////////////////////////////////////////////////////////////

class TCGroupConfig
    : public NYTree::TYsonSerializable
{
public:
    bool EnableCGroups;
    std::vector<Stroka> SupportedCGroups;

    TCGroupConfig()
    {
        RegisterParameter("enable_cgroups", EnableCGroups)
            .Default(true);
        RegisterParameter("supported_cgroups", SupportedCGroups)
            .Default();

        RegisterValidator([&] () {
            for (const auto& type : SupportedCGroups) {
                if (!IsValidCGroupType(type)) {
                    THROW_ERROR_EXCEPTION("Invalid cgroup type %Qv", type);
                }
            }
        });
    }

    bool IsCGroupSupported(const Stroka& cgroupType) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCGroup
} // namespace NYT