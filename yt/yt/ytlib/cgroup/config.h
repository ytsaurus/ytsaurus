#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NCGroup {

////////////////////////////////////////////////////////////////////////////////

class TCGroupConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    std::vector<TString> SupportedCGroups;

    TCGroupConfig();

    bool IsCGroupSupported(const TString& cgroupType) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCGroup
