#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NCGroup {

////////////////////////////////////////////////////////////////////////////////

class TCGroupConfig
    : public virtual NYTree::TYsonStruct
{
public:
    std::vector<TString> SupportedCGroups;

    bool IsCGroupSupported(const TString& cgroupType) const;

    REGISTER_YSON_STRUCT(TCGroupConfig);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCGroup
