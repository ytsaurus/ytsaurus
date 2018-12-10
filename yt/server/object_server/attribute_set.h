#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/core/misc/property.h>

#include <yt/core/yson/string.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

class TAttributeSet
{
public:
    using TAttributeMap = THashMap<TString, NYson::TYsonString>;
    DEFINE_BYREF_RW_PROPERTY(TAttributeMap, Attributes);

public:
    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
