#pragma once

#include "public.h"

#include <core/misc/nullable.h>
#include <core/misc/property.h>

#include <core/yson/string.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

class TAttributeSet
{
public:
    using TAttributeMap = yhash_map<Stroka, TNullable<NYson::TYsonString>>;
    DEFINE_BYREF_RW_PROPERTY(TAttributeMap, Attributes);

public:
    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
