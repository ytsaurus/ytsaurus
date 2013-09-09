#pragma once

#include "public.h"

#include <core/misc/nullable.h>
#include <core/misc/property.h>
#include <core/misc/ref_tracked.h>

#include <core/ytree/public.h>
#include <core/ytree/yson_string.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

class TAttributeSet
    : public TRefTracked<TAttributeSet>
{
    typedef yhash_map<Stroka, TNullable<NYTree::TYsonString> > TAttributeMap;
    DEFINE_BYREF_RW_PROPERTY(TAttributeMap, Attributes);

public:
    TAttributeSet();
    explicit TAttributeSet(const TVersionedObjectId& id); // Just for meta map

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
