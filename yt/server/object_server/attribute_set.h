#pragma once

#include "public.h"

#include <core/misc/nullable.h>
#include <core/misc/property.h>
#include <core/misc/ref_tracked.h>

#include <core/ytree/public.h>
#include <core/ytree/yson_string.h>

#include <server/hydra/entity_map.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

class TAttributeSet
    : public NHydra::TEntityBase
    , public TRefTracked<TAttributeSet>
{
public:
    typedef yhash_map<Stroka, TNullable<NYTree::TYsonString> > TAttributeMap;
    DEFINE_BYREF_RW_PROPERTY(TAttributeMap, Attributes);

public:
    TAttributeSet();
    explicit TAttributeSet(const TVersionedObjectId& id); // for serialization only

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
