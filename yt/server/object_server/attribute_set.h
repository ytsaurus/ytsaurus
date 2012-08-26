#pragma once

#include "public.h"

#include <ytlib/misc/nullable.h>
#include <ytlib/misc/property.h>

#include <ytlib/ytree/public.h>
#include <ytlib/ytree/yson_string.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

class TAttributeSet
{
    typedef yhash_map<Stroka, TNullable<NYTree::TYsonString> > TAttributeMap;
    DEFINE_BYREF_RW_PROPERTY(TAttributeMap, Attributes);
    
public:
    TAttributeSet();
    explicit TAttributeSet(const TVersionedObjectId& id); // Just for meta map

    void Save(TOutputStream* output) const;
    void Load(const NCellMaster::TLoadContext& context, TInputStream* input);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
