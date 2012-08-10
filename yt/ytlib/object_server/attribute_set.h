#pragma once

#include "id.h"

#include <ytlib/ytree/public.h>
#include <ytlib/ytree/yson_string.h>
#include <ytlib/misc/nullable.h>
#include <ytlib/cell_master/public.h>

#include <ytlib/misc/property.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

class TAttributeSet
{
    typedef std::unordered_map<Stroka, TNullable<NYTree::TYsonString> > TAttributeMap;
    DEFINE_BYREF_RW_PROPERTY(TAttributeMap, Attributes);
    
public:
    TAttributeSet();
    TAttributeSet(const TVersionedObjectId&); // Just for meta map

    void Save(TOutputStream* output) const;
    void Load(const NCellMaster::TLoadContext& context, TInputStream* input);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
