#pragma once

#include "common.h"
#include "id.h"

#include <ytlib/ytree/public.h>
#include <ytlib/misc/property.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

class TAttributeSet
{
    typedef yhash_map<Stroka, NYTree::TYPath> TAttributeMap;
    DEFINE_BYREF_RW_PROPERTY(TAttributeMap, Attributes);
    
public:
    TAttributeSet();
    TAttributeSet(const TVersionedObjectId&); // Just for meta map

    void Save(TOutputStream* output) const;
    void Load(TInputStream* input, TVoid context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
