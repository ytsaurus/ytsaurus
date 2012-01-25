#pragma once

#include "common.h"
#include "id.h"

#include <ytlib/ytree/ytree_fwd.h>
#include <ytlib/misc/property.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace NObjectServer {

class TAttributeSet
{
    typedef yhash_map<Stroka, NYTree::TYPath> TAttributeMap;
    DEFINE_BYREF_RW_PROPERTY(TAttributeMap, Attributes);
    
public:
    TAutoPtr<TAttributeSet> Clone() const;

    void Save(TOutputStream* output) const;
    static TAutoPtr<TAttributeSet> Load(const TVersionedObjectId& id, TInputStream* input);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
