#include "stdafx.h"
#include "attribute_set.h"

#include <ytlib/misc/serialize.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

TAutoPtr<TAttributeSet> TAttributeSet::Clone() const
{
    TAutoPtr<TAttributeSet> result = new TAttributeSet();
    result->Attributes_ = Attributes_;
    return result;
}

void TAttributeSet::Save(TOutputStream* output) const
{
    SaveMap(output, Attributes_);
}

TAutoPtr<TAttributeSet> TAttributeSet::Load(const TVersionedObjectId& id, TInputStream* input)
{
    UNUSED(id);

    TAutoPtr<TAttributeSet> result = new TAttributeSet();
    LoadMap(input, result->Attributes_);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
