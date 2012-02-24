#include "stdafx.h"
#include "attribute_set.h"

#include <ytlib/misc/serialize.h>

namespace NYT {

using namespace NCellMaster;

namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

TAttributeSet::TAttributeSet()
{ }

TAttributeSet::TAttributeSet(const TVersionedObjectId&)
{ }

void TAttributeSet::Save(TOutputStream* output) const
{
    SaveMap(output, Attributes_);
}

void TAttributeSet::Load(TInputStream* input, const TLoadContext& context)
{
    LoadMap(input, Attributes_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
