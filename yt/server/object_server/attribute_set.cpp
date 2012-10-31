#include "stdafx.h"
#include "attribute_set.h"

#include <ytlib/misc/serialize.h>

#include <server/cell_master/serialization_context.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

TAttributeSet::TAttributeSet()
{ }

TAttributeSet::TAttributeSet(const TVersionedObjectId&)
{ }

void TAttributeSet::Save(const NCellMaster::TSaveContext& context) const
{
    auto* output = context.GetOutput();
    SaveMap(output, Attributes_);
}

void TAttributeSet::Load(const NCellMaster::TLoadContext& context)
{
    auto* input = context.GetInput();
    LoadMap(input, Attributes_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
