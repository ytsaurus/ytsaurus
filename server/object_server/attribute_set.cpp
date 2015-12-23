#include "attribute_set.h"

#include <yt/server/cell_master/serialize.h>

#include <yt/core/misc/common.h>
#include <yt/core/misc/serialize.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

void TAttributeSet::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, Attributes_);
}

void TAttributeSet::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    Load(context, Attributes_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
