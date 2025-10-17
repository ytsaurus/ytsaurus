#include "schemaful_node.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/misc/serialize.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

void TSchemafulNode::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;

    Save(context, Schema_);
    Save(context, SchemaMode_);
}

void TSchemafulNode::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;

    Load(context, Schema_);
    Load(context, SchemaMode_);
    // COMPAT(theevilbird)
    if (context.GetVersion() >= NCellMaster::EMasterReign::AddSchemaRevision &&
        context.GetVersion() < NCellMaster::EMasterReign::RemoveSchemaRevision) {
        NHydra::TRevision tmpRevision;
        Load(context, tmpRevision);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
