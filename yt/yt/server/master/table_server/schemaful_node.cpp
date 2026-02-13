#include "schemaful_node.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/misc/serialize.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

const NTableClient::TColumnStableNameToConstraintMap& TSchemafulNode::Constraints() const
{
    static const NTableClient::TColumnStableNameToConstraintMap EmptyColumnStableNameToConstraintMap;
    if (!Constraints_) {
        return EmptyColumnStableNameToConstraintMap;
    }

    return *Constraints_;
}

void TSchemafulNode::SetConstraints(NTableClient::TColumnStableNameToConstraintMap constraints)
{
    if (constraints.empty()) {
        Constraints_.reset();
    } else {
        Constraints_ = std::make_unique<NTableClient::TColumnStableNameToConstraintMap>(std::move(constraints));
    }
}

void TSchemafulNode::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;

    Save(context, Schema_);
    Save(context, SchemaMode_);
    Save(context, Constraints());
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
    // COMPAT(cherepashka)
    if (context.GetVersion() >=  NCellMaster::EMasterReign::AddConstraintsIntoMasterSnapshot) {
        SetConstraints(Load<NTableClient::TColumnStableNameToConstraintMap>(context));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
