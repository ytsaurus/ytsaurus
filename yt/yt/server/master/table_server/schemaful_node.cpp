#include "schemaful_node.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/phoenix/type_def.h>

namespace NYT::NTableServer {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ECompatTableSchemaMode,
    ((Weak)      (0))
    ((Strong)    (1))
);

////////////////////////////////////////////////////////////////////////////////

const NTableClient::TColumnStableNameToConstraintMap& TSchemafulNode::GetConstraints() const
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

void TSchemafulNode::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Schema_);

    // COMPAT(cherepashka)
    registrar.template VirtualField<2>("SchemaMode_", [] (TThis* this_, auto& context) {
        if (context.GetVersion() < NCellMaster::EMasterReign::ReduceSchemaModeAndOptimizeFor) {
            this_->SchemaMode_ = CheckedEnumCast<NTableClient::ETableSchemaMode>(NYT::Load<ECompatTableSchemaMode>(context));
        } else {
            NYT::Load(context, this_->SchemaMode_);
        }
    }, [] (const TThis* this_, auto& context) {
        NYT::Save(context, this_->SchemaMode_);
    })();

    // COMPAT(theevilbird)
    PHOENIX_REGISTER_DELETED_FIELD(3, NHydra::TRevision, SchemaRevision_, NCellMaster::EMasterReign::RemoveSchemaRevision);

    // COMPAT(cherepashka)
    registrar.template PropertyField<4, &TThis::GetConstraints, &TThis::SetConstraints>("Constraints_")
        .SinceVersion(NCellMaster::EMasterReign::AddConstraintsIntoMasterSnapshot)();
}

PHOENIX_DEFINE_TYPE(TSchemafulNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer
