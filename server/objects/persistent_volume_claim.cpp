#include "persistent_volume_claim.h"
#include "persistent_volume.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TPersistentVolumeClaim, TPersistentVolumeClaim::TSpec::TEtc> TPersistentVolumeClaim::TSpec::EtcSchema{
    &PersistentVolumeClaimsTable.Fields.Spec_Etc,
    [] (TPersistentVolumeClaim* claim) { return &claim->Spec().Etc(); }
};

TPersistentVolumeClaim::TSpec::TSpec(TPersistentVolumeClaim* claim)
    : Etc_(claim, &EtcSchema)
{ }

////////////////////////////////////////////////////////////////////////////////

const TOneToOneAttributeSchema<TPersistentVolumeClaim, TPersistentVolume> TPersistentVolumeClaim::TStatus::BoundVolumeSchema{
    &PersistentVolumeClaimsTable.Fields.Status_BoundVolumeId,
    [] (TPersistentVolumeClaim* claim) { return &claim->Status().BoundVolume(); },
    [] (TPersistentVolume* volume) { return &volume->Status().BoundClaim(); }
};

TPersistentVolumeClaim::TStatus::TStatus(TPersistentVolumeClaim* claim)
    : BoundVolume_(claim, &BoundVolumeSchema)
{ }

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TPersistentVolumeClaim, EPersistentVolumeClaimKind> TPersistentVolumeClaim::KindSchema{
    &PersistentVolumeClaimsTable.Fields.Meta_Kind,
    [] (TPersistentVolumeClaim* claim) { return &claim->Kind(); }
};

TPersistentVolumeClaim::TPersistentVolumeClaim(
    const TObjectId& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, TObjectId(), typeHandler, session)
    , Kind_(this, &KindSchema)
    , Spec_(this)
    , Status_(this)
{ }

EObjectType TPersistentVolumeClaim::GetType() const
{
    return EObjectType::PersistentVolumeClaim;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
