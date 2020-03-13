#include "persistent_volume.h"
#include "persistent_volume_claim.h"
#include "persistent_disk.h"
#include "pod.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TPersistentVolume, TPersistentVolume::TSpec::TEtc> TPersistentVolume::TSpec::EtcSchema{
    &PersistentVolumesTable.Fields.Spec_Etc,
    [] (TPersistentVolume* volume) { return &volume->Spec().Etc(); }
};

NTransactionClient::TTimestamp TPersistentVolume::TSpec::LoadTimestamp() const
{
    return Etc_.LoadTimestamp();
}

TPersistentVolume::TSpec::TSpec(TPersistentVolume* disk)
    : Etc_(disk, &EtcSchema)
{ }

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TPersistentVolume, TPersistentVolume::TStatus::TEtc> TPersistentVolume::TStatus::EtcSchema{
    &PersistentVolumesTable.Fields.Status_Etc,
    [] (TPersistentVolume* volume) { return &volume->Status().Etc(); }
};

const TOneToOneAttributeSchema<TPersistentVolume, TPersistentVolumeClaim> TPersistentVolume::TStatus::BoundClaimSchema{
    &PersistentVolumesTable.Fields.Status_BoundClaimId,
    [] (TPersistentVolume* volume) { return &volume->Status().BoundClaim(); },
    [] (TPersistentVolumeClaim* claim) { return &claim->Status().BoundVolume(); }
};

const TManyToOneAttributeSchema<TPersistentVolume, TPod> TPersistentVolume::TStatus::MountedToPodSchema{
    &PersistentVolumesTable.Fields.Status_MountedToPodId,
    [] (TPersistentVolume* volume) { return &volume->Status().MountedToPod(); },
    [] (TPod* pod) { return &pod->Status().MountedPersistentVolumes(); }
};

TPersistentVolume::TStatus::TStatus(TPersistentVolume* volume)
    : BoundClaim_(volume, &BoundClaimSchema)
    , MountedToPod_(volume, &MountedToPodSchema)
    , Etc_(volume, &EtcSchema)
{ }

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TPersistentVolume, EPersistentVolumeKind> TPersistentVolume::KindSchema{
    &PersistentVolumesTable.Fields.Meta_Kind,
    [] (TPersistentVolume* volume) { return &volume->Kind(); }
};

TPersistentVolume::TPersistentVolume(
    const TObjectId& id,
    const TObjectId& diskId,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, diskId, typeHandler, session)
    , Disk_(this)
    , Kind_(this, &KindSchema)
    , Spec_(this)
    , Status_(this)
{ }

EObjectType TPersistentVolume::GetType() const
{
    return EObjectType::PersistentVolume;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
