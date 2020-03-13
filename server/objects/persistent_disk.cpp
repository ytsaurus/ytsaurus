#include "persistent_disk.h"
#include "persistent_volume.h"
#include "node.h"
#include "db_schema.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TPersistentDisk, TPersistentDisk::TSpec::TEtc> TPersistentDisk::TSpec::EtcSchema{
    &PersistentDisksTable.Fields.Spec_Etc,
    [] (TPersistentDisk* disk) { return &disk->Spec().Etc(); }
};

TPersistentDisk::TSpec::TSpec(TPersistentDisk* disk)
    : Etc_(disk, &EtcSchema)
{ }

////////////////////////////////////////////////////////////////////////////////

const TManyToOneAttributeSchema<TPersistentDisk, TNode> TPersistentDisk::TStatus::AttachedToNodeSchema{
    &PersistentDisksTable.Fields.Status_AttachedToNodeId,
    [] (TPersistentDisk* disk) { return &disk->Status().AttachedToNode(); },
    [] (TNode* node) { return &node->Status().AttachedPersistentDisks(); }
};

const TScalarAttributeSchema<TPersistentDisk, TPersistentDisk::TStatus::TEtc> TPersistentDisk::TStatus::EtcSchema{
    &PersistentDisksTable.Fields.Status_Etc,
    [] (TPersistentDisk* disk) { return &disk->Status().Etc(); }
};

TPersistentDisk::TStatus::TStatus(TPersistentDisk* disk)
    : AttachedToNode_(disk, &AttachedToNodeSchema)
    , Etc_(disk, &EtcSchema)
{ }

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TPersistentDisk, EPersistentDiskKind> TPersistentDisk::KindSchema{
    &PersistentDisksTable.Fields.Meta_Kind,
    [] (TPersistentDisk* disk) { return &disk->Kind(); }
};

TPersistentDisk::TPersistentDisk(
    const TObjectId& id,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(id, TObjectId(), typeHandler, session)
    , Kind_(this, &KindSchema)
    , Spec_(this)
    , Status_(this)
    , Volumes_(this)
{ }

EObjectType TPersistentDisk::GetType() const
{
    return EObjectType::PersistentDisk;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

