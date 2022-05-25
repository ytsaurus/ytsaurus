#include "backup.h"

#include <yt/yt/core/misc/guid.h>
#include <yt/yt/core/misc/serialize.h>

namespace NYT::NTabletClient {

////////////////////////////////////////////////////////////////////////////////

void TTableReplicaBackupDescriptor::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, ReplicaId);
    Persist(context, Mode);
    Persist(context, ReplicaPath);
}

void ToProto(
    NProto::TTableReplicaBackupDescriptor* protoDescriptor,
    const TTableReplicaBackupDescriptor& descriptor)
{
    using NYT::ToProto;

    ToProto(protoDescriptor->mutable_replica_id(), descriptor.ReplicaId);
    protoDescriptor->set_replica_mode(static_cast<int>(descriptor.Mode));
    protoDescriptor->set_replica_path(descriptor.ReplicaPath);
}

void FromProto(
    TTableReplicaBackupDescriptor* descriptor,
    const NProto::TTableReplicaBackupDescriptor& protoDescriptor)
{
    using NYT::FromProto;

    FromProto(&descriptor->ReplicaId, protoDescriptor.replica_id());
    descriptor->Mode = static_cast<ETableReplicaMode>(protoDescriptor.replica_mode());
    descriptor->ReplicaPath = protoDescriptor.replica_path();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletClient
