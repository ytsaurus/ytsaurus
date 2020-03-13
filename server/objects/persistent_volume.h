#pragma once

#include "object.h"

#include <yp/server/objects/proto/autogen.pb.h>

#include <yp/client/api/proto/persistent_volume.pb.h>

#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TPersistentVolume
    : public TObject
    , public NYT::TRefTracked<TPersistentVolume>
{
public:
    static constexpr EObjectType Type = EObjectType::PersistentVolume;

    TPersistentVolume(
        const TObjectId& id,
        const TObjectId& diskId,
        IObjectTypeHandler* typeHandler,
        ISession* session);

    virtual EObjectType GetType() const override;

    using TPersistentDiskAttribute = TParentAttribute<TPersistentDisk>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TPersistentDiskAttribute, Disk);

    static const TScalarAttributeSchema<TPersistentVolume, EPersistentVolumeKind> KindSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<EPersistentVolumeKind>, Kind);

    class TSpec
    {
    public:
        explicit TSpec(TPersistentVolume* disk);

        using TEtc = NProto::TPersistentVolumeSpecEtc;
        static const TScalarAttributeSchema<TPersistentVolume, TEtc> EtcSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TEtc>, Etc);

        NTransactionClient::TTimestamp LoadTimestamp() const;
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TPersistentVolume* disk);

        using TBoundClaimAttribute = TOneToOneAttribute<TPersistentVolume, TPersistentVolumeClaim>;
        static const TBoundClaimAttribute::TSchema BoundClaimSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TBoundClaimAttribute, BoundClaim);

        using TMountedToPodAttribute = TManyToOneAttribute<TPersistentVolume, TPod>;
        static const TMountedToPodAttribute::TSchema MountedToPodSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TMountedToPodAttribute, MountedToPod);

        using TEtc = NProto::TPersistentVolumeStatusEtc;
        static const TScalarAttributeSchema<TPersistentVolume, TEtc> EtcSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
