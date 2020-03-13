#pragma once

#include "object.h"

#include <yp/server/objects/proto/autogen.pb.h>

#include <yp/client/api/proto/persistent_volume_claim.pb.h>

#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TPersistentVolumeClaim
    : public TObject
    , public NYT::TRefTracked<TPersistentVolumeClaim>
{
public:
    static constexpr EObjectType Type = EObjectType::PersistentVolumeClaim;

    TPersistentVolumeClaim(
        const TObjectId& id,
        IObjectTypeHandler* typeHandler,
        ISession* session);

    virtual EObjectType GetType() const override;

    static const TScalarAttributeSchema<TPersistentVolumeClaim, EPersistentVolumeClaimKind> KindSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<EPersistentVolumeClaimKind>, Kind);

    class TSpec
    {
    public:
        explicit TSpec(TPersistentVolumeClaim* disk);

        using TEtc = NProto::TPersistentVolumeClaimSpecEtc;
        static const TScalarAttributeSchema<TPersistentVolumeClaim, TEtc> EtcSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TPersistentVolumeClaim* claim);

        using TBoundVolumeAttribute = TOneToOneAttribute<TPersistentVolumeClaim, TPersistentVolume>;
        static const TBoundVolumeAttribute::TSchema BoundVolumeSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TBoundVolumeAttribute, BoundVolume);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
