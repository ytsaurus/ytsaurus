#pragma once

#include "object.h"

#include <yp/server/objects/proto/autogen.pb.h>

#include <yp/client/api/proto/persistent_disk.pb.h>

#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TPersistentDisk
    : public TObject
    , public NYT::TRefTracked<TPersistentDisk>
{
public:
    static constexpr EObjectType Type = EObjectType::PersistentDisk;

    TPersistentDisk(
        const TObjectId& id,
        IObjectTypeHandler* typeHandler,
        ISession* session);

    virtual EObjectType GetType() const override;

    static const TScalarAttributeSchema<TPersistentDisk, EPersistentDiskKind> KindSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<EPersistentDiskKind>, Kind);

    class TSpec
    {
    public:
        explicit TSpec(TPersistentDisk* disk);

        using TEtc = NProto::TPersistentDiskSpecEtc;
        static const TScalarAttributeSchema<TPersistentDisk, TEtc> EtcSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TPersistentDisk* disk);

        using TAttachedToNodeAttribute = TManyToOneAttribute<TPersistentDisk, TNode>;
        static const TAttachedToNodeAttribute::TSchema AttachedToNodeSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TAttachedToNodeAttribute, AttachedToNode);

        using TEtc = NProto::TPersistentDiskStatusEtc;
        static const TScalarAttributeSchema<TPersistentDisk, TEtc> EtcSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);

    using TVolumesAttribute = TChildrenAttribute<TPersistentVolume>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TVolumesAttribute, Volumes);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
