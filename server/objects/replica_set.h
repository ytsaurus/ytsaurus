#pragma once

#include "object.h"

#include <yp/server/objects/proto/autogen.pb.h>

#include <yp/client/api/proto/data_model.pb.h>
#include <yp/client/api/proto/replica_set.pb.h>

#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TReplicaSet
    : public TObject
    , public NYT::TRefTracked<TReplicaSet>
{
public:
    static constexpr EObjectType Type = EObjectType::ReplicaSet;

    TReplicaSet(
        const TObjectId& id,
        IObjectTypeHandler* typeHandler,
        ISession* session);

    virtual EObjectType GetType() const override;

    class TSpec
    {
    public:
        explicit TSpec(TReplicaSet* replicaSet);

        static const TManyToOneAttributeSchema<TReplicaSet, TAccount> AccountSchema;
        using TAccountAttribute = TManyToOneAttribute<TReplicaSet, TAccount>;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TAccountAttribute, Account);

        using TOther = NProto::TReplicaSetSpecOther;
        static const TScalarAttributeSchema<TReplicaSet, TOther> OtherSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TOther>, Other);
    };
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    using TResourceCacheAttribute = TChildrenAttribute<TResourceCache>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TResourceCacheAttribute, ResourceCache);

    using TStatus = NYP::NClient::NApi::NProto::TReplicaSetStatus;
    static const TScalarAttributeSchema<TReplicaSet, TStatus> StatusSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TStatus>, Status);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
