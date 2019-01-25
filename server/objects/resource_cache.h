#pragma once

#include "object.h"

#include <yp/client/api/proto/data_model.pb.h>
#include <yp/client/api/proto/resource_cache.pb.h>

#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TResourceCache
    : public TObject
    , public NYT::TRefTracked<TResourceCache>
{
public:
    static constexpr EObjectType Type = EObjectType::ResourceCache;

    TResourceCache(
        const TObjectId& id,
        const TObjectId& replicaSetId,
        IObjectTypeHandler* typeHandler,
        ISession* session);

    virtual EObjectType GetType() const override;

    using TReplicaSetAttribute = TParentAttribute<TReplicaSet>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TReplicaSetAttribute, ReplicaSet);

    using TSpec = NYP::NClient::NApi::NProto::TResourceCacheSpec;
    static const TScalarAttributeSchema<TResourceCache, TSpec> SpecSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TSpec>, Spec);

    using TStatus = NYP::NClient::NApi::NProto::TResourceCacheStatus;
    static const TScalarAttributeSchema<TResourceCache, TStatus> StatusSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TStatus>, Status);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
