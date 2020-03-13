#pragma once

#include "object.h"

#include <yp/server/objects/proto/autogen.pb.h>

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TAccount
    : public TObject
    , public NYT::TRefTracked<TAccount>
{
public:
    static constexpr EObjectType Type = EObjectType::Account;

    TAccount(
        const TObjectId& id,
        IObjectTypeHandler* typeHandler,
        ISession* session);

    virtual EObjectType GetType() const override;

    using TStatus = NClient::NApi::NProto::TAccountStatus;
    static const TScalarAttributeSchema<TAccount, TStatus> StatusSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TStatus>, Status);

    class TSpec
    {
    public:
        explicit TSpec(TAccount* account);

        using TParentAttribute = TManyToOneAttribute<TAccount, TAccount>;
        static const TParentAttribute::TSchema ParentSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TParentAttribute, Parent);

        using TEtc = NProto::TAccountSpecEtc;
        static const TScalarAttributeSchema<TAccount, TEtc> EtcSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    using TPodSetsAttribute = TOneToManyAttribute<TAccount, TPodSet>;
    static const TPodSetsAttribute::TSchema PodSetsSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TPodSetsAttribute, PodSets);

    // NB: Only pods explicitly overriding their podsets' account are present here.
    using TPodsAttribute = TOneToManyAttribute<TAccount, TPod>;
    static const TPodsAttribute::TSchema PodsSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TPodsAttribute, Pods);

    using TReplicaSetsAttribute = TOneToManyAttribute<TAccount, TReplicaSet>;
    static const TReplicaSetsAttribute::TSchema ReplicaSetsSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TReplicaSetsAttribute, ReplicaSets);

    using TMultiClusterReplicaSetsAttribute = TOneToManyAttribute<TAccount, TMultiClusterReplicaSet>;
    static const TMultiClusterReplicaSetsAttribute::TSchema MultiClusterReplicaSetsSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TMultiClusterReplicaSetsAttribute, MultiClusterReplicaSets);

    using TStagesAttribute = TOneToManyAttribute<TAccount, TStage>;
    static const TStagesAttribute::TSchema StagesSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStagesAttribute, Stages);

    using TChildrenAttribute = TOneToManyAttribute<TAccount, TAccount>;
    static const TChildrenAttribute::TSchema ChildrenSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TChildrenAttribute, Children);

    using TProjectsAttribute = TOneToManyAttribute<TAccount, TProject>;
    static const TProjectsAttribute::TSchema ProjectsSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TProjectsAttribute, Projects);

    virtual bool IsBuiltin() const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
