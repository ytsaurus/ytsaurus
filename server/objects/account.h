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

        static const TManyToOneAttributeSchema<TAccount, TAccount> ParentSchema;
        using TParentAttribute = TManyToOneAttribute<TAccount, TAccount>;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TParentAttribute, Parent);

        using TEtc = NProto::TAccountSpecEtc;
        static const TScalarAttributeSchema<TAccount, TEtc> EtcSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    static const TOneToManyAttributeSchema<TAccount, TPodSet> PodSetsSchema;
    using TPodSets = TOneToManyAttribute<TAccount, TPodSet>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TPodSets, PodSets);

    // NB: Only pods explicitly overriding their podsets' account are present here.
    static const TOneToManyAttributeSchema<TAccount, TPod> PodsSchema;
    using TPods = TOneToManyAttribute<TAccount, TPod>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TPods, Pods);

    static const TOneToManyAttributeSchema<TAccount, TReplicaSet> ReplicaSetsSchema;
    using TReplicaSets = TOneToManyAttribute<TAccount, TReplicaSet>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TReplicaSets, ReplicaSets);

    static const TOneToManyAttributeSchema<TAccount, TMultiClusterReplicaSet> MultiClusterReplicaSetsSchema;
    using TMultiClusterReplicaSets = TOneToManyAttribute<TAccount, TMultiClusterReplicaSet>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TMultiClusterReplicaSets, MultiClusterReplicaSets);

    static const TOneToManyAttributeSchema<TAccount, TStage> StagesSchema;
    using TStages = TOneToManyAttribute<TAccount, TStage>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStages, Stages);

    static const TOneToManyAttributeSchema<TAccount, TAccount> ChildrenSchema;
    using TChildrenAttribute = TOneToManyAttribute<TAccount, TAccount>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TChildrenAttribute, Children);

    static const TOneToManyAttributeSchema<TAccount, TProject> ProjectsSchema;
    using TProjects = TOneToManyAttribute<TAccount, TProject>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TProjects, Projects);

    virtual bool IsBuiltin() const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
