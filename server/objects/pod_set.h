#pragma once

#include "object.h"

#include <yp/server/objects/proto/autogen.pb.h>

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TPodSet
    : public TObject
    , public NYT::TRefTracked<TPodSet>
{
public:
    static constexpr EObjectType Type = EObjectType::PodSet;

    TPodSet(
        const TObjectId& id,
        IObjectTypeHandler* typeHandler,
        ISession* session);

    virtual EObjectType GetType() const override;

    using TPodsAttribute = TChildrenAttribute<TPod>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TPodsAttribute, Pods);

    using TDynamicResourcesAttribute = TChildrenAttribute<TDynamicResource>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TDynamicResourcesAttribute, DynamicResources);

    using TResourceCacheAttribute = TChildrenAttribute<TResourceCache>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TResourceCacheAttribute, ResourceCache);

    class TSpec
    {
    public:
        explicit TSpec(TPodSet* podSet);

        using TAntiaffinityConstraints = std::vector<NClient::NApi::NProto::TAntiaffinityConstraint>;
        static const TScalarAttributeSchema<TPodSet, TAntiaffinityConstraints> AntiaffinityConstraintsSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TAntiaffinityConstraints>, AntiaffinityConstraints);

        static const TManyToOneAttributeSchema<TPodSet, TNodeSegment> NodeSegmentSchema;
        using TNodeSegmentAttribute = TManyToOneAttribute<TPodSet, TNodeSegment>;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TNodeSegmentAttribute, NodeSegment);

        static const TManyToOneAttributeSchema<TPodSet, TAccount> AccountSchema;
        using TAccountAttribute = TManyToOneAttribute<TPodSet, TAccount>;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TAccountAttribute, Account);

        static const TManyToOneAttributeSchema<TPodSet, TPodDisruptionBudget> PodDisruptionBudgetSchema;
        using TPodDisruptionBudgetAttribute = TManyToOneAttribute<TPodSet, TPodDisruptionBudget>;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TPodDisruptionBudgetAttribute, PodDisruptionBudget);

        static const TScalarAttributeSchema<TPodSet, TString> NodeFilterSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TString>, NodeFilter);

        using TEtc = NProto::TPodSetSpecEtc;
        static const TScalarAttributeSchema<TPodSet, TEtc> EtcSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
