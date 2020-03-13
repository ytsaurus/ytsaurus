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

        using TNodeSegmentAttribute = TManyToOneAttribute<TPodSet, TNodeSegment>;
        static const TNodeSegmentAttribute::TSchema NodeSegmentSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TNodeSegmentAttribute, NodeSegment);

        using TAccountAttribute = TManyToOneAttribute<TPodSet, TAccount>;
        static const TAccountAttribute::TSchema AccountSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TAccountAttribute, Account);

        using TPodDisruptionBudgetAttribute = TManyToOneAttribute<TPodSet, TPodDisruptionBudget>;
        static const TPodDisruptionBudgetAttribute::TSchema PodDisruptionBudgetSchema;
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
