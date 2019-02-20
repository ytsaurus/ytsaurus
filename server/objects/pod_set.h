#pragma once

#include "object.h"

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
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
