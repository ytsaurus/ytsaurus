#pragma once

#include "object.h"

#include <yp/server/nodes/public.h>

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/ref_tracked.h>

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

class TResource
    : public TObject
    , public TRefTracked<TResource>
{
public:
    static constexpr EObjectType Type = EObjectType::Resource;

    TResource(
        const TObjectId& id,
        const TObjectId& nodeId,
        IObjectTypeHandler* typeHandler,
        ISession* session);

    virtual EObjectType GetType() const override;

    using TNodeAttribute = TParentAttribute<TNode>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TNodeAttribute, Node);

    static const TScalarAttributeSchema<TResource, EResourceKind> KindSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<EResourceKind>, Kind);

    using TSpec = NYP::NClient::NApi::NProto::TResourceSpec;
    static const TScalarAttributeSchema<TResource, TSpec> SpecSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TSpec>, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TResource* resource);

        using TScheduledAllocations = std::vector<NYP::NClient::NApi::NProto::TResourceStatus_TAllocation>;
        static const TScalarAttributeSchema<TResource, TScheduledAllocations> ScheduledAllocationsSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TScheduledAllocations>, ScheduledAllocations);

        using TActualAllocations = std::vector<NYP::NClient::NApi::NProto::TResourceStatus_TAllocation>;
        static const TScalarAttributeSchema<TResource, TActualAllocations> ActualAllocationsSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TActualAllocations>, ActualAllocations);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP
