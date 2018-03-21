#pragma once

#include "object.h"

#include <yp/server/nodes/public.h>

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/ref_tracked.h>

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

class TNodeSegment
    : public TObject
    , public TRefTracked<TNodeSegment>
{
public:
    static constexpr EObjectType Type = EObjectType::NodeSegment;

    TNodeSegment(
        const TObjectId& id,
        IObjectTypeHandler* typeHandler,
        ISession* session);

    virtual EObjectType GetType() const override;

    using TSpec = NClient::NApi::NProto::TNodeSegmentSpec;
    static const TScalarAttributeSchema<TNodeSegment, TSpec> SpecSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TSpec>, Spec);

    static const TOneToManyAttributeSchema<TNodeSegment, TPodSet> PodSetsSchema;
    using TPodSets = TOneToManyAttribute<TNodeSegment, TPodSet>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TPodSets, PodSets);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP
