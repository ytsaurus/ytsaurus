#pragma once

#include "subject.h"

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/ref_tracked.h>

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

class TGroup
    : public TSubject
    , public NYT::TRefTracked<TGroup>
{
public:
    static constexpr EObjectType Type = EObjectType::Group;

    TGroup(
        const TObjectId& id,
        IObjectTypeHandler* typeHandler,
        ISession* session);

    virtual EObjectType GetType() const override;

    using TSpec = NYP::NClient::NApi::NProto::TGroupSpec;
    static const TScalarAttributeSchema<TGroup, TSpec> SpecSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TSpec>, Spec);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP
