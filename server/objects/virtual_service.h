#pragma once

#include "object.h"

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/ref_tracked.h>

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

class TVirtualService
    : public TObject
    , public NYT::TRefTracked<TVirtualService>
{
public:
    static constexpr EObjectType Type = EObjectType::VirtualService;

    TVirtualService(
        const TObjectId& id,
        IObjectTypeHandler* typeHandler,
        ISession* session);

    virtual EObjectType GetType() const override;

    using TSpec = NYP::NClient::NApi::NProto::TVirtualServiceSpec;
    static const TScalarAttributeSchema<TVirtualService, TSpec> SpecSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TSpec>, Spec);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP
