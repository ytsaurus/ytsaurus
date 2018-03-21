#pragma once

#include "object.h"

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/ref_tracked.h>

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

class TNetworkProject
    : public TObject
    , public NYT::TRefTracked<TNetworkProject>
{
public:
    static constexpr EObjectType Type = EObjectType::NetworkProject;

    TNetworkProject(
        const TObjectId& id,
        IObjectTypeHandler* typeHandler,
        ISession* session);

    virtual EObjectType GetType() const override;

    class TSpec
    {
    public:
        explicit TSpec(TNetworkProject* project);

        static const TScalarAttributeSchema<TNetworkProject, ui32> ProjectIdSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<ui32>, ProjectId);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP
