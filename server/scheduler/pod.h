#pragma once

#include "object.h"

#include <yp/server/objects/proto/objects.pb.h>

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

namespace NYP {
namespace NServer {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TPod
    : public TObject
    , public NYT::TRefTracked<TPod>
{
public:
    TPod(
        const TObjectId& id,
        TPodSet* podSet,
        NYT::NYson::TYsonString labels,
        TNode* node,
        NServer::NObjects::NProto::TPodSpecOther specOther,
        NServer::NObjects::NProto::TPodStatusOther statusOther);

    DEFINE_BYVAL_RO_PROPERTY(TPodSet*, PodSet);
    DEFINE_BYVAL_RO_PROPERTY(TNode*, Node);
    DEFINE_BYREF_RO_PROPERTY(NServer::NObjects::NProto::TPodSpecOther, SpecOther);
    DEFINE_BYREF_RO_PROPERTY(NServer::NObjects::NProto::TPodStatusOther, StatusOther);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NServer
} // namespace NYP
