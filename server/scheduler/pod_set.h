#pragma once

#include "object.h"

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

namespace NYP {
namespace NServer {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TPodSet
    : public TObject
    , public NYT::TRefTracked<TPodSet>
{
public:
    TPodSet(
        const TObjectId& id,
        NYT::NYson::TYsonString labels,
        TNodeSegment* nodeSegment,
        std::vector<NClient::NApi::NProto::TPodSetSpec_TAntiaffinityConstraint> antiaffinityConstraints);

    DEFINE_BYVAL_RO_PROPERTY(TNodeSegment*, NodeSegment);
    DEFINE_BYREF_RO_PROPERTY(std::vector<NClient::NApi::NProto::TPodSetSpec_TAntiaffinityConstraint>, AntiaffinityConstraints);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NServer
} // namespace NYP
