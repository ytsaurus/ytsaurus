#pragma once

#include "object.h"

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NScheduler {

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
        TAccount* account,
        std::vector<NClient::NApi::NProto::TAntiaffinityConstraint> antiaffinityConstraints,
        TString nodeFilter);

    DEFINE_BYVAL_RO_PROPERTY(TNodeSegment*, NodeSegment);
    DEFINE_BYVAL_RO_PROPERTY(TAccount*, Account);
    DEFINE_BYREF_RO_PROPERTY(std::vector<NClient::NApi::NProto::TAntiaffinityConstraint>, AntiaffinityConstraints);
    DEFINE_BYREF_RO_PROPERTY(TString, NodeFilter);
    DEFINE_BYREF_RW_PROPERTY(THashSet<TPod*>, Pods);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
