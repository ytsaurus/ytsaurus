#pragma once

#include "object.h"

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

class TPodSet
    : public TObject
    , public NYT::TRefTracked<TPodSet>
{
public:
    static constexpr EObjectType Type = EObjectType::PodSet;

    TPodSet(
        TObjectId id,
        NYT::NYson::TYsonString labels,
        TObjectId nodeSegmentId,
        TObjectId accountId,
        TObjectId podDisruptionBudgetId,
        std::vector<NClient::NApi::NProto::TAntiaffinityConstraint> antiaffinityConstraints,
        TString nodeFilter);

    DEFINE_BYREF_RO_PROPERTY(TObjectId, NodeSegmentId);
    DEFINE_BYREF_RO_PROPERTY(TObjectId, AccountId);
    DEFINE_BYREF_RO_PROPERTY(TObjectId, PodDisruptionBudgetId);

    DEFINE_BYREF_RO_PROPERTY(std::vector<NClient::NApi::NProto::TAntiaffinityConstraint>, AntiaffinityConstraints);
    DEFINE_BYREF_RO_PROPERTY(TString, NodeFilter);

    DEFINE_BYREF_RW_PROPERTY(THashSet<TPod*>, SchedulablePods);

    DEFINE_BYVAL_RW_PROPERTY(TNodeSegment*, NodeSegment);
    DEFINE_BYVAL_RW_PROPERTY(TAccount*, Account);
    DEFINE_BYVAL_RW_PROPERTY(TPodDisruptionBudget*, PodDisruptionBudget);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
