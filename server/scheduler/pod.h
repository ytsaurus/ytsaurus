#pragma once

#include "object.h"

#include <yp/server/objects/proto/autogen.pb.h>
#include <yp/server/objects/proto/objects.pb.h>

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TPod
    : public TObject
    , public NYT::TRefTracked<TPod>
{
public:
    TPod(
        const TObjectId& id,
        TPodSet* podSet,
        NServer::NObjects::NProto::TMetaEtc metaEtc,
        TNode* node,
        NServer::NObjects::NProto::TPodSpecEtc specEtc,
        TAccount* account,
        NServer::NObjects::NProto::TPodStatusEtc statusEtc,
        NYT::NYson::TYsonString labels);

    DEFINE_BYVAL_RO_PROPERTY(TPodSet*, PodSet);
    DEFINE_BYREF_RO_PROPERTY(NServer::NObjects::NProto::TMetaEtc, MetaEtc);
    DEFINE_BYVAL_RO_PROPERTY(TNode*, Node);
    DEFINE_BYREF_RO_PROPERTY(NServer::NObjects::NProto::TPodSpecEtc, SpecEtc);
    DEFINE_BYVAL_RO_PROPERTY(TAccount*, Account);
    DEFINE_BYREF_RO_PROPERTY(NServer::NObjects::NProto::TPodStatusEtc, StatusEtc);

    TAccount* GetEffectiveAccount() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
