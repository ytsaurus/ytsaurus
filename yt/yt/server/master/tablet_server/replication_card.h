#pragma once

#include "public.h"

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TReplicationCardToken
    : public NYTree::TYsonSerializable
{
public:
    NObjectClient::TCellId ChaosCellId;
    NChaosClient::TReplicationCardId ReplicationCardId;

    TReplicationCardToken();
};

DEFINE_REFCOUNTED_TYPE(TReplicationCardToken)

////////////////////////////////////////////////////////////////////////////////

NChaosClient::TReplicationCardToken ConvertToClientReplicationCardToken(const TReplicationCardTokenPtr replicationCardToken);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
