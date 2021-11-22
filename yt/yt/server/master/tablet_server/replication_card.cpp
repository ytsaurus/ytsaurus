#include "replication_card.h"

#include <yt/yt/client/chaos_client/replication_card.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

TReplicationCardToken::TReplicationCardToken()
{
    RegisterParameter("chaos_cell_id", ChaosCellId)
        .Default();
    RegisterParameter("replication_card_id", ReplicationCardId)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

NChaosClient::TReplicationCardToken ConvertToClientReplicationCardToken(const TReplicationCardTokenPtr replicationCardToken)
{
    return NChaosClient::TReplicationCardToken{
        replicationCardToken->ChaosCellId,
        replicationCardToken->ReplicationCardId
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
