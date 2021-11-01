#include "shard_type_handler.h"
#include "shard_proxy.h"
#include "shard.h"

#include <yt/yt/server/master/object_server/type_handler_detail.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NCypressServer {

using namespace NHydra;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

class TCypressShardTypeHandler
    : public TObjectTypeHandlerWithMapBase<TCypressShard>
{
public:
    TCypressShardTypeHandler(
        TBootstrap* bootstrap,
        TEntityMap<TCypressShard>* map)
        : TObjectTypeHandlerWithMapBase<TCypressShard>(bootstrap, map)
        , Bootstrap_(bootstrap)
    { }

    EObjectType GetType() const override
    {
        return EObjectType::CypressShard;
    }

private:
    TBootstrap* const Bootstrap_;

    IObjectProxyPtr DoGetProxy(TCypressShard* shard, TTransaction* /*transaction*/) override
    {
        return CreateCypressShardProxy(Bootstrap_, &Metadata_, shard);
    }
};

IObjectTypeHandlerPtr CreateShardTypeHandler(
    TBootstrap* bootstrap,
    TEntityMap<TCypressShard>* map)
{
    return New<TCypressShardTypeHandler>(bootstrap, map);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
