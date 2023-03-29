#pragma once

#include "public.h"

#include <yt/yt/server/master/object_server/type_handler_detail.h>

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

class TTransactionTypeHandler
    : public NObjectServer::TObjectTypeHandlerWithMapBase<TTransaction>
{
public:
    TTransactionTypeHandler(
        NCellMaster::TBootstrap* bootstrap,
        NObjectClient::EObjectType objectType);

private:
    NCellMaster::TBootstrap* const Bootstrap_;
    const NObjectClient::EObjectType ObjectType_;

    NObjectServer::ETypeFlags GetFlags() const override;
    NObjectServer::EObjectType GetType() const override;

    NObjectClient::TCellTagList DoGetReplicationCellTags(const TTransaction* transaction) override;

    NObjectServer::IObjectProxyPtr DoGetProxy(
        TTransaction* transaction,
        TTransaction* /*dummyTransaction*/) override;

    NSecurityServer::TAccessControlDescriptor* DoFindAcd(TTransaction* transaction) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
