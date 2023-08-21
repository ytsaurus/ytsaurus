#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra_common/public.h>

#include <yt/yt/server/master/object_server/public.h>
#include <yt/yt/server/master/object_server/type_handler_detail.h>

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class TCellBundleTypeHandlerBase
    : public NObjectServer::TConcreteObjectTypeHandlerBase<TImpl>
{
public:
    using NObjectServer::TConcreteObjectTypeHandlerBase<TImpl>::TConcreteObjectTypeHandlerBase;

    NObjectServer::ETypeFlags GetFlags() const override;
    NObjectServer::TObject* FindObject(NObjectClient::TObjectId id) override;

protected:
    using TBase = NObjectServer::TConcreteObjectTypeHandlerBase<TImpl>;

    NObjectServer::TObject* DoCreateObject(
        std::unique_ptr<TCellBundle> holder,
        NYTree::IAttributeDictionary* attributes,
        NTabletClient::TTabletCellOptionsPtr options);

    NObjectClient::TCellTagList DoGetReplicationCellTags(const TImpl* cellBundle) override;
    NSecurityServer::TAccessControlDescriptor* DoFindAcd(TImpl* cellBundle) override;
    void DoZombifyObject(TImpl* cellBundle) override;
    void DoDestroyObject(TImpl* cellBundle) noexcept override;

    void CheckInvariants(NCellMaster::TBootstrap* bootstrap) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
