#pragma once

#include "public.h"

#include <yt/server/lib/hydra/public.h>

#include <yt/server/master/object_server/public.h>
#include <yt/server/master/object_server/type_handler_detail.h>

#include <yt/server/master/cell_master/public.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class TCellBundleTypeHandlerBase
    : public NObjectServer::TObjectTypeHandlerBase<TImpl>
{
public:
    explicit TCellBundleTypeHandlerBase(
        NCellMaster::TBootstrap* bootstrap);

    virtual NObjectServer::ETypeFlags GetFlags() const override;
    virtual NObjectServer::TObject* FindObject(NObjectClient::TObjectId id) override;

protected:
    using TBase = NObjectServer::TObjectTypeHandlerBase<TImpl>;

    NObjectServer::TObject* DoCreateObject(
        std::unique_ptr<TCellBundle> holder,
        NYTree::IAttributeDictionary* attributes);

    virtual NObjectClient::TCellTagList DoGetReplicationCellTags(const TImpl* /*cellBundle*/) override;
    virtual NSecurityServer::TAccessControlDescriptor* DoFindAcd(TImpl* cellBundle) override;
    virtual void DoZombifyObject(TImpl* cellBundle) override;
    virtual void DoDestroyObject(TImpl* cellBundle) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
