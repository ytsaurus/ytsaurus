#pragma once

#include "public.h"

#include <yt/server/lib/hydra/public.h>

#include <yt/server/master/object_server/public.h>
#include <yt/server/master/object_server/type_handler_detail.h>

#include <yt/server/master/cell_master/public.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class TCellTypeHandlerBase
    : public NObjectServer::TObjectTypeHandlerBase<TImpl>
{
public:
    explicit TCellTypeHandlerBase(
        NCellMaster::TBootstrap* bootstrap);

    virtual NObjectServer::ETypeFlags GetFlags() const override;
    virtual NObjectServer::TObject* FindObject(NObjectClient::TObjectId id) override;

protected:
    using TBase = NObjectServer::TObjectTypeHandlerBase<TImpl>;

    NObjectServer::TObject* DoCreateObject(
        std::unique_ptr<TCellBase> holder,
        NYTree::IAttributeDictionary* attributes);
    virtual NObjectClient::TCellTagList DoGetReplicationCellTags(const TImpl* /*cell*/) override;
    virtual void DoZombifyObject(TImpl* cell) override;
    virtual void DoDestroyObject(TImpl* cell) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
