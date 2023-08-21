#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra_common/public.h>

#include <yt/yt/server/master/object_server/public.h>
#include <yt/yt/server/master/object_server/type_handler_detail.h>

#include <yt/yt/server/master/cell_master/public.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class TCellTypeHandlerBase
    : public NObjectServer::TConcreteObjectTypeHandlerBase<TImpl>
{
protected:
    using TBase = NObjectServer::TConcreteObjectTypeHandlerBase<TImpl>;

public:
    using TBase::TBase;

    NObjectServer::ETypeFlags GetFlags() const override;
    NObjectServer::TObject* FindObject(NObjectClient::TObjectId id) override;

protected:
    TImpl* DoCreateObject(
        NObjectClient::TObjectId id,
        NYTree::IAttributeDictionary* attributes);

    NObjectClient::TCellTagList DoGetReplicationCellTags(const TImpl* /*cell*/) override;
    void DoZombifyObject(TImpl* cell) override;
    void DoDestroyObject(TImpl* cell) noexcept override;

    void CheckInvariants(NCellMaster::TBootstrap* bootstrap) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
