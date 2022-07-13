#pragma once

#include "public.h"

#include <yt/yt/server/master/object_server/public.h>
#include <yt/yt/server/master/object_server/type_handler_detail.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class TTabletTypeHandlerBase
    : public NObjectServer::TConcreteObjectTypeHandlerBase<TImpl>
{
protected:
    using TBase = NObjectServer::TConcreteObjectTypeHandlerBase<TImpl>;

public:
    using TBase::TBase;

    NObjectServer::TObject* FindObject(NObjectClient::TObjectId id) override;

protected:
    void DoDestroyObject(TImpl* cell) noexcept override;

    void CheckInvariants(NCellMaster::TBootstrap* bootstrap) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
