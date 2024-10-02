#pragma once

#include <yt/yt/orm/server/objects/type_handler_detail.h>

namespace NYT::NOrm::NExample::NServer::NPlugins {

////////////////////////////////////////////////////////////////////////////////

class TObjectTypeHandlerBase
    : public NYT::NOrm::NServer::NObjects::TObjectTypeHandlerBase
{
    using TBase = NYT::NOrm::NServer::NObjects::TObjectTypeHandlerBase;

public:
    TObjectTypeHandlerBase(
        NOrm::NServer::NMaster::IBootstrap* bootstrap,
        NOrm::NServer::NObjects::TObjectTypeValue type)
        : TBase(bootstrap, type)
    { }

    void PostInitialize() override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NPlugins
