#pragma once

#include <server/cypress_server/public.h>
#include <core/ytree/yson_serializable.h>
#include <server/cell_master/public.h>

namespace NYT {
namespace NOrchid {

////////////////////////////////////////////////////////////////////////////////

struct TOrchidManifest
    : public TYsonSerializable
{
    typedef TIntrusivePtr<TOrchidManifest> TPtr;

    Stroka RemoteAddress;
    Stroka RemoteRoot;
    TDuration Timeout;

    TOrchidManifest()
    {
        RegisterParameter("remote_address", RemoteAddress);
        RegisterParameter("remote_root", RemoteRoot)
            .Default("/");
        RegisterParameter("timeout", Timeout)
            .Default(TDuration::MilliSeconds(3000));
    }
};

////////////////////////////////////////////////////////////////////////////////

NCypressServer::INodeTypeHandlerPtr CreateOrchidTypeHandler(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NOrchid
} // namespace NYT
