#pragma once

#include <yt/server/cell_master/public.h>

#include <yt/server/cypress_server/public.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NOrchid {

////////////////////////////////////////////////////////////////////////////////

struct TOrchidManifest
    : public NYTree::TYsonSerializable
{
    Stroka RemoteAddress;
    Stroka RemoteRoot;
    TDuration Timeout;

    TOrchidManifest()
    {
        RegisterParameter("remote_address", RemoteAddress);
        RegisterParameter("remote_root", RemoteRoot)
            .Default("/");
        RegisterParameter("timeout", Timeout)
            .Default(TDuration::Seconds(15));
    }
};

typedef TIntrusivePtr<TOrchidManifest> TOrchidManifestPtr;

////////////////////////////////////////////////////////////////////////////////

NCypressServer::INodeTypeHandlerPtr CreateOrchidTypeHandler(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NOrchid
} // namespace NYT
