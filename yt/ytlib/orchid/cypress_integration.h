#pragma once

#include <ytlib/cypress/public.h>
#include <ytlib/ytree/yson_serializable.h>
#include <ytlib/cell_master/public.h>

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
        Register("remote_address", RemoteAddress);
        Register("remote_root", RemoteRoot)
            .Default("/");
        Register("timeout", Timeout)
            .Default(TDuration::MilliSeconds(3000));
    }
};

////////////////////////////////////////////////////////////////////////////////

NCypress::INodeTypeHandlerPtr CreateOrchidTypeHandler(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NOrchid
} // namespace NYT
