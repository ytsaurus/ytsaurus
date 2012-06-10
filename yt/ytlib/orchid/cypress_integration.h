#pragma once

#include <ytlib/misc/configurable.h>
#include <ytlib/cypress/public.h>
#include <ytlib/cell_master/public.h>

namespace NYT {
namespace NOrchid {

////////////////////////////////////////////////////////////////////////////////

struct TOrchidManifest
    : public TConfigurable
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
