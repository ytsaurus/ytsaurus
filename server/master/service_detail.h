#pragma once

#include "public.h"

#include <yt/core/rpc/service_detail.h>

namespace NYP {
namespace NServer {
namespace NMaster {

////////////////////////////////////////////////////////////////////////////////

class TServiceBase
    : public NRpc::TServiceBase
{
protected:
    TBootstrap* const Bootstrap_;

    TServiceBase(
        TBootstrap* bootstrap,
        const NYT::NRpc::TServiceDescriptor& descriptor,
        const NLogging::TLogger& logger);

    virtual void BeforeInvoke(NRpc::IServiceContext* context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMaster
} // namespace NServer
} // namespace NYP
