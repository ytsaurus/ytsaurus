#pragma once

#include "public.h"

#include <yt/server/master/cell_master/public.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

class TCellHydraJanitor
    : public TRefCounted
{
public:
    explicit TCellHydraJanitor(NCellMaster::TBootstrap* bootstrap);

    ~TCellHydraJanitor();

    void Initialize();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TCellHydraJanitor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
