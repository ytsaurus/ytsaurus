#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra/public.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

struct IMasterConnector
    : public TRefCounted
{
    virtual void Start() = 0;

    // Thread affinity: any.
    virtual bool IsUp() const = 0;
    virtual void ValidateRegistration() const = 0;

    // Throws iff proxy is not registered.
    virtual NHydra::TReign GetMasterReign() const = 0;
    virtual int GetMaxCopiableSubtreeSize() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IMasterConnector)

////////////////////////////////////////////////////////////////////////////////

IMasterConnectorPtr CreateMasterConnector(IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
