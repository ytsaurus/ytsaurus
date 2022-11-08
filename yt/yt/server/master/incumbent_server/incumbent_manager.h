#pragma once

#include "public.h"

#include <yt/yt/server/lib/incumbent_client/proto/incumbent_service.pb.h>

#include <yt/yt/ytlib/incumbent_client/incumbent_descriptor.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NIncumbentServer {

////////////////////////////////////////////////////////////////////////////////

struct IIncumbentManager
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    virtual NYTree::IYPathServicePtr GetOrchidService() = 0;

    virtual void RegisterIncumbent(IIncumbentPtr incumbent) = 0;

    virtual void OnHeartbeat(
        TInstant peerLeaseDeadline,
        const NIncumbentClient::TIncumbentMap& incumbentMap) = 0;

    virtual int GetIncumbentCount(EIncumbentType type) const = 0;

    virtual bool HasIncumbency(EIncumbentType type, int shardIndex) const = 0;

    virtual std::optional<TString> GetIncumbentAddress(EIncumbentType type, int shardIndex) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IIncumbentManager)

////////////////////////////////////////////////////////////////////////////////

IIncumbentManagerPtr CreateIncumbentManager(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIncumbentServer
