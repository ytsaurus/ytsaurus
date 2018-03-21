#pragma once

#include "public.h"

#include <yp/server/objects/public.h>

#include <yp/server/master/public.h>

namespace NYP {
namespace NServer {
namespace NNet {

////////////////////////////////////////////////////////////////////////////////

class TNetManager
    : public TRefCounted
{
public:
    TNetManager(
        NMaster::TBootstrap* bootstrap,
        TNetManagerConfigPtr config);

    //! Constructs the FQDN for a given pod.
    //! This is supposed to never change.
    TString BuildPersistentPodFqdn(NObjects::TPod* pod);

    //! Constructs the FQDN for a given pod.
    //! This is only present if the pod is assigned to some node;
    //! moreover it changes to a _unique_ new value the pod gets reassigned to another node.
    TString BuildTransientPodFqdn(NObjects::TPod* pod);

    //! Preloads data for #UpdatePodAddresses.
    void PrepareUpdatePodAddresses(NObjects::TPod* pod);

    //! Drops previously assigned addresses and generates new (unique) ones.
    void UpdatePodAddresses(
        const NObjects::TTransactionPtr& transaction,
        NObjects::TPod* pod);

private:
    class TImpl;
    const NYT::TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TNetManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NNet
} // namespace NServer
} // namespace NYP
