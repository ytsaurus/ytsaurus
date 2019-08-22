#pragma once

#include "public.h"

#include <yt/core/misc/ref_counted.h>

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct IClusterReader
    : virtual public NYT::TRefCounted
{
    template <class T>
    using TObjectConsumer = std::function<void(std::unique_ptr<T>)>;

    virtual NObjects::TTimestamp StartTransaction() = 0;

    virtual void ReadIP4AddressPools(
        TObjectConsumer<TIP4AddressPool> ip4AddressPool) = 0;

    virtual void ReadInternetAddresses(
        TObjectConsumer<TInternetAddress> internetAddressConsumer) = 0;

    virtual void ReadNodes(
        TObjectConsumer<TNode> nodeConsumer) = 0;

    virtual void ReadAccounts(
        TObjectConsumer<TAccount> accountConsumer) = 0;

    virtual void ReadNodeSegments(
        TObjectConsumer<TNodeSegment> nodeSegmentConsumer) = 0;

    virtual void ReadPodDisruptionBudgets(
        TObjectConsumer<TPodDisruptionBudget> podDisruptionBudgetConsumer) = 0;

    virtual void ReadPodSets(
        TObjectConsumer<TPodSet> podSetConsumer) = 0;

    virtual void ReadPods(
        TObjectConsumer<TPod> podConsumer) = 0;

    virtual void ReadResources(
        TObjectConsumer<TResource> resourceConsumer) = 0;
};

DEFINE_REFCOUNTED_TYPE(IClusterReader)

////////////////////////////////////////////////////////////////////////////////

IClusterReaderPtr CreateClusterReader(NMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
