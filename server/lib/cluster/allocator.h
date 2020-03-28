#pragma once

#include "public.h"

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EAllocatorConstraintKind,
    (Antiaffinity)
    (Cpu)
    (Memory)
    (Network)
    (Disk)
    (IP6AddressIP4TunnelCapacity)
    (IP6AddressIP4TunnelUnknownIP4AddressPool)
    (IP6AddressVlan)
    (IP6Subnet)
    (Slot)
    (Gpu)
    (DaemonSet)
);

using TAllocatorConstraintCounters = TEnumIndexedVector<EAllocatorConstraintKind, int>;

////////////////////////////////////////////////////////////////////////////////

class TAllocatorDiagnostics
{
public:
    void RegisterUnsatisfiedConstraint(EAllocatorConstraintKind constraintKind);

    const TAllocatorConstraintCounters& GetUnsatisfiedConstraintCounters() const;

private:
    TAllocatorConstraintCounters UnsatisfiedConstraintCounters_;
};

////////////////////////////////////////////////////////////////////////////////

class TAllocator
{
public:
    //! Allocates node resources for pod or throws an error
    //! if any of the constraints is not satisfied.
    void Allocate(TNode* node, TPod* pod);

    //! Checks whether all the constraints are satisfied.
    bool CanAllocate(TNode* node, TPod* pod);

    const TAllocatorDiagnostics& GetDiagnostics() const;

private:
    TAllocatorDiagnostics Diagnostics_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
