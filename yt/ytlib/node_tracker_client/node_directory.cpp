#include "stdafx.h"
#include "node_directory.h"

#include <core/misc/address.h>
#include <core/misc/protobuf_helpers.h>

#include <core/concurrency/thread_affinity.h>

namespace NYT {
namespace NNodeTrackerClient {

using namespace NChunkClient;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

bool TNodeDescriptor::IsLocal() const
{
    return GetServiceHostName(Address) == TAddressResolver::Get()->GetLocalHostName();
}

void TNodeDescriptor::Persist(TStreamPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Address);
}

Stroka ToString(const TNodeDescriptor& descriptor)
{
    return descriptor.Address;
}

void ToProto(NProto::TNodeDescriptor* protoDescriptor, const TNodeDescriptor& descriptor)
{
    protoDescriptor->set_address(descriptor.Address);
}

void FromProto(TNodeDescriptor* descriptor, const NProto::TNodeDescriptor& protoDescriptor)
{
    descriptor->Address = protoDescriptor.address();
}

///////////////////////////////////////////////////////////////////////////////

void TNodeDirectory::MergeFrom(const NProto::TNodeDirectory& source)
{
    TGuard<TSpinLock> guard(SpinLock);
    FOREACH (const auto& item, source.items()) {
        DoAddDescriptor(item.node_id(), FromProto<TNodeDescriptor>(item.node_descriptor()));
    }
}

void TNodeDirectory::DumpTo(NProto::TNodeDirectory* destination)
{
    TGuard<TSpinLock> guard(SpinLock);
    FOREACH (const auto& pair, IdToDescriptor) {
        auto* item = destination->add_items();
        item->set_node_id(pair.first);
        ToProto(item->mutable_node_descriptor(), pair.second);
    }
}

void TNodeDirectory::AddDescriptor(TNodeId id, const TNodeDescriptor& descriptor)
{
    TGuard<TSpinLock> guard(SpinLock);
    DoAddDescriptor(id, descriptor);
}

void TNodeDirectory::DoAddDescriptor(TNodeId id, const TNodeDescriptor& descriptor)
{
    IdToDescriptor[id] = descriptor;
    AddressToDescriptor[descriptor.Address] = descriptor;
}

const TNodeDescriptor* TNodeDirectory::FindDescriptor(TNodeId id) const
{
    TGuard<TSpinLock> guard(SpinLock);
    auto it = IdToDescriptor.find(id);
    return it == IdToDescriptor.end() ? nullptr : &it->second;
}

const TNodeDescriptor& TNodeDirectory::GetDescriptor(TNodeId id) const
{
    const auto* result = FindDescriptor(id);
    YCHECK(result);
    return *result;
}

const TNodeDescriptor& TNodeDirectory::GetDescriptor(TChunkReplica replica) const
{
    return GetDescriptor(replica.GetNodeId());
}

std::vector<TNodeDescriptor> TNodeDirectory::GetDescriptors(const std::vector<TChunkReplica>& replicas) const
{
    std::vector<TNodeDescriptor> result;
    FOREACH (auto replica, replicas) {
        result.push_back(GetDescriptor(replica));
    }
    return result;
}

const TNodeDescriptor* TNodeDirectory::FindDescriptor(const Stroka& address)
{
    TGuard<TSpinLock> guard(SpinLock);
    auto it = AddressToDescriptor.find(address);
    return it == AddressToDescriptor.end() ? nullptr : &it->second;
}

const TNodeDescriptor& TNodeDirectory::GetDescriptor(const Stroka& address)
{
    const auto* result = FindDescriptor(address);
    YCHECK(result);
    return *result;
}

void TNodeDirectory::Persist(TStreamPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, IdToDescriptor);
    Persist(context, AddressToDescriptor);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerClient
} // namespace NYT

