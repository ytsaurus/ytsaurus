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

TNodeDescriptor::TNodeDescriptor()
{ }

TNodeDescriptor::TNodeDescriptor(const yhash_map<Stroka, Stroka>& addresses)
    : Addresses_(addresses)
{ }

bool TNodeDescriptor::IsLocal() const
{
    return GetServiceHostName(GetDefaultAddress()) == TAddressResolver::Get()->GetLocalHostName();
}

const Stroka& TNodeDescriptor::GetDefaultAddress() const
{
    auto it = Addresses_.find(DefaultNetworkName);
    YCHECK(it != Addresses_.end());
    return it->second;
}

const Stroka& TNodeDescriptor::GetInterconnectAddress() const
{
    auto it = Addresses_.find(InterconnectNetworkName);
    if (it != Addresses_.end()) {
        return it->second;
    }
    return GetDefaultAddress();
}

const Stroka& TNodeDescriptor::GetAddressOrThrow(const Stroka& name) const
{
    // Fallback to default address if interconnect address requested.
    if (name == InterconnectNetworkName) {
        return GetInterconnectAddress();
    }

    auto it = Addresses_.find(name);
    if (it == Addresses_.end()) {
        THROW_ERROR_EXCEPTION(
            NNodeTrackerClient::EErrorCode::NoSuchNetwork,
            "Cannot find %Qv address for %v",
            name,
            GetDefaultAddress());
    }
    return it->second;
}

TNullable<Stroka> TNodeDescriptor::FindAddress(const Stroka& name) const
{
    // Fallback to default address if interconnect address requested.
    if (name == InterconnectNetworkName) {
        return GetInterconnectAddress();
    }

    auto it = Addresses_.find(name);
    if (it == Addresses_.end()) {
        return Null;
    }
    return it->second;
}

const Stroka& TNodeDescriptor::GetAddress(const Stroka& name) const
{
    // Fallback to default address if interconnect address requested.
    if (name == InterconnectNetworkName) {
        return GetInterconnectAddress();
    }

    auto it = Addresses_.find(name);
    YCHECK(it != Addresses_.end());
    return it->second;
}

void TNodeDescriptor::Persist(TStreamPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Addresses_);
}

Stroka ToString(const TNodeDescriptor& descriptor)
{
    return descriptor.GetDefaultAddress();
}

void ToProto(NProto::TNodeDescriptor* protoDescriptor, const TNodeDescriptor& descriptor)
{
    protoDescriptor->set_address(descriptor.GetDefaultAddress());
    for (const auto& pair : descriptor.Addresses()) {
        if (pair.first == DefaultNetworkName) {
            continue;
        }
        NProto::TAddressDescriptor addressDescriptor;
        addressDescriptor.set_network(pair.first);
        addressDescriptor.set_address(pair.second);
        *protoDescriptor->add_addresses() = addressDescriptor;
    }
}

void FromProto(TNodeDescriptor* descriptor, const NProto::TNodeDescriptor& protoDescriptor)
{
    TNodeDescriptor::TAddressMap addresses;
    for (const auto& addressDescriptor : protoDescriptor.addresses()) {
        addresses[addressDescriptor.network()] = addressDescriptor.address();
    }
    addresses[DefaultNetworkName] = protoDescriptor.address();
    *descriptor = TNodeDescriptor(addresses);
}

bool operator == (const TNodeDescriptor& lhs, const TNodeDescriptor& rhs)
{
    return lhs.GetDefaultAddress() == rhs.GetDefaultAddress();
}

bool operator != (const TNodeDescriptor& lhs, const TNodeDescriptor& rhs)
{
    return !(lhs == rhs);
}

///////////////////////////////////////////////////////////////////////////////

void TNodeDirectory::MergeFrom(const NProto::TNodeDirectory& source)
{
    TGuard<TSpinLock> guard(SpinLock);
    for (const auto& item : source.items()) {
        DoAddDescriptor(item.node_id(), FromProto<TNodeDescriptor>(item.node_descriptor()));
    }
}

void TNodeDirectory::DumpTo(NProto::TNodeDirectory* destination)
{
    TGuard<TSpinLock> guard(SpinLock);
    for (const auto& pair : IdToDescriptor) {
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
    auto it = IdToDescriptor.find(id);
    YCHECK(it == IdToDescriptor.end() || it->second == descriptor);
    if (it != IdToDescriptor.end()) {
        return;
    }
    IdToDescriptor[id] = descriptor;
    AddressToDescriptor[descriptor.GetDefaultAddress()] = descriptor;
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
    for (auto replica : replicas) {
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

