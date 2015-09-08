#include "stdafx.h"
#include "node_directory.h"

#include <core/misc/address.h>
#include <core/misc/protobuf_helpers.h>
#include <core/misc/string.h>

#include <core/concurrency/thread_affinity.h>

namespace NYT {
namespace NNodeTrackerClient {

using namespace NChunkClient;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const Stroka NullAddress;

////////////////////////////////////////////////////////////////////////////////

TNodeDescriptor::TNodeDescriptor(const Stroka& defaultAddress)
{
    YCHECK(Addresses_.insert(std::make_pair(DefaultNetworkName, defaultAddress)).second);
}

TNodeDescriptor::TNodeDescriptor(
    const TAddressMap& addresses,
    const TNullable<Stroka>& rack)
    : Addresses_(addresses)
    , Rack_(rack)
{ }

bool TNodeDescriptor::IsNull() const
{
    return Addresses_.empty();
}

const Stroka& TNodeDescriptor::GetDefaultAddress() const
{
    return IsNull() ? NullAddress : NNodeTrackerClient::GetDefaultAddress(Addresses_);
}

const Stroka& TNodeDescriptor::GetInterconnectAddress() const
{
    return IsNull() ? NullAddress : NNodeTrackerClient::GetInterconnectAddress(Addresses_);
}

const Stroka& TNodeDescriptor::GetAddressOrThrow(const Stroka& name) const
{
    // Interconnect address requires special handling since we fallback to
    // the default one when the interconnect address is missing.
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
    // See above.
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
    // See above.
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
    Persist(context, Rack_);
}

Stroka ToString(const TNodeDescriptor& descriptor)
{
    TStringBuilder builder;
    if (descriptor.IsNull()) {
        builder.AppendString("<Null>");
    } else {
        builder.AppendString(descriptor.GetDefaultAddress());
        if (descriptor.GetRack()) {
            builder.AppendChar('@');
            builder.AppendString(*descriptor.GetRack());
        }
    }
    return builder.Flush();
}

const Stroka& GetDefaultAddress(const TAddressMap& addresses)
{
    auto it = addresses.find(DefaultNetworkName);
    YCHECK(it != addresses.end());
    return it->second;
}

const Stroka& GetInterconnectAddress(const TAddressMap& addresses)
{
    auto it = addresses.find(InterconnectNetworkName);
    return it == addresses.end() ? GetDefaultAddress(addresses) : it->second;
}

EAddressLocality ComputeAddressLocality(const TNodeDescriptor& first, const TNodeDescriptor& second)
{
    if (GetServiceHostName(first.GetDefaultAddress()) == GetServiceHostName(second.GetDefaultAddress())) {
        return EAddressLocality::SameHost;
    }

    if (first.GetRack() && second.GetRack() && *first.GetRack() == *second.GetRack()) {
        return EAddressLocality::SameRack;
    }

    return EAddressLocality::None;
}

namespace NProto {

void ToProto(NNodeTrackerClient::NProto::TAddressMap* protoAddresses, const NNodeTrackerClient::TAddressMap& addresses)
{
    for (const auto& pair : addresses) {
        auto* entry = protoAddresses->add_entries();
        entry->set_network(pair.first);
        entry->set_address(pair.second);
    }
}

void FromProto(NNodeTrackerClient::TAddressMap* addresses, const NNodeTrackerClient::NProto::TAddressMap& protoAddresses)
{
    addresses->clear();
    for (const auto& entry : protoAddresses.entries()) {
        YCHECK(addresses->insert(std::make_pair(entry.network(), entry.address())).second);
    }
}

void ToProto(NNodeTrackerClient::NProto::TNodeDescriptor* protoDescriptor, const NNodeTrackerClient::TNodeDescriptor& descriptor)
{
    using NYT::ToProto;

    ToProto(protoDescriptor->mutable_addresses(), descriptor.Addresses());

    if (descriptor.GetRack()) {
        protoDescriptor->set_rack(*descriptor.GetRack());
    } else {
        protoDescriptor->clear_rack();
    }
}

void FromProto(NNodeTrackerClient::TNodeDescriptor* descriptor, const NNodeTrackerClient::NProto::TNodeDescriptor& protoDescriptor)
{
    using NYT::FromProto;

    *descriptor = NNodeTrackerClient::TNodeDescriptor(
        FromProto<NNodeTrackerClient::TAddressMap>(protoDescriptor.addresses()),
        protoDescriptor.has_rack() ? MakeNullable(protoDescriptor.rack()) : Null);
}

} // namespace NProto

bool operator == (const TNodeDescriptor& lhs, const TNodeDescriptor& rhs)
{
    return
        lhs.GetDefaultAddress() == rhs.GetDefaultAddress() &&
        lhs.GetRack() == rhs.GetRack();
}

bool operator != (const TNodeDescriptor& lhs, const TNodeDescriptor& rhs)
{
    return !(lhs == rhs);
}

///////////////////////////////////////////////////////////////////////////////

void TNodeDirectory::MergeFrom(const NProto::TNodeDirectory& source)
{
    TGuard<TSpinLock> guard(SpinLock_);
    for (const auto& item : source.items()) {
        DoAddDescriptor(item.node_id(), FromProto<TNodeDescriptor>(item.node_descriptor()));
    }
}

void TNodeDirectory::DumpTo(NProto::TNodeDirectory* destination)
{
    TGuard<TSpinLock> guard(SpinLock_);
    for (const auto& pair : IdToDescriptor_) {
        auto* item = destination->add_items();
        item->set_node_id(pair.first);
        ToProto(item->mutable_node_descriptor(), pair.second);
    }
}

void TNodeDirectory::AddDescriptor(TNodeId id, const TNodeDescriptor& descriptor)
{
    TGuard<TSpinLock> guard(SpinLock_);
    DoAddDescriptor(id, descriptor);
}

void TNodeDirectory::DoAddDescriptor(TNodeId id, const TNodeDescriptor& descriptor)
{
    auto it = IdToDescriptor_.find(id);
    if (it != IdToDescriptor_.end()) {
        YCHECK(it->second.GetDefaultAddress() == descriptor.GetDefaultAddress());
        return;
    }
    IdToDescriptor_[id] = descriptor;
    AddressToDescriptor_[descriptor.GetDefaultAddress()] = descriptor;
}

const TNodeDescriptor* TNodeDirectory::FindDescriptor(TNodeId id) const
{
    TGuard<TSpinLock> guard(SpinLock_);
    auto it = IdToDescriptor_.find(id);
    return it == IdToDescriptor_.end() ? nullptr : &it->second;
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
    TGuard<TSpinLock> guard(SpinLock_);
    auto it = AddressToDescriptor_.find(address);
    return it == AddressToDescriptor_.end() ? nullptr : &it->second;
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
    Persist(context, IdToDescriptor_);
    Persist(context, AddressToDescriptor_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerClient
} // namespace NYT

