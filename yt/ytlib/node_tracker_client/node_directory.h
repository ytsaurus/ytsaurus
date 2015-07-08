#pragma once

#include "public.h"

#include <core/misc/nullable.h>
#include <core/misc/property.h>
#include <core/misc/enum.h>

#include <ytlib/chunk_client/chunk_replica.h>

#include <ytlib/node_tracker_client/node_tracker_service.pb.h>

namespace NYT {
namespace NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

//! Network-related node information.
class TNodeDescriptor
{
public:
    DEFINE_BYREF_RO_PROPERTY(TAddressMap, Addresses);
    DEFINE_BYVAL_RO_PROPERTY(TNullable<Stroka>, Rack);

public:
    TNodeDescriptor() = default;
    explicit TNodeDescriptor(const Stroka& defaultAddress);
    explicit TNodeDescriptor(
        const TAddressMap& addresses,
        const TNullable<Stroka>& rack = Null);

    bool IsNull() const;

    const Stroka& GetDefaultAddress() const;
    const Stroka& GetInterconnectAddress() const;
    const Stroka& GetAddressOrThrow(const Stroka& name) const;
    const Stroka& GetAddress(const Stroka& name) const;
    TNullable<Stroka> FindAddress(const Stroka& name) const;

    void Persist(TStreamPersistenceContext& context);
};

bool operator == (const TNodeDescriptor& lhs, const TNodeDescriptor& rhs);
bool operator != (const TNodeDescriptor& lhs, const TNodeDescriptor& rhs);

Stroka ToString(const TNodeDescriptor& descriptor);

// Accessors for some well-known adddresses.
const Stroka& GetDefaultAddress(const TAddressMap& addresses);
const Stroka& GetInterconnectAddress(const TAddressMap& addresses);

//! Please keep the items in this particular order: the further the better.
DEFINE_ENUM(EAddressLocality,
    (None)
    (SameRack)
    (SameHost)
);

EAddressLocality ComputeAddressLocality(const TNodeDescriptor& first, const TNodeDescriptor& second);

namespace NProto {

void ToProto(NNodeTrackerClient::NProto::TAddressMap* protoAddresses, const NNodeTrackerClient::TAddressMap& addresses);
void FromProto(NNodeTrackerClient::TAddressMap* addresses, const NNodeTrackerClient::NProto::TAddressMap& protoAddresses);

void ToProto(NNodeTrackerClient::NProto::TNodeDescriptor* protoDescriptor, const NNodeTrackerClient::TNodeDescriptor& descriptor);
void FromProto(NNodeTrackerClient::TNodeDescriptor* descriptor, const NNodeTrackerClient::NProto::TNodeDescriptor& protoDescriptor);

} // namespace

////////////////////////////////////////////////////////////////////////////////

//! Caches node descriptors obtained by fetch requests.
/*!
 *  \note
 *  Thread affinity: thread-safe
 */
class TNodeDirectory
    : public TIntrinsicRefCounted
{
public:
    void MergeFrom(const NProto::TNodeDirectory& source);
    void DumpTo(NProto::TNodeDirectory* destination);

    void AddDescriptor(TNodeId id, const TNodeDescriptor& descriptor);

    const TNodeDescriptor* FindDescriptor(TNodeId id) const;
    const TNodeDescriptor& GetDescriptor(TNodeId id) const;
    const TNodeDescriptor& GetDescriptor(NChunkClient::TChunkReplica replica) const;
    std::vector<TNodeDescriptor> GetDescriptors(const std::vector<NChunkClient::TChunkReplica>& replicas) const;

    const TNodeDescriptor* FindDescriptor(const Stroka& address);
    const TNodeDescriptor& GetDescriptor(const Stroka& address);

    void Persist(TStreamPersistenceContext& context);

private:
    TSpinLock SpinLock_;
    yhash_map<TNodeId, TNodeDescriptor> IdToDescriptor_;
    yhash_map<Stroka, TNodeDescriptor> AddressToDescriptor_;

    void DoAddDescriptor(TNodeId id, const TNodeDescriptor& descriptor);

};

DEFINE_REFCOUNTED_TYPE(TNodeDirectory)

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerClient
} // namespace NYT
