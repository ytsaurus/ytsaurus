#pragma once

#include "public.h"

#include <core/misc/serialize.h>
#include <core/misc/nullable.h>

#include <ytlib/chunk_client/chunk_replica.h>

#include <ytlib/node_tracker_client/node_tracker_service.pb.h>

namespace NYT {
namespace NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

//! Keeps a cached information about data node obtained by fetch request.
class TNodeDescriptor
{
public:
    TNodeDescriptor();

    explicit TNodeDescriptor(const yhash_map<Stroka, Stroka>& addresses);

    bool IsLocal() const;

    const Stroka& GetDefaultAddress() const;
    const Stroka& GetInterconnectAddress() const;
    const Stroka& GetAddressOrThrow(const Stroka& name) const;
    const Stroka& GetAddress(const Stroka& name) const;
    TNullable<Stroka> FindAddress(const Stroka& name) const;

    void Persist(TStreamPersistenceContext& context);

    typedef yhash_map<Stroka, Stroka> TAddressMap;
    DEFINE_BYREF_RO_PROPERTY(TAddressMap, Addresses);
};

bool operator == (const TNodeDescriptor& lhs, const TNodeDescriptor& rhs);
bool operator != (const TNodeDescriptor& lhs, const TNodeDescriptor& rhs);

Stroka ToString(const TNodeDescriptor& descriptor);

void ToProto(NProto::TNodeDescriptor* protoDescriptor, const TNodeDescriptor& descriptor);
void FromProto(TNodeDescriptor* descriptor, const NProto::TNodeDescriptor& protoDescriptor);

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
    TSpinLock SpinLock;
    yhash_map<TNodeId, TNodeDescriptor> IdToDescriptor;
    yhash_map<Stroka, TNodeDescriptor> AddressToDescriptor;

    void DoAddDescriptor(TNodeId id, const TNodeDescriptor& descriptor);

};

DEFINE_REFCOUNTED_TYPE(TNodeDirectory)

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerClient
} // namespace NYT
