#pragma once

#include "public.h"
#include "chunk_replica.h"

#include <ytlib/chunk_client/node.pb.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! Keeps a cached information about data node obtained by fetch request.
struct TNodeDescriptor
{
    bool IsLocal() const;

    Stroka Address;

};

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
    : public TRefCounted
{
public:
    void MergeFrom(const NProto::TNodeDirectory& source);
    void DumpTo(NProto::TNodeDirectory* destination);

    void AddDescriptor(TNodeId id, const TNodeDescriptor& descriptor);

    const TNodeDescriptor* FindDescriptor(TNodeId id) const;
    const TNodeDescriptor& GetDescriptor(TNodeId id) const;
    const TNodeDescriptor& GetDescriptor(TChunkReplica replica) const;
    std::vector<TNodeDescriptor> GetDescriptors(const std::vector<TChunkReplica>& replicas) const;

    const TNodeDescriptor* FindDescriptor(const Stroka& address);
    const TNodeDescriptor& GetDescriptor(const Stroka& address);

private:
    TSpinLock SpinLock;
    yhash_map<TNodeId, TNodeDescriptor> IdToDescriptor;
    yhash_map<Stroka, TNodeDescriptor> AddressToDescriptor;

    void DoAddDescriptor(TNodeId id, const TNodeDescriptor& descriptor);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

