#pragma once

#include "public.h"

#include <ytlib/node_tracker_client/public.h>

#include <ytlib/misc/small_vector.h>

#include <contrib/libs/protobuf/repeated_field.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

// Forward declarations.
class TChunkReplica;
typedef TSmallVector<TChunkReplica, TypicalReplicaCount> TChunkReplicaList;

void ToProto(ui32* value, TChunkReplica replica);
void FromProto(TChunkReplica* replica, ui32 value);

////////////////////////////////////////////////////////////////////////////////

//! A compact representation of |(nodeId, index)| pair.
class TChunkReplica
{
public:
    TChunkReplica();
    TChunkReplica(int nodeId, int index);

    int GetNodeId() const;
    int GetIndex() const;

private:
    /*!
     *  Bits:
     *   0-27: node id
     *  28-31: index
     */
    ui32 Value;

    explicit TChunkReplica(ui32 value);

    friend void ToProto(ui32* value, TChunkReplica replica);
    friend void FromProto(TChunkReplica* replica, ui32 value);

};

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(TChunkReplica replica);
Stroka ToString(TChunkReplica replica, NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory);
Stroka JoinToString(const TChunkReplicaList& replicas, NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
