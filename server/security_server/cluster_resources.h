#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/chunk_server/public.h>

#include <yt/core/yson/public.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

//! Cluster resources occupied by a particular user or object.
struct TClusterResources
{
    TClusterResources();
    TClusterResources(
        int nodeCount,
        int chunkCount);

    //! Space occupied on data nodes in bytes per medium.
    /*!
     *  This takes replication into account. At intermediate stages
     *  the actual space may be different.
     */
    i64 DiskSpace[NChunkClient::MaxMediumCount];

    //! Number of Cypress nodes created at master.
    /*!
     *  Branched copies are also counted.
     */
    int NodeCount;

    //! Number of chunks created at master.
    int ChunkCount;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

//! A helper for (de)serializing TClusterResources.
//! This cannot be done directly as serialization requires converting medium
//! indexes to names, which is impossible without the chunk manager.
struct TSerializableClusterResources
    : public NYTree::TYsonSerializable
{
private:
    int NodeCount_ = 0;
    int ChunkCount_ = 0;
    yhash_map<Stroka, i64> DiskSpacePerMedium_;
    i64 DiskSpace_; // Compatibility.

public:
    // For deserialization.
    TSerializableClusterResources();
    // For serialization.
    TSerializableClusterResources(
        const NChunkServer::TChunkManagerPtr& chunkManager,
        const TClusterResources& clusterResources);

    TClusterResources ToClusterResources(const NChunkServer::TChunkManagerPtr& chunkManager) const;

private:
    void ValidateDiskSpaceOrThrow(i64 diskSpace) const;
};

DECLARE_REFCOUNTED_TYPE(TSerializableClusterResources)

void ToProto(NProto::TClusterResources* protoResources, const TClusterResources& resources);
void FromProto(TClusterResources* resources, const NProto::TClusterResources& protoResources);

TClusterResources& operator += (TClusterResources& lhs, const TClusterResources& rhs);
TClusterResources  operator +  (const TClusterResources& lhs, const TClusterResources& rhs);

TClusterResources& operator -= (TClusterResources& lhs, const TClusterResources& rhs);
TClusterResources  operator -  (const TClusterResources& lhs, const TClusterResources& rhs);

TClusterResources& operator *= (TClusterResources& lhs, i64 rhs);
TClusterResources  operator *  (const TClusterResources& lhs, i64 rhs);

TClusterResources  operator -  (const TClusterResources& resources);

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT

