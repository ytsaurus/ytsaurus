#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/chunk_server/public.h>

#include <yt/core/yson/public.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

//! Cluster resources occupied by a particular user or object.
struct TClusterResources
{
    TClusterResources();

    //! Set node count.
    TClusterResources&& SetNodeCount(int nodeCount) &&;

    //! Set chunk count.
    TClusterResources&& SetChunkCount(int chunkCount) &&;

    //! Set tablet count.
    TClusterResources&& SetTabletCount(int tabletCount) &&;

    //! Set tablet static memory size.
    TClusterResources&& SetTabletStaticMemory(i64 tabletStaticMemory) &&;

    //! Set medium disk space.
    TClusterResources&& SetMediumDiskSpace(int mediumIndex, i64 diskSpace) &&;

    //! Space occupied on data nodes in bytes per medium.
    /*!
     *  This takes replication into account. At intermediate stages
     *  the actual space may be different.
     */
    NChunkServer::TPerMediumArray<i64> DiskSpace;

    //! Number of Cypress nodes created at master.
    /*!
     *  Branched copies are also counted.
     */
    int NodeCount;

    //! Number of chunks created at master.
    int ChunkCount;

    //! Number of tablets.
    int TabletCount;

    //! Occupied tablet static memory.
    i64 TabletStaticMemory;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

////////////////////////////////////////////////////////////////////////////////

//! A helper for (de)serializing TClusterResources.
//! This cannot be done directly as serialization requires converting medium
//! indexes to names, which is impossible without the chunk manager.
class TSerializableClusterResources
    : public NYTree::TYsonSerializable
{
public:
    // For deserialization.
    TSerializableClusterResources();
    // For serialization.
    TSerializableClusterResources(
        const NChunkServer::TChunkManagerPtr& chunkManager,
        const TClusterResources& clusterResources);

    TClusterResources ToClusterResources(const NChunkServer::TChunkManagerPtr& chunkManager) const;

private:
    int NodeCount_ = 0;
    int ChunkCount_ = 0;
    int TabletCount_ = 0;
    i64 TabletStaticMemory_ = 0;
    THashMap<TString, i64> DiskSpacePerMedium_;
    i64 DiskSpace_; // Compatibility.

};

DEFINE_REFCOUNTED_TYPE(TSerializableClusterResources)

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TClusterResources* protoResources, const TClusterResources& resources);
void FromProto(TClusterResources* resources, const NProto::TClusterResources& protoResources);

TClusterResources& operator += (TClusterResources& lhs, const TClusterResources& rhs);
TClusterResources  operator +  (const TClusterResources& lhs, const TClusterResources& rhs);

TClusterResources& operator -= (TClusterResources& lhs, const TClusterResources& rhs);
TClusterResources  operator -  (const TClusterResources& lhs, const TClusterResources& rhs);

TClusterResources& operator *= (TClusterResources& lhs, i64 rhs);
TClusterResources  operator *  (const TClusterResources& lhs, i64 rhs);

TClusterResources  operator -  (const TClusterResources& resources);

bool operator == (const TClusterResources& lhs, const TClusterResources& rhs);
bool operator != (const TClusterResources& lhs, const TClusterResources& rhs);

void FormatValue(TStringBuilder* builder, const TClusterResources& resources, TStringBuf /*format*/);
TString ToString(const TClusterResources& resources);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

