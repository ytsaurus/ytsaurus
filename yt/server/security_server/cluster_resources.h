#pragma once

#include "public.h"

#include <core/misc/serialize.h>

#include <core/yson/public.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

//! Cluster resources occupied by a particular user or object.
struct TClusterResources
{
    TClusterResources();
    TClusterResources(
        i64 diskSpace,
        int nodeCount,
        int chunkCount);

    //! Space occupied on data nodes in bytes.
    /*!
     *  This takes replication into account. At intermediate stages
     *  the actual space may be different.
     */
    i64 DiskSpace;

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

void Serialize(const TClusterResources& resources, NYson::IYsonConsumer* consumer);
void Deserialize(TClusterResources& value, NYTree::INodePtr node);

const TClusterResources& ZeroClusterResources();

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

