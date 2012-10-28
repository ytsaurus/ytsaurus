#pragma once

#include "public.h"

#include <ytlib/ytree/public.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

//! Cluster resources occupied by a particular user or object.
struct TClusterResources
{
    TClusterResources();

    //! Space occupied on data nodes in bytes.
    /*!
     *  This takes replication into account. At intermediate stages
     *  the actual space may be different.
     */
    i64 DiskSpace;
};

void Serialize(const TClusterResources& resources, NYTree::IYsonConsumer* consumer);

const TClusterResources& ZeroClusterResources();

TClusterResources& operator += (TClusterResources& lhs, const TClusterResources& rhs);
TClusterResources  operator +  (const TClusterResources& lhs, const TClusterResources& rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

