#pragma once

#include "public.h"

#include <ytlib/yson/public.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

//! Cluster resources occupied by a particular user or object.
struct TClusterResources
{
    TClusterResources();
    explicit TClusterResources(i64 diskSpace);

    //! Space occupied on data nodes in bytes.
    /*!
     *  This takes replication into account. At intermediate stages
     *  the actual space may be different.
     */
    i64 DiskSpace;
};

void Serialize(const TClusterResources& resources, NYson::IYsonConsumer* consumer);

void Save(TOutputStream* output, const TClusterResources& resources);
void Load(TInputStream* input, TClusterResources& resources);

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

