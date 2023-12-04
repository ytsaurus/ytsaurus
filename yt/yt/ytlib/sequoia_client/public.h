#pragma once

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESequoiaTable,
    (ResolveNode)
    (ChunkReplicas)
    (LocationReplicas)
    (ReverseResolveNode)
    (ChildNode)
);

namespace NRecords {

struct TResolveNode;
struct TReverseResolveNode;
struct TChildNode;

struct TChunkReplicas;
struct TLocationReplicas;

} // namespace NRecords

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ISequoiaClient)
DECLARE_REFCOUNTED_STRUCT(ISequoiaTransaction)

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_STRONG_TYPEDEF(TMangledSequoiaPath, NYPath::TYPath);

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((SequoiaClientNotReady)    (6000))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
