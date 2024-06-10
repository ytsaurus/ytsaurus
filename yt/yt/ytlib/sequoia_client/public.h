#pragma once

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/client/sequoia_client/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESequoiaTable,
    (PathToNodeId)
    (NodeIdToPath)
    (ChunkReplicas)
    (LocationReplicas)
    (ChildNode)
    (Transactions)
    (TransactionDescendants)
    (TransactionReplicas)
    (DependentTransactions)
);

namespace NRecords {

struct TPathToNodeId;
struct TNodeIdToPath;
struct TChildNode;

struct TChunkReplicas;
struct TLocationReplicas;

struct TTransaction;
struct TTransactionDescendant;
struct TTransactionReplica;
struct TDependentTransaction;

} // namespace NRecords

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ISequoiaClient)
DECLARE_REFCOUNTED_STRUCT(ISequoiaTransaction)

DECLARE_REFCOUNTED_STRUCT(ILazySequoiaClient)

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_STRONG_TYPEDEF(TMangledSequoiaPath, TString);
YT_DEFINE_STRONG_TYPEDEF(TRawYPath, TString);

template <bool Absolute>
class TBasicYPath;

template <bool Absolute>
class TBasicYPathBuf;

using TYPath = TBasicYPath<false>;
using TYPathBuf = TBasicYPathBuf<false>;

using TAbsoluteYPath = TBasicYPath<true>;
using TAbsoluteYPathBuf = TBasicYPathBuf<true>;

////////////////////////////////////////////////////////////////////////////////

struct TSequoiaTransactionSequencingOptions;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
